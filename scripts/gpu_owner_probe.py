"""Fail-closed Linux AMD compute-owner probe; stdout is a bounded JSON receipt.

KFD enumerates compute clients across users. Missing visibility is an error, not
an empty owner list. Render-only desktop clients are not compute reservations.
This does not isolate display activity; measurement isolation remains a policy.

On multi-GPU hosts, ownership is filtered to the selected GPU node: an owner
counts against the resource only when it holds KFD queues on the topology node
matching the expected gfx target. Owners attached solely to other GPUs are
reported as other_gpu_owners for diagnostics and do not block acquisition.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
from execution_worker import proc_info


def _topology_target_map():
    """Map KFD node directory to integer gfx target version.

    Raises RuntimeError when topology data is missing or contradictory,
    so callers stay fail-closed instead of guessing.
    """
    nodes = Path('/sys/class/kfd/kfd/topology/nodes')
    if not nodes.is_dir():
        raise RuntimeError('KFD topology unavailable')
    mapping = {}
    for node in nodes.iterdir():
        if not node.name.isdigit():
            continue
        props = node / 'properties'
        try:
            text = props.read_text()
        except OSError:
            raise RuntimeError('KFD topology properties unreadable: ' + node.name)
        match = re.search(r'^gfx_target_version\s+(\d+)\s*$', text, re.MULTILINE)
        if not match:
            raise RuntimeError('KFD topology target missing: ' + node.name)
        version = int(match.group(1))
        if version in mapping:
            raise RuntimeError('KFD topology target ambiguous: ' + str(version))
        mapping[node.name] = version
    if not mapping:
        raise RuntimeError('KFD topology has no GPU nodes')
    return mapping


def _expected_version(expected):
    """Convert an expected gfx name (e.g. gfx1201) to its topology version int.

    gfx_target_version encodes major/minor/stepping in decimal digits:
    gfx1201 -> 120001, gfx1100 -> 110000, gfx1151 -> 110501, gfx1036 -> 100306.
    The trailing two digits split into minor (first) and stepping (second):
    version = major * 10000 + minor * 100 + stepping.
    """
    match = re.fullmatch(r'gfx(\d+)', expected)
    if not match:
        raise RuntimeError('unmappable expected GPU target: ' + expected)
    digits = match.group(1)
    if len(digits) < 3:
        raise RuntimeError('unmappable expected GPU target: ' + expected)
    major = int(digits[:-2])
    minor = int(digits[-2])
    stepping = int(digits[-1])
    return major * 10000 + minor * 100 + stepping


def _owner_queue_gpuids(pid):
    """Return the set of KFD queue gpuids held by a PID.

    Raises RuntimeError when queue data is missing or contradictory, so an
    owner whose attachment cannot be proven never reads as absent.
    """
    queues = Path(f'/sys/class/kfd/kfd/proc/{pid}/queues')
    if not queues.is_dir():
        raise RuntimeError(f'KFD queue data unavailable for owner: {pid}')
    gpuids = set()
    seen = False
    for queue in queues.iterdir():
        gpuid_file = queue / 'gpuid'
        if not gpuid_file.is_file():
            continue
        seen = True
        try:
            gpuids.add(int(gpuid_file.read_text().strip()))
        except (OSError, ValueError):
            raise RuntimeError(f'KFD queue gpuid unreadable for owner: {pid}')
    if not seen or not gpuids:
        raise RuntimeError(f'KFD queue data incomplete for owner: {pid}')
    return gpuids


def _selected_node(topology, expected):
    """Return the single topology node matching the expected target version."""
    want = _expected_version(expected)
    hits = [node for node, version in topology.items() if version == want]
    if len(hits) != 1:
        raise RuntimeError('expected GPU target maps ambiguously in KFD topology: ' + expected)
    return hits[0]


def _node_gpuid(node):
    """Return the KFD gpuid for a topology node directory name."""
    try:
        return int((Path('/sys/class/kfd/kfd/topology/nodes') / node / 'gpu_id').read_text().strip())
    except (OSError, ValueError):
        raise RuntimeError('KFD topology gpu_id unreadable: ' + node)


def _proc_entries(root):
    """Yield numeric KFD proc entries. Split seam for deterministic tests."""
    for entry in root.iterdir():
        if entry.name.isdigit():
            yield entry


def _transient_owner_paths(pid):
    """Read the per-PID /proc paths for one KFD owner candidate.

    Returns None when the process exited between discovery and these
    reads (FileNotFoundError on /proc/<pid>/...). Only disappearance
    is transient; permission errors, malformed data, and queue/topology
    failures stay fail-closed in their own helpers.
    """
    try:
        cgroup = Path(f'/proc/{pid}/cgroup').read_text()
    except FileNotFoundError:
        return None
    try:
        executable = str(Path(f'/proc/{pid}/exe').readlink())
    except FileNotFoundError:
        return None
    try:
        held = _owner_queue_gpuids(pid)
    except FileNotFoundError:
        return None
    except RuntimeError as e:
        # A KFD proc entry whose queue data vanishes mid-read belongs to
        # an exiting process, not a persistent owner. Confirm proven
        # disappearance (both the PID and its KFD sysfs entry gone)
        # before treating it as transient.
        if 'incomplete for owner' not in str(e) and 'unavailable for owner' not in str(e):
            raise
        try:
            pid_gone = not Path(f'/proc/{pid}').exists()
            kfd_gone = not Path(f'/sys/class/kfd/kfd/proc/{pid}').exists()
        except OSError:
            raise
        if not (pid_gone and kfd_gone):
            raise
        return None
    return cgroup, executable, held


def probe(rocminfo, expected):
    info = subprocess.run([rocminfo], capture_output=True, text=True, timeout=15, check=True,
                            env={**os.environ, 'ROCR_VISIBLE_DEVICES': os.environ.get(
                                'ROCR_VISIBLE_DEVICES', '1')}).stdout
    # Concrete HSA agent names only; ISA compatibility names include gfx11-generic.
    targets = sorted(set(re.findall(r'^\s*Name:\s+(gfx[0-9a-f]+)\s*$', info, re.MULTILINE)))
    if targets != [expected]:
        raise RuntimeError('prototype requires one unambiguous configured AMD GPU: ' + str(targets))
    root = Path('/sys/class/kfd/kfd/proc')
    if not root.is_dir() or not Path('/dev/kfd').exists():
        raise RuntimeError('KFD owner inspection unavailable')
    topology = _topology_target_map()
    selected = _selected_node(topology, expected)
    selected_gpuid = _node_gpuid(selected)
    owners = []
    others = []
    for entry in _proc_entries(root):
        identity = proc_info(int(entry.name))
        if not identity:
            # The short rocminfo process can exit before KFD removes its sysfs
            # node. Wait only for proven disappearance; never call a persistent
            # invisible owner safe.
            end = time.monotonic() + 1
            while entry.exists() and time.monotonic() < end:
                time.sleep(.02)
            if entry.exists():
                raise RuntimeError('GPU owner process identity unavailable: ' + entry.name)
            continue
        paths = _transient_owner_paths(identity['pid'])
        if paths is None:
            # Any short-lived KFD client (rocminfo included) can exit
            # after its identity was read but before these paths are
            # inspected. A disappeared PID holds no queues; skip it and
            # keep evaluating the rest. Permission errors and malformed
            # state still raise inside the helper.
            end = time.monotonic() + 1
            while entry.exists() and time.monotonic() < end:
                time.sleep(.02)
            if entry.exists():
                raise RuntimeError('GPU owner process inspection raced: ' + entry.name)
            continue
        identity['cgroup'], identity['executable'], held = paths[0], paths[1], paths[2]
        identity['queue_gpuids'] = sorted(held)
        if selected_gpuid in held:
            owners.append(identity)
        else:
            others.append(identity)
    return {'complete': True, 'owners': owners, 'other_gpu_owners': others,
            'capabilities': ['gpu', 'rocm', expected],
            'environment': {'rocminfo': rocminfo, 'targets': targets,
                            'rocminfo_sha256': __import__('hashlib').sha256(info.encode()).hexdigest(),
                            'selected_topology_node': selected,
                            'selected_gpuid': selected_gpuid}}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rocminfo', required=True)
    p.add_argument('--target', required=True)
    a = p.parse_args()
    print(json.dumps(probe(a.rocminfo, a.target)))
