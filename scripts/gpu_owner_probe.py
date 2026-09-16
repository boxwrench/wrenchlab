"""Fail-closed Linux AMD compute-owner probe; stdout is a bounded JSON receipt.

KFD enumerates compute clients across users. Missing visibility is an error, not
an empty owner list. Render-only desktop clients are not compute reservations.
This does not isolate display activity; measurement isolation remains a policy.
"""
import argparse
import json
from pathlib import Path
import subprocess
import time
from execution_worker import proc_info


def probe(rocminfo, expected):
    info = subprocess.run([rocminfo], capture_output=True, text=True, timeout=15, check=True).stdout
    import re
    # Concrete HSA agent names only; ISA compatibility names include gfx11-generic.
    targets = sorted(set(re.findall(r'^\s*Name:\s+(gfx[0-9a-f]+)\s*$', info, re.MULTILINE)))
    if targets != [expected]:
        raise RuntimeError('prototype requires one unambiguous configured AMD GPU: ' + str(targets))
    root = Path('/sys/class/kfd/kfd/proc')
    if not root.is_dir() or not Path('/dev/kfd').exists():
        raise RuntimeError('KFD owner inspection unavailable')
    owners = []
    for entry in root.iterdir():
        if not entry.name.isdigit():
            continue
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
        identity['cgroup'] = Path(f"/proc/{identity['pid']}/cgroup").read_text()
        identity['executable'] = str(Path(f"/proc/{identity['pid']}/exe").readlink())
        owners.append(identity)
    return {'complete': True, 'owners': owners, 'capabilities': ['rocm', expected],
            'environment': {'rocminfo': rocminfo, 'targets': targets,
                            'rocminfo_sha256': __import__('hashlib').sha256(info.encode()).hexdigest()}}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rocminfo', required=True)
    p.add_argument('--target', required=True)
    a = p.parse_args()
    print(json.dumps(probe(a.rocminfo, a.target)))
