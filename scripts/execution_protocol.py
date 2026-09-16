"""Version 1 CPU execution contract. Standard library only; no model runtime."""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import time

VERSION = 1
MAX_BUNDLE = 16 * 1024 * 1024
MAX_ARTIFACT = 4 * 1024 * 1024
MAX_TOTAL = 8 * 1024 * 1024
MAX_WIRE = 32 * 1024 * 1024


def now():
    return time.time_ns() // 1_000_000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.write-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(canonical(value) + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@contextlib.contextmanager
def locked(path, nonblocking=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | (fcntl.LOCK_NB if nonblocking else 0))
        yield


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', value):
        raise ValueError('invalid identifier')
    return value


def integer(value, minimum=1, maximum=2**53-1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError('integer outside permitted bounds')
    return value


def fields(value, required, optional=()):
    if not isinstance(value, dict) or set(value) - set(required) - set(optional) or set(required) - set(value):
        raise ValueError('invalid object fields')


def relative(value):
    if (not isinstance(value, str) or not value or len(value) > 240 or '\\' in value
            or any(ord(c) < 32 for c in value)):
        raise ValueError('invalid relative artifact path')
    p = PurePosixPath(value)
    if p.is_absolute() or any(x in ('', '.', '..', '.git') for x in value.split('/')):
        raise ValueError('unsafe artifact path')
    return value


def sha(value):
    if not isinstance(value, str) or not re.fullmatch('[a-f0-9]{64}', value):
        raise ValueError('invalid SHA-256')


def validate_request(r):
    common = ['schema_version', 'job_id', 'flow_id', 'worker_id', 'source', 'command',
              'timeout_ms', 'deadline', 'artifacts', 'created_at']
    if r.get('schema_version') == 2 and type(r['schema_version']) is int:
        fields(r, common + ['requirements', 'placement', 'host_config_hash'], ['progress'])
        from execution_hosts import validate_requirements
        validate_requirements(r['requirements'])
        fields(r['placement'], ['executor'])
        identifier(r['placement']['executor']); sha(r['host_config_hash'])
        if 'progress' in r:
            fields(r['progress'], ['path'])
            relative(r['progress']['path'])
            if r['progress']['path'].endswith('/'):
                raise ValueError('progress path must be a file')
    else:
        fields(r, common + ['resource'])
        if type(r['schema_version']) is not int or r['schema_version'] != VERSION or r['resource'] != 'cpu':
            raise ValueError('unsupported request contract')
    for k in ('job_id', 'flow_id', 'worker_id'):
        identifier(r[k])
    integer(r['created_at']); integer(r['deadline'])
    integer(r['timeout_ms'], 50, 3_600_000)
    if r['created_at'] >= r['deadline']:
        raise ValueError('request creation must precede deadline')
    fields(r['source'], ['commit', 'bundle_sha256'])
    if not isinstance(r['source']['commit'], str) or not re.fullmatch('[a-f0-9]{40}|[a-f0-9]{64}', r['source']['commit']):
        raise ValueError('source must be an exact Git commit')
    sha(r['source']['bundle_sha256'])
    command = r['command']
    if (not isinstance(command, list) or not 1 <= len(command) <= 64
            or any(not isinstance(x, str) or '\x00' in x for x in command)
            or not command[0] or sum(len(x) for x in command) > 8192):
        raise ValueError('command must be bounded argv')
    if not isinstance(r['artifacts'], list) or len(r['artifacts']) > 16:
        raise ValueError('too many artifact expectations')
    paths = set()
    for a in r['artifacts']:
        fields(a, ['path', 'required'], ['sha256'])
        relative(a['path'])
        if a['path'] in paths or type(a['required']) is not bool:
            raise ValueError('duplicate artifact or invalid required flag')
        paths.add(a['path'])
        if 'sha256' in a:
            sha(a['sha256'])
    return r


def validate_config(c):
    if c.get('schema_version') == 2:
        from execution_hosts import validate_host
        return validate_host(c)
    fields(c, ['schema_version', 'worker_id', 'host', 'root'])
    if type(c['schema_version']) is not int or c['schema_version'] != VERSION:
        raise ValueError('unsupported configuration version')
    identifier(c['worker_id'])
    if not isinstance(c['host'], str) or not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,95}', c['host']):
        raise ValueError('host must be an explicit SSH alias')
    p = Path(c['root'])
    if not p.is_absolute() or '..' in p.parts or len(p.parts) < 4 or str(p) != c['root']:
        raise ValueError('worker root must be a dedicated absolute directory')
    return c


def safe_file(root, name):
    relative(name)
    root = Path(root).resolve()
    p = root
    for part in name.split('/'):
        p = p / part
        if p.is_symlink():
            raise ValueError('symlinks are not transferable')
    if not p.resolve().is_relative_to(root):
        raise ValueError('path escapes artifact root')
    return p
