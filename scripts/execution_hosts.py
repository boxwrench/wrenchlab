"""Small static host/requirement contract. Placement is not model placement."""
from pathlib import Path
import re
from execution_protocol import fields, identifier, integer, canonical, digest


def argv(value):
    if (not isinstance(value, list) or not 1 <= len(value) <= 64 or
            any(not isinstance(x, str) or '\0' in x for x in value) or
            not value[0].startswith('/') or sum(map(len, value)) > 8192):
        raise ValueError('configured command must be bounded absolute argv')


def directory(value):
    p = Path(value)
    if not p.is_absolute() or '..' in p.parts or len(p.parts) < 4 or str(p) != value:
        raise ValueError('dedicated absolute directory required')


def capabilities(value):
    if not isinstance(value, list) or len(value) > 32 or len(set(value)) != len(value):
        raise ValueError('capabilities must be a unique bounded list')
    for item in value:
        identifier(item)


def validate_host(c):
    fields(c, ['schema_version', 'worker_id', 'host_id', 'transport', 'root',
               'capabilities', 'capability_evidence', 'resource_root', 'resources', 'managed_services'], ['host', 'runtimes'])
    if c['schema_version'] != 2 or type(c['schema_version']) is not int:
        raise ValueError('host contract requires version 2')
    identifier(c['host_id']); identifier(c['worker_id'])
    if c['transport'] not in ('local', 'ssh'):
        raise ValueError('unsupported transport')
    if c['transport'] == 'ssh':
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,95}', c.get('host', '')):
            raise ValueError('SSH alias required')
    elif 'host' in c:
        raise ValueError('local transport does not take an SSH alias')
    directory(c['root']); directory(c['resource_root'])
    if c['root'] == c['resource_root']:
        raise ValueError('host resource ledger must be distinct from worker root')
    capabilities(c['capabilities'])
    fields(c['capability_evidence'], ['verified_at', 'description'])
    integer(c['capability_evidence']['verified_at'])
    if not isinstance(c['capability_evidence']['description'], str) or not 1 <= len(c['capability_evidence']['description']) <= 4096:
        raise ValueError('capability evidence required')
    if not isinstance(c['resources'], dict) or set(c['resources']) - {'gpu'}:
        raise ValueError('only exclusive GPU resources implemented in this phase')
    for name, r in c['resources'].items():
        fields(r, ['resource_id', 'capabilities', 'probe', 'environment'])
        if not re.fullmatch(r'gpu:[A-Za-z0-9_-]{1,64}', r['resource_id']):
            raise ValueError('invalid GPU resource identity')
        capabilities(r['capabilities']); argv(r['probe'])
        if not set(r['capabilities']) <= set(c['capabilities']):
            raise ValueError('resource capabilities absent on host')
        if not isinstance(r['environment'], dict) or set(r['environment']) - {'HIP_VISIBLE_DEVICES', 'ROCR_VISIBLE_DEVICES', 'LD_LIBRARY_PATH'}:
            raise ValueError('unsupported GPU environment key')
        for k, v in r['environment'].items():
            if not isinstance(v, str) or '\0' in v or len(v) > 1024:
                raise ValueError('invalid GPU environment value')
    if not isinstance(c['managed_services'], dict) or len(c['managed_services']) > 8:
        raise ValueError('invalid managed services')
    if 'runtimes' in c:
        if not isinstance(c['runtimes'], dict) or len(c['runtimes']) > 16:
            raise ValueError('invalid runtimes')
        for name, runtime in c['runtimes'].items():
            identifier(name)
            fields(runtime, ['launcher'])
            argv(runtime['launcher'])
    resource_ids = {r['resource_id'] for r in c['resources'].values()}
    for sid, s in c['managed_services'].items():
        identifier(sid)
        fields(s, ['unit', 'conflicts_with', 'health', 'expects_owner'])
        if not re.fullmatch(r'[A-Za-z0-9_.@-]+\.service', s['unit']):
            raise ValueError('explicit systemd user service unit required')
        if not isinstance(s['conflicts_with'], list) or not set(s['conflicts_with']) <= resource_ids:
            raise ValueError('unknown conflicting resource')
        argv(s['health'])
        if type(s['expects_owner']) is not bool:
            raise ValueError('expects_owner must be boolean')
    return c


def validate_requirements(r):
    fields(r, ['capabilities', 'resources'])
    capabilities(r['capabilities'])
    if not isinstance(r['resources'], dict) or set(r['resources']) - {'gpu'}:
        raise ValueError('unsupported resource requirement')
    if 'gpu' in r['resources']:
        fields(r['resources']['gpu'], ['exclusive'])
        if r['resources']['gpu']['exclusive'] is not True:
            raise ValueError('GPU must be exclusive')
    return r


def admit(host, request):
    validate_host(host)
    validate_requirements(request['requirements'])
    if request['placement']['executor'] != host['host_id']:
        raise ValueError('executor placement does not match host')
    if request['host_config_hash'] != digest(canonical(host)):
        raise ValueError('immutable host configuration mismatch')
    if not set(request['requirements']['capabilities']) <= set(host['capabilities']):
        raise ValueError('host lacks required capabilities')
    if not set(request['requirements']['resources']) <= set(host['resources']):
        raise ValueError('host lacks required resources')
    return host
