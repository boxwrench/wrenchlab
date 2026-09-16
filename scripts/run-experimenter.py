#!/usr/bin/env python3
"""One bounded, durable experiment session over the existing Broker.

This is intentionally a small session/controller layer, not another job system:
the Broker remains authoritative for transport, ownership, cleanup and evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

from execution_broker import Broker
from execution_protocol import canonical, digest, fields, identifier, integer, locked, now, read, save
from execution_hosts import validate_host, validate_requirements

ROOT = Path(__file__).resolve().parents[1]
WORKLOAD = r'''
import hashlib, json, os, sys, tempfile, time

progress_path, result_path, steps, delay_ms, mode = sys.argv[2:7]
steps, delay_ms = int(steps), int(delay_ms)

def write(path, value):
    directory = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.progress-', dir=directory)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, sort_keys=True, separators=(',', ':'))
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def update(state, step, message, **extra):
    write(progress_path, {'state': state, 'step': step, 'total': steps,
                          'message': message, 'updated_at': time.time_ns() // 1_000_000, **extra})

started = time.time_ns() // 1_000_000
update('RUNNING', 0, 'started', mode=mode)
checksums = []
for step in range(1, steps + 1):
    if mode == 'accelerator':
        try:
            import torch
            if not torch.cuda.is_available():
                raise RuntimeError('requested accelerator is unavailable')
            a = torch.ones((512, 512), device='cuda') * step
            value = float((a @ a).sum().item())
            device = torch.cuda.get_device_name(0)
        except Exception as exc:
            update('FAILED', step - 1, 'accelerator check failed', error=str(exc))
            raise
    else:
        value = sum((step * i) % 997 for i in range(200000))
        device = 'cpu'
    checksums.append(value)
    update('RUNNING', step, f'completed step {step}/{steps}', device=device,
           value=value)
    time.sleep(delay_ms / 1000)
payload = {'started_at': started, 'ended_at': time.time_ns() // 1_000_000,
           'steps': steps, 'mode': mode, 'checksums_sha256': hashlib.sha256(
               json.dumps(checksums, sort_keys=True).encode()).hexdigest(),
           'device': device}
write(result_path, payload)
update('SUCCEEDED', steps, 'complete', result_sha256=hashlib.sha256(
    json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
'''.strip()


def author_host(value):
    """Validate the author placement without importing executor-only details."""
    fields(value, ['host_id', 'transport'], ['capabilities', 'description', 'host'])
    identifier(value['host_id'])
    if value['transport'] not in ('local', 'ssh'):
        raise ValueError('unsupported author host transport')
    if value['transport'] == 'ssh':
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,95}', value.get('host', '')):
            raise ValueError('SSH author host alias required')
    elif 'host' in value:
        raise ValueError('local author host does not take an SSH alias')
    if 'capabilities' in value:
        if not isinstance(value['capabilities'], list) or len(value['capabilities']) > 32:
            raise ValueError('invalid author capabilities')
        for item in value['capabilities']:
            identifier(item)
    for key in ('description',):
        if key in value and (not isinstance(value[key], str) or len(value[key]) > 4096):
            raise ValueError('invalid author host description')
    return value


def state_path(campaign):
    return Path(campaign).resolve() / 'experimenter' / 'state.json'


def load_state(campaign):
    return read(state_path(campaign))


def save_state(campaign, state):
    save(state_path(campaign), state)


def transition(state, target, note=None):
    current = state['state']
    if current == target:
        return
    allowed = {
        'CREATED': {'READY', 'FAILED'}, 'READY': {'QUEUED', 'FAILED'},
        'QUEUED': {'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'},
        'RUNNING': {'SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'},
        'SUCCEEDED': set(), 'FAILED': set(), 'CANCELLED': set(), 'TIMED_OUT': set(),
    }
    if target not in allowed.get(current, set()):
        raise ValueError(f'invalid experiment transition {current} -> {target}')
    stamp = now()
    state['transitions'].append({'from': current, 'to': target, 'at': stamp,
                                 'note': note or ''})
    state['state'] = target
    if target in ('RUNNING',):
        state['started_at'] = state.get('started_at') or stamp
    if target in ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'):
        state['ended_at'] = stamp


def result_state(result):
    return {'success': 'SUCCEEDED', 'failed': 'FAILED', 'cancelled': 'CANCELLED',
            'timeout': 'TIMED_OUT'}[result['execution']]


def refresh(broker, state, campaign):
    if not state.get('job_id'):
        return state
    observation = broker.action('inspect', state['job_id'])
    state['executor_identity'] = state['executor_host']['host_id']
    state['worker_status'] = observation.get('worker_status')
    state['last_observation_at'] = now()
    state['resource_ownership'] = observation.get('job', {}).get('resource')
    if observation.get('job', {}).get('state') == 'RUNNING' and state['state'] == 'QUEUED':
        transition(state, 'RUNNING', 'executor process is owned and running')
    progress = observation.get('progress')
    if progress and progress.get('state') != 'INVALID':
        state['progress'] = progress
        state['last_progress_at'] = progress.get('updated_at', now())
    result = observation.get('result')
    if result:
        if result.get('resource') is not None:
            state['resource_ownership'] = result['resource']
        state['started_at'] = state.get('started_at') or result.get('started_at')
        target = result_state(result)
        if state['state'] not in ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'):
            transition(state, target, result.get('error') or result.get('execution'))
        state['result_summary'] = {
            'execution': result['execution'], 'exit_code': result['exit_code'],
            'cleanup': result['cleanup'], 'error': result.get('error'),
            'resource_cleanup': result.get('resource', {}).get('cleanup') if result.get('resource') else 'not_requested',
            'progress': result.get('progress'),
        }
        if state.get('evidence_packet') is None:
            try:
                collected = broker.action('collect', state['job_id'])
                state['evidence_packet'] = str(Path(campaign).resolve() / 'execution' / 'jobs' /
                                               state['job_id'] / 'evidence' / 'packet.json')
                state['artifact_hashes'] = {a['path']: a['sha256'] for a in result['artifacts']}
                state['evidence_integrity'] = 'verified'
                state['partial_artifacts'] = result['execution'] != 'success'
            except Exception as exc:
                state['evidence_integrity'] = 'unavailable'
                state['error'] = str(exc)
    save_state(campaign, state)
    return state


def runtime_command(executor, runtime, mode, steps, delay_ms):
    runtimes = executor.get('runtimes', {})
    if runtime not in runtimes:
        raise ValueError(f'executor host has no configured runtime: {runtime}')
    launcher = runtimes[runtime]['launcher']
    return launcher + ['-c', WORKLOAD, 'wrenchlab-experiment', 'progress.json',
                       'experiment-result.json', str(steps), str(delay_ms), mode]


def initialize(args):
    executor = read(args.executor_host)
    validate_host(executor)
    if args.runtime not in executor.get('runtimes', {}):
        raise ValueError(f'executor host has no configured runtime: {args.runtime}')
    author = author_host(read(args.author_host))
    integer(args.deadline); integer(args.timeout_ms, 50, 3_600_000)
    if args.native_session_id is not None:
        identifier(args.native_session_id)
    if args.deadline <= now():
        raise ValueError('deadline must be in the future')
    if args.steps < 1 or args.steps > 120 or args.delay_ms < 0 or args.delay_ms > 60_000:
        raise ValueError('bounded workload parameters required')
    campaign = Path(args.campaign).resolve()
    if state_path(campaign).exists():
        raise ValueError('experiment session already exists')
    requirements = read(args.requirements) if args.requirements else {'capabilities': ['cpu'], 'resources': {}}
    validate_requirements(requirements)
    if args.mode == 'accelerator' and 'gpu' not in requirements.get('resources', {}):
        raise ValueError('accelerator mode requires an exclusive GPU requirement')
    broker = Broker(campaign)
    broker.create(executor, args.deadline, args.owner, args.objective,
                  max_jobs=1, author_host=author)
    state = {
        'schema_version': 1, 'experiment_id': identifier(campaign.name),
        'session_id': identifier(campaign.name), 'native_session_id': args.native_session_id,
        'campaign_id': identifier(campaign.name),
        'created_at': now(), 'state': 'CREATED', 'owner': args.owner,
        'objective': args.objective, 'author_host': author,
        'executor_host': {'host_id': executor['host_id'], 'transport': executor['transport'],
                          'config_sha256': digest(canonical(executor))},
        'source': {'repo': str(Path(args.repo).resolve(strict=True)), 'revision': args.revision},
        'workload': {'runtime': args.runtime, 'mode': args.mode, 'steps': args.steps,
                     'delay_ms': args.delay_ms, 'progress_path': 'progress.json',
                     'result_path': 'experiment-result.json',
                     'workload_sha256': hashlib.sha256(WORKLOAD.encode()).hexdigest()},
        'requirements': requirements,
        'timeout_ms': args.timeout_ms, 'job_id': None, 'flow_id': broker.campaign.name,
        'transitions': [], 'progress': None, 'last_progress_at': None,
        'started_at': None, 'ended_at': None, 'executor_identity': executor['host_id'],
        'resource_ownership': None, 'evidence_packet': None, 'evidence_integrity': None,
    }
    save_state(campaign, state)
    return state


def start(args):
    campaign = Path(args.campaign).resolve(); state = load_state(campaign)
    if state['state'] == 'CREATED':
        transition(state, 'READY', 'broker admission')
        save_state(campaign, state)
    elif state['state'] in ('QUEUED', 'RUNNING'):
        return refresh(Broker(campaign), state, campaign)
    elif state['state'] != 'READY':
        raise ValueError('experiment is already terminal')
    broker = Broker(campaign)
    executor = read(Broker(campaign).state_file)['worker']
    spec = {
        'job_id': state['experiment_id'] + '-job', 'repo': state['source']['repo'],
        'revision': state['source']['revision'],
        'command': runtime_command(executor, state['workload']['runtime'], state['workload']['mode'],
                                   state['workload']['steps'], state['workload']['delay_ms']),
        'timeout_ms': state['timeout_ms'],
        'artifacts': [{'path': 'progress.json', 'required': True},
                      {'path': 'experiment-result.json', 'required': False}],
        'requirements': state['requirements'],
        'placement': {'executor': executor['host_id']}, 'progress': {'path': 'progress.json'},
    }
    request = broker.prepare(spec)
    state['job_id'] = request['job_id']; state['flow_id'] = request['flow_id']
    transition(state, 'QUEUED', 'stable job request prepared')
    save_state(campaign, state)
    response = broker.action('submit', state['job_id'])
    if response.get('job', {}).get('state') == 'RUNNING':
        transition(state, 'RUNNING', 'executor accepted and started')
    elif response.get('result'):
        transition(state, result_state(response['result']), response['result'].get('execution'))
    save_state(campaign, state)
    return refresh(broker, state, campaign)


def command(args):
    campaign = Path(args.campaign).resolve()
    if args.operation == 'init':
        result = initialize(args)
    elif args.operation == 'start':
        result = start(args)
    else:
        state = load_state(campaign); broker = Broker(campaign)
        if args.operation == 'status':
            result = refresh(broker, state, campaign) if state.get('job_id') else state
        elif args.operation == 'cancel':
            if state['state'] not in ('QUEUED', 'RUNNING'):
                raise ValueError('only active experiments can be cancelled')
            broker.action('cancel', state['job_id'])
            deadline = time.monotonic() + args.wait_seconds
            while time.monotonic() < deadline:
                state = refresh(broker, state, campaign)
                if state['state'] in ('CANCELLED', 'FAILED', 'TIMED_OUT', 'SUCCEEDED'):
                    break
                time.sleep(.2)
            result = state
        elif args.operation == 'watch':
            deadline = time.monotonic() + args.wait_seconds
            while time.monotonic() < deadline:
                state = refresh(broker, state, campaign)
                if state['state'] in ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT'):
                    break
                time.sleep(args.poll_seconds)
            result = state
        else:
            raise ValueError('unsupported experimenter operation')
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--campaign', required=True)
    sub = parser.add_subparsers(dest='operation', required=True)
    init = sub.add_parser('init')
    init.add_argument('--author-host', required=True); init.add_argument('--executor-host', required=True)
    init.add_argument('--repo', required=True); init.add_argument('--revision', required=True)
    init.add_argument('--runtime', required=True); init.add_argument('--mode', choices=['cpu', 'accelerator'], default='cpu')
    init.add_argument('--steps', type=int, default=12); init.add_argument('--delay-ms', type=int, default=5000)
    init.add_argument('--timeout-ms', type=int, default=180000); init.add_argument('--deadline', type=int, required=True)
    init.add_argument('--owner', required=True); init.add_argument('--objective', required=True); init.add_argument('--requirements')
    init.add_argument('--native-session-id')
    sub.add_parser('start').add_argument('--executor-host')
    sub.add_parser('status')
    cancel = sub.add_parser('cancel'); cancel.add_argument('--wait-seconds', type=float, default=30)
    watch = sub.add_parser('watch'); watch.add_argument('--wait-seconds', type=float, default=300); watch.add_argument('--poll-seconds', type=float, default=1)
    args = parser.parse_args()
    command(args)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'error': f'{type(exc).__name__}: {exc}'}))
        sys.exit(1)
