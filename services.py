#!/usr/bin/env python3
"""Control download/upload user services from one private JSON configuration."""
import argparse
import fcntl
import json
import os
from pathlib import Path
from string import Template
import subprocess
import sys
import uuid

import storage

NAMES = ('queue', 'range', 'nextcloud', 'opencloud')
RUNNING = {'active', 'activating', 'reloading', 'deactivating'}
DEFAULT_CONFIG = Path.home() / '.config/yds-download/services.json'
CONTROL_LOCK = Path.home() / '.local/state/yds-download/services.lock'
PROPERTIES = dict(KillSignal='SIGINT', KillMode='control-group', TimeoutStopSec='2min',
                  SuccessExitStatus='130 143', Restart='on-failure', RestartSec='15s')


def unit(name):
    return 'yds-download-' + name + '.service'


def variables(config, root):
    return dict(home=str(Path.home()), repo=config['repo'], state_dir=config['state_dir'],
                mount_root=str(root))


def expand(value, values):
    try:
        return Template(value).substitute(values)
    except (KeyError, ValueError) as error:
        raise ValueError('Invalid config template: ' + str(error)) from error


def load(path):
    config = json.loads(path.read_text())
    if not isinstance(config, dict) or not isinstance(config.get('services'), dict):
        raise ValueError('Config must contain a services object')
    uuid.UUID(config['disk_uuid'])
    for key in ('repo', 'state_dir', 'mount_root'):
        value = Path(config[key]).expanduser()
        if not value.is_absolute():
            raise ValueError(key + ' must be an absolute path')
        config[key] = str(value)
    previous = config.get('previous_mounts', [])
    if not isinstance(previous, list) or any(not isinstance(p, str) or not Path(p).is_absolute()
                                             or p == '/' for p in previous):
        raise ValueError('previous_mounts must contain absolute disk mount paths')
    if set(config['services']) - set(NAMES):
        raise ValueError('Unknown service; choose ' + ', '.join(NAMES))
    for name, service in config['services'].items():
        if not isinstance(service, dict):
            raise ValueError('service must be an object: ' + name)
        command = service['command']
        if not isinstance(command, list) or not command or any(not isinstance(s, str) or '\0' in s for s in command):
            raise ValueError('service command must be a nonempty list of strings: ' + name)
        start_command(config, name, config['mount_root'])
    if 'queue_tuning' in config:
        from settings import resolve
        resolve(config['queue_tuning'])
        expand(config['course_queue'], variables(config, config['mount_root']))
    return config


def states(names):
    result = subprocess.run(['systemctl', '--user', 'show', *[unit(n) for n in names],
                             '-p', 'Id', '-p', 'ActiveState'], capture_output=True, text=True)
    rows = [dict(line.split('=', 1) for line in block.splitlines() if '=' in line)
            for block in result.stdout.strip().split('\n\n')]
    found = {row.get('Id'): row.get('ActiveState') for row in rows}
    if any(not found.get(unit(n)) for n in names):
        raise ValueError('Cannot query user systemd: ' + result.stderr.strip())
    return {n: found[unit(n)] for n in names}


def start_command(config, name, root):
    service = config['services'][name]
    values = variables(config, root)
    properties = {**PROPERTIES, 'WorkingDirectory': config['repo'],
                  **config.get('properties', {}), **service.get('properties', {})}
    environment = {**config.get('environment', {}), **service.get('environment', {})}
    if any(not isinstance(k, str) or not isinstance(v, str) or '\0' in v or '\n' in k or '=' in k
           for k, v in [*properties.items(), *environment.items()]):
        raise ValueError('Properties and environment must be string maps')
    return ['systemd-run', '--user', '--collect', '--quiet', '--service-type=exec',
            '--expand-environment=no', '--unit=' + unit(name),
            *['--property=' + k + '=' + expand(v, values) for k, v in properties.items()],
            *['--setenv=' + k + '=' + expand(v, values) for k, v in environment.items()],
            sys.executable, str(Path(config['repo']) / 'storage.py'),
            '--uuid', config['disk_uuid'], '--mount-root', str(root),
            '--state-dir', config['state_dir'],
            '--log-file', str(Path(config['state_dir']) / (name + '.log')), '--',
            *[expand(arg, values) for arg in service['command']]]


def prepare(config, current):
    root = storage.select_mount(config['disk_uuid'], config['mount_root'])
    if root is None:
        raise ValueError('Writable download disk is unavailable; nothing started')
    root = root.resolve()
    device = root.stat().st_dev
    if (not storage.probe(root) or storage.select_mount(config['disk_uuid'], root) != root
            or not storage.mounted_ok(root, device)):
        raise ValueError('Download disk changed or became unavailable; nothing started')
    if not (Path(config['repo']) / 'storage.py').is_file():
        raise ValueError('repo does not contain storage.py')
    queue = None
    if 'queue_tuning' in config:
        queue = Path(expand(config['course_queue'], variables(config, root))).resolve()
        if not queue.is_relative_to(root) or queue.stat().st_dev != device:
            raise ValueError('course_queue must be on the download disk')
    state = Path(config['state_dir'])
    record = state / (config['disk_uuid'] + '.json')
    saved = json.loads(record.read_text()) if record.exists() else {}
    previous = config.get('previous_mounts', [])
    changed = ((saved.get('mount', config['mount_root']) != str(root))
               or not set(previous).issubset(saved.get('previous', [])))
    busy = any(value in RUNNING for value in current.values())
    if changed and busy:
        raise ValueError('Mount migration requires all services stopped; use restart without a service name')
    if not busy:
        storage.migrate(config['mount_root'], root, config['disk_uuid'], state, previous=previous)
    if queue is not None:
        data = json.loads(queue.read_text())
        if data.get('tuning') != config['queue_tuning']:
            if busy:
                raise ValueError('Tuning change requires all services stopped; use restart without a service name')
            if not storage.mounted_ok(root, device) or queue.stat().st_dev != device:
                raise storage.StorageUnavailable(queue)
            from download import write_json
            write_json(queue, {**data, 'tuning': config['queue_tuning']})
    return root


def control(config, action, names):
    current = states(NAMES)
    if action == 'status':
        for name in names:
            print(f'{name:10} {current[name]:12} {Path(config["state_dir"]) / (name + ".log")}')
        return
    if action in ('down', 'restart'):
        stopping = [unit(n) for n in names if current[n] in RUNNING or current[n] == 'failed']
        if stopping:
            subprocess.run(['systemctl', '--user', 'stop', *stopping], check=True)
        if action == 'down':
            return
        current = states(NAMES)
    pending = [n for n in names if n in config['services'] and current[n] not in RUNNING]
    if any(current[n] == 'deactivating' for n in names):
        raise ValueError('Services are still stopping; wait before up')
    if not pending:
        return
    root = prepare(config, current)
    commands = [(name, start_command(config, name, root)) for name in pending]
    started = []
    try:
        for name, command in commands:
            if current[name] == 'failed':
                subprocess.run(['systemctl', '--user', 'reset-failed', unit(name)], check=True)
            started.append(unit(name))
            subprocess.run(command, check=True)
            print('UP: ' + name)
    except BaseException:
        subprocess.run(['systemctl', '--user', 'stop', *started], check=False)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('action', choices=('up', 'down', 'restart', 'status'))
    parser.add_argument('services', nargs='*', metavar='SERVICE', help=', '.join(NAMES) + '; up: configured; other actions: all')
    args = parser.parse_args()
    os.umask(0o077)
    try:
        config = load(args.config.expanduser())
        names = list(dict.fromkeys(args.services or (config['services'] if args.action == 'up' else NAMES)))
        allowed = config['services'] if args.action == 'up' else NAMES
        if set(names) - set(allowed):
            raise ValueError('Service is not configured or unknown: ' + ', '.join(set(names) - set(allowed)))
        if args.action == 'status':
            control(config, args.action, names)
            return 0
        CONTROL_LOCK.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with CONTROL_LOCK.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            control(config, args.action, names)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as error:
        print(f'ERROR: {error.cmd[0]} exited {error.returncode}; see journalctl --user', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
