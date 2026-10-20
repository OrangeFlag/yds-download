#!/usr/bin/env python3
"""Export host metadata now; provision an explicitly mounted, empty disk later.

export --config services.json --bundle handoff.json [--catalog-state DIRECTORY]
provision --bundle handoff.json --mount /media/NEW --uuid UUID --output-config services-new.json
Neither command formats disks, installs configuration, nor starts services.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import uuid

import download
import services
import storage
import volume_catalog


def _relative(path, root):
    path = Path(path).resolve()
    relative = path.relative_to(root)
    if not relative.parts:
        raise ValueError('Expected a path below the disk root: ' + str(path))
    return str(relative)


def _read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _new_json(path, data):
    """Create private metadata without overwriting an existing file or symlink."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.open('x', encoding='utf-8') as stream:
        os.chmod(path, 0o600)
        json.dump(data, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    storage.sync_directory(path.parent)


def export(config_path, bundle, catalog_state=None):
    config = services.load(Path(config_path))
    root = volume_catalog._mounted(config['mount_root'], config['disk_uuid'])
    bundle = Path(bundle).expanduser().absolute()
    if bundle.resolve().is_relative_to(root):
        raise ValueError('Handoff bundle must be stored on the host')
    state = Path(catalog_state or Path(config['state_dir']) / 'volume-registry').expanduser().resolve()
    if state.is_relative_to(root):
        raise ValueError('Host catalog must be outside the source disk')
    values = services.variables(config, root)
    course = Path(services.expand(config['course_queue'], values)).resolve()
    queues, extras = [course], []
    for service in config['services'].values():
        command = [services.expand(arg, values) for arg in service['command']]
        for index, arg in enumerate(command[:-1]):
            if arg == '--queue':
                queues.append(Path(command[index + 1]).resolve())
            elif arg == '--destination':
                extras.append(Path(command[index + 1]).resolve())
    files, destinations = {}, set()
    for path in dict.fromkeys(queues):
        data = storage.read_queue(path)
        fresh = {key: value for key, value in data.items() if key in
                 ('retries', 'report_repo', 'mark', 'git_push', 'tuning', 'download_budget')}
        fresh['archives'] = []
        for row in data['archives']:
            destination = _relative(row['destination'], root)
            catalog = _relative(row['catalog'], root)
            definitions = _read(row['catalog'])
            if not isinstance(definitions.get('courses'), list) or not definitions.get('source'):
                raise ValueError('Invalid course catalog: ' + row['catalog'])
            files[catalog] = definitions
            job = {key: value for key, value in row.items() if key in
                   ('title', 'source', 'enabled', 'tuning')}
            job.update(destination=destination, catalog=catalog, state='pending')
            fresh['archives'].append(job)
            destinations.add(destination)
        files[_relative(path, root)] = fresh
    # Extra archives are saved independently of queues (e.g. the newest semester).
    extra_jobs = []
    for archive in dict.fromkeys(extras):
        destination = _relative(archive, root)
        if destination in destinations:
            continue
        definitions = _read(archive / '.download/catalog.json')
        selected = _read(archive / '.download/courses.json')
        indexed = {row['name']: row for row in definitions['courses']}
        if len(set(selected)) != len(selected) or any(name not in indexed for name in selected):
            raise ValueError('Invalid saved course selection: ' + str(archive))
        # Retain the saved processing order, including its explicit selection.
        definitions['courses'] = [dict(indexed[name], enabled=True) for name in selected] + [
            dict(row, enabled=False) for row in definitions['courses'] if row['name'] not in selected]
        catalog = destination + '/.download/catalog.json'
        files[catalog] = definitions
        extra_jobs.append(dict(title=definitions.get('title', archive.name),
                               source=definitions['source'], enabled=True, state='pending',
                               destination=destination, catalog=catalog))
        destinations.add(destination)
    files[_relative(course, root)]['archives'][:0] = extra_jobs
    payload = dict(schema=1, config=config, catalog_state=str(state),
                   volume_catalog=volume_catalog.load(state), files=files,
                   queues=[_relative(path, root) for path in dict.fromkeys(queues)])
    _new_json(bundle, payload)
    return payload


def _rebase(value, roots, target):
    if isinstance(value, str):
        for root in roots:
            if value == root or value.startswith(root.rstrip('/') + '/'):
                return str(target) + value[len(root):]
        return value
    if isinstance(value, list):
        return [_rebase(item, roots, target) for item in value]
    if isinstance(value, dict):
        return {key: _rebase(item, roots, target) for key, item in value.items()}
    return value


def provision(bundle, mount, identity, output_config):
    saved = _read(bundle)
    if saved.get('schema') != 1:
        raise ValueError('Unsupported handoff schema')
    config = copy.deepcopy(saved['config'])
    uuid.UUID(identity)
    if identity.lower() == config['disk_uuid'].lower():
        raise ValueError('New disk UUID must differ from the source disk UUID')
    root = Path(mount).expanduser().resolve()
    if root in [Path('/'), Path.home(), *map(Path, ('/home', '/usr', '/var', '/boot', '/etc'))]:
        raise ValueError('Refusing a system directory as the new disk')
    root = volume_catalog._mounted(root, identity)
    if any(path.name != 'lost+found' for path in root.iterdir()):
        raise ValueError('New disk must be empty (only lost+found is allowed)')
    output = Path(output_config).expanduser().absolute()
    state = Path(saved['catalog_state']).resolve()
    for host in (output.resolve(), state):
        if host.is_relative_to(root) or host.is_relative_to(Path(config['mount_root']).resolve()):
            raise ValueError('Output config and catalog must live on the host')
    if os.path.lexists(output):
        raise ValueError('Output configuration already exists: ' + str(output))
    files = {}
    for relative, data in saved['files'].items():
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts or not path.parts:
            raise ValueError('Unsafe handoff metadata path: ' + relative)
        files[root / path] = copy.deepcopy(data)
    for queue in saved['queues']:
        path = root / queue
        if path not in files:
            raise ValueError('Missing handoff queue: ' + queue)
        for row in files[path]['archives']:
            for field in ('destination', 'catalog'):
                relative = Path(row[field])
                if relative.is_absolute() or '..' in relative.parts or not relative.parts:
                    raise ValueError('Unsafe archive path: ' + str(relative))
                row[field] = str(root / relative)
            if Path(row['catalog']) not in files:
                raise ValueError('Missing handoff course catalog')
    roots = [config['mount_root'], *config.get('previous_mounts', [])]
    config = _rebase(config, roots, root)
    config.update(mount_root=str(root), disk_uuid=identity, previous_mounts=[])
    config.setdefault('environment', {}).update(YDS_VOLUME_CATALOG=str(state), YDS_VOLUME_UUID=identity)
    for service in config['services'].values():
        for key in ('YDS_VOLUME_CATALOG', 'YDS_VOLUME_UUID'):
            if key in service.get('environment', {}):
                service['environment'][key] = config['environment'][key]
    # Preserve offline inventory even when the source disk is already unplugged.
    catalog = volume_catalog._merge(volume_catalog.load(state), saved.get('volume_catalog'))
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    download.write_json(state / volume_catalog.CATALOG,
                        dict(payload=catalog, sha256=volume_catalog._digest(catalog)))
    for path, data in files.items():
        volume_catalog._mounted(root, identity)
        _new_json(path, data)
    volume_catalog.synchronize(root, identity, state)
    _new_json(output, config)
    return config


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='action', required=True)
    prepare = sub.add_parser('export', help='Capture definitions into a host-only metadata bundle')
    prepare.add_argument('--config', type=Path, default=services.DEFAULT_CONFIG)
    prepare.add_argument('--bundle', type=Path, required=True)
    prepare.add_argument('--catalog-state', type=Path)
    target = sub.add_parser('provision', help='Write fresh metadata to an empty mounted disk; never format')
    target.add_argument('--bundle', type=Path, required=True)
    target.add_argument('--mount', type=Path, required=True)
    target.add_argument('--uuid', required=True)
    target.add_argument('--output-config', type=Path, required=True)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.action == 'export':
            result = export(args.config, args.bundle, args.catalog_state)
            print(f'Handoff saved: {args.bundle}; metadata files={len(result["files"])}')
        else:
            provision(args.bundle, args.mount, args.uuid, args.output_config)
            print(f'Fresh disk prepared; configuration saved: {args.output_config}; services not started')
    except (OSError, ValueError, KeyError, TypeError) as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
