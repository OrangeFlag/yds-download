#!/usr/bin/env python3
"""Remember verified files on removable volumes without requiring simultaneous mounts."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid as uuidlib

import download as d
import file_hashes
from local_paths import local_path
import storage

CATALOG = 'volume-catalog.json'
MIRROR_DIR = '.yds-download'


def _digest(payload):
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def load(directory):
    """Read a validated catalog, or None when no snapshot exists."""
    path = Path(directory) / CATALOG
    try:
        saved = json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return None
    except (ValueError, UnicodeError) as error:
        raise ValueError(f'Damaged volume catalog: {path}') from error
    try:
        payload = saved['payload']
        if (payload['schema'] != 1 or not isinstance(payload['volumes'], dict)
                or saved['sha256'] != _digest(payload)):
            raise ValueError('Invalid catalog checksum or schema')
        for identity, volume in payload['volumes'].items():
            uuidlib.UUID(identity)
            if (type(volume['updated_ns']) is not int or volume['updated_ns'] < 0
                    or not isinstance(volume['archives'], dict)):
                raise ValueError('Invalid catalog volume')
    except (TypeError, KeyError, ValueError) as error:
        raise ValueError(f'Damaged volume catalog: {path}') from error
    return payload


def _merge(*catalogs):
    merged = dict(schema=1, volumes={})
    for catalog in catalogs:
        if catalog is None:
            continue
        for identity, volume in catalog['volumes'].items():
            old = merged['volumes'].get(identity)
            if old is None or volume['updated_ns'] > old['updated_ns']:
                merged['volumes'][identity] = volume
            elif volume['updated_ns'] == old['updated_ns'] and volume != old:
                raise ValueError('Conflicting snapshots of volume ' + identity)
    return merged


def _mounted(root, identity):
    uuidlib.UUID(identity)
    root = Path(root).resolve()
    selected = storage.select_mount(identity, root)
    if (selected is None or selected.resolve() != root
            or not storage.mounted_ok(root, root.stat().st_dev)
            or not storage.probe(root)):
        raise ValueError(f'Volume {identity} is not mounted writable at {root}')
    return root


@contextmanager
def _guarded_catalog(root, identity, state):
    root = _mounted(root, identity)
    state = Path(state).expanduser().resolve()
    if state.is_relative_to(root):
        raise ValueError('Host catalog must live outside the removable volume')
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (state / '.volume-catalog.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        catalogs, errors = [], []
        for directory in (state, root / MIRROR_DIR):
            try:
                catalogs.append(load(directory))
            except ValueError as error:
                catalogs.append(None)
                errors.append(error)
        if not any(catalog is not None for catalog in catalogs) and errors:
            raise errors[0]  # Never overwrite the last surviving evidence.
        yield root, state, _merge(*catalogs)


def _publish(root, identity, state, catalog):
    # An offline disk must never redirect the backup into an ordinary empty folder.
    _mounted(root, identity)
    disk_state = root / MIRROR_DIR
    if not disk_state.exists():
        disk_state.mkdir(mode=0o700)
        storage.sync_directory(root)
    if not disk_state.is_dir() or disk_state.stat().st_dev != root.stat().st_dev:
        raise ValueError('Volume catalog mirror must be on the connected disk')
    signed = dict(payload=catalog, sha256=_digest(catalog))
    # Host first: even a failed/unplugged disk cannot erase its previous inventory.
    d.write_json(state / CATALOG, signed)
    _mounted(root, identity)
    d.write_json(disk_state / CATALOG, signed)
    return catalog


def synchronize(root, identity, state, label=None):
    """Reconcile the host and inserted disk; keep other disconnected volumes."""
    with _guarded_catalog(root, identity, state) as (root, state, catalog):
        if identity not in catalog['volumes']:
            catalog['volumes'][identity] = dict(
                updated_ns=time.time_ns(), label=label or identity, archives={})
        return _publish(root, identity, state, catalog)


def _archives(root, queues, archives):
    found = {Path(a).resolve() for a in archives}
    for queue in queues:
        queue = Path(queue).resolve()
        if not queue.is_relative_to(root):
            raise ValueError('Queue is not on this volume: ' + str(queue))
        for row in storage.read_queue(queue)['archives']:
            found.add(Path(row['destination']).resolve())
    if not found:
        raise ValueError('Scan needs at least one --archive or --queue')
    for archive in sorted(found):
        if not archive.is_relative_to(root):
            raise ValueError('Archive is not on this volume: ' + str(archive))
    return sorted(found)


def _recovery_inventory(root, archive):
    """Inventory retained bodies without treating them as SHA-verified files.

    Includes previous/rejected bodies saved during repeated Range recovery.
    The raw data and control files remain on the original volume.
    """
    recovery = {}
    parent = (archive / 'corrupted').resolve()
    device = root.stat().st_dev
    for failure in sorted(parent.glob('**/failed.json')):
        for event in json.loads(failure.read_text(encoding='utf-8')):
            current = event.get('corrupted_path')
            if current is None:
                current = str(local_path(failure.parent, event['path']).relative_to(archive))
            paths = [current]
            for key in ('preserved_archive_paths', 'sha256_rejected_paths'):
                paths.extend(event.get(key, []))
            for relative in dict.fromkeys(paths):
                if not isinstance(relative, str):
                    raise ValueError('Unsafe corrupted path')
                name = Path(relative)
                body = (archive / name).resolve()
                if (name.is_absolute() or '..' in name.parts or
                        not body.is_relative_to(parent) or not body.is_relative_to(root)):
                    raise ValueError('Unsafe corrupted path: ' + relative)
                metadata = {}
                for suffix, key in (('', 'body_bytes'), ('.ranges.json', 'has_ranges'),
                                    ('.aria2', 'has_aria2')):
                    candidate = (archive / (relative + suffix)).resolve()
                    if not candidate.is_relative_to(parent):
                        raise ValueError('Unsafe corrupted path: ' + relative + suffix)
                    try:
                        stat_result = candidate.stat()
                    except FileNotFoundError:
                        stat_result = None
                    if stat_result and (stat_result.st_dev != device
                                        or not stat.S_ISREG(stat_result.st_mode)):
                        raise ValueError('Unsafe corrupted artifact: ' + str(candidate))
                    if suffix:
                        metadata[key] = stat_result is not None
                    elif stat_result is not None:
                        metadata[key] = stat_result.st_size
                recovery[relative] = dict(
                    source_path=event['path'], expected_sha256=event['sha256'],
                    expected_size=event['size'],
                    status='retained_unverified' if 'body_bytes' in metadata else 'missing',
                    **metadata)
    return recovery


def _catalog_archive(root, archive):
    state = archive / '.download'
    manifests = sorted(state.glob('*-manifest.json'), key=lambda p: (p.stat().st_mtime_ns, str(p)))
    if not manifests:
        return None  # Not started; do not erase any earlier observation.
    source = json.loads((state / 'source.json').read_text())
    if not source.get('source'):
        raise ValueError('Missing archive source identity: ' + str(archive))
    entries = {}
    for manifest in manifests:
        for item in json.loads(manifest.read_text()):
            name, size, sha = item['path'], item['size'], item['sha256']
            if (not isinstance(name, str) or type(size) is not int or size < 0
                    or not isinstance(sha, str) or not re.fullmatch(r'[a-fA-F0-9]{64}', sha)):
                raise ValueError('Invalid archive manifest item')
            entries[name] = dict(size=size, sha256=sha.lower(), status='expected')
    journal = state / 'events.jsonl'
    proofs = d.EventIndex(journal, repair=False, checkpoint=True, checkpoint_read_only=True).refresh()
    base = root.resolve()
    device = base.stat().st_dev
    for name, row in entries.items():
        event = proofs.get(name, {})
        primary = local_path(archive, name)
        candidates = ([storage.saved_local(archive, event['local'])] if event.get('local') else []) + [primary]
        for candidate in dict.fromkeys(candidates):
            resolved = candidate.resolve()
            if not resolved.is_relative_to(base):
                continue
            try:
                current = resolved.stat()
            except FileNotFoundError:
                continue
            if (not stat.S_ISREG(current.st_mode) or current.st_dev != device
                    or current.st_size != row['size'] or Path(str(resolved) + '.aria2').exists()):
                continue
            same = (event.get('status') == 'valid'
                    and (event.get('size'), event.get('sha256')) == (row['size'], row['sha256'])
                    and event.get('source', source['source']) == source['source']
                    and (not event.get('local') or storage.saved_local(archive, event['local']).resolve() == resolved))
            if ((same and file_hashes.proof_current(resolved, event, root=archive))
                    or (file_hashes.load(resolved, root=archive) or {}).get('sha256') == row['sha256']):
                row.update(status='verified', local=str(resolved.relative_to(base)))
                break
    return dict(source=source['source'], title=source.get('title', ''), scanned_ns=time.time_ns(),
                files=entries, recovery=_recovery_inventory(root, archive))


def snapshot(root, identity, state, *, archives=(), queues=(), label=None):
    """Observe current disk; never reinterpret an offline volume as missing."""
    with _guarded_catalog(root, identity, state) as (root, state, catalog):
        found = _archives(root, queues, archives)
        previous = catalog['volumes'].get(identity, {})
        current_archives = dict(previous.get('archives', {}))
        for archive in found:
            details = _catalog_archive(root, archive)
            if details is not None:
                current_archives[str(archive.relative_to(root))] = details
        catalog['volumes'][identity] = dict(
            updated_ns=max(time.time_ns(), previous.get('updated_ns', 0) + 1),
            label=label or previous.get('label', identity), archives=current_archives)
        return _publish(root, identity, state, catalog)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('snapshot', 'sync', 'show'))
    parser.add_argument('--state-dir', type=Path,
                        default=Path.home() / '.local/state/yds-download/volume-registry')
    parser.add_argument('--mount', type=Path)
    parser.add_argument('--uuid')
    parser.add_argument('--label')
    parser.add_argument('--archive', type=Path, action='append', default=[])
    parser.add_argument('--queue', type=Path, action='append', default=[])
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.action == 'show':
            result = load(args.state_dir) or dict(volumes={})
            for identity, volume in result['volumes'].items():
                records = [f for a in volume['archives'].values() for f in a['files'].values()]
                recovery = [entry for archive in volume['archives'].values()
                            for entry in archive.get('recovery', {}).values()
                            if entry['status'] == 'retained_unverified']
                print(f'{volume["label"]}: UUID={identity}; files={len(records)}; '
                      f'verified_last_seen={sum(f["status"] == "verified" for f in records)}; '
                      f'corrupted_bodies={len(recovery)}; '
                      f'corrupted_logical_bytes={sum(e["body_bytes"] for e in recovery)}')
        else:
            if not args.uuid or args.mount is None:
                parser.error('snapshot/sync require --uuid and --mount')
            if args.action == 'snapshot':
                result = snapshot(args.mount, args.uuid, args.state_dir,
                                  archives=args.archive, queues=args.queue, label=args.label)
            else:
                result = synchronize(args.mount, args.uuid, args.state_dir, label=args.label)
            print(f'Catalog synchronized: {len(result["volumes"])} volumes, '
                  f'host={args.state_dir / CATALOG}, disk={args.mount / MIRROR_DIR / CATALOG}')
    except (ValueError, OSError, KeyError, TypeError) as error:
        print('ERROR: ' + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
