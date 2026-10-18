"""Supervise downloads across loss, readonly remounts and reattachment of one disk."""
import argparse
from contextlib import contextmanager, ExitStack
import errno
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
from threading import Event, Thread
import time
import urllib.error
import uuid as uuid_module

TEMPFAIL = 75


class StorageUnavailable(OSError):
    def __init__(self, path=''):
        super().__init__(errno.EIO, 'Download disk unavailable; resume state retained', str(path))


def disk_error(error):
    return (isinstance(error, OSError) and not isinstance(error, urllib.error.URLError)
            and error.errno in (errno.EIO, errno.EROFS, errno.ENODEV, errno.ENXIO, errno.ESTALE, errno.ENOSPC))


def mounted_ok(root, device):
    try:
        return root.stat().st_dev == device and not os.statvfs(root).f_flag & os.ST_RDONLY
    except OSError:
        return False


def check(path):
    root = os.environ.get('YDS_DISK_ROOT')
    device = os.environ.get('YDS_DISK_DEVICE')
    if root and device and (str(path) == root or str(path).startswith(root.rstrip('/') + '/')):
        if not mounted_ok(Path(root), int(device)):
            raise StorageUnavailable(root)


@contextmanager
def lock_archives(roots, filename):
    """Keep archive ownership until exit; admit only roots whose lock is held."""
    with ExitStack() as stack:
        owned=set()
        def claim(selected, strict=False):
            admitted=[]
            for root in sorted({Path(root).resolve() for root in selected}):
                check(root)
                if root not in owned:
                    state=root/'.download'
                    if not state.is_dir():continue
                    lock=(state/filename).open('a')
                    try:
                        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError:
                        lock.close()
                        if strict:raise
                        print('UPLOAD BUSY: archive deferred: '+str(root),flush=True)
                        continue
                    except BaseException:
                        lock.close();raise
                    stack.enter_context(lock)
                    owned.add(root)
                admitted.append(root)
            return admitted
        claim(roots,strict=True)
        yield claim


def saved_local(root, value):
    """Resolve archive-relative paths and accept legacy absolute paths."""
    path = Path(value)
    if not path.is_absolute() and ('..' in path.parts or not path.parts):
        raise ValueError('Unsafe saved local path')
    return path if path.is_absolute() else Path(root) / path


def portable_local(root, value):
    """Store archive-owned files relative to that archive, leave external files absolute."""
    path = Path(value)
    root = Path(root)
    return str(path.relative_to(root)) if path.is_absolute() and path.is_relative_to(root) else value


def queue_path(queue, value):
    """Resolve a queue-relative path, also accepting legacy absolute paths."""
    path = Path(value)
    return path if path.is_absolute() else (Path(queue).parent / path).resolve()


def read_queue(queue, data=None):
    """Return an absolute-path view without changing persisted queue data."""
    data = json.loads(Path(queue).read_text()) if data is None else data
    result = dict(data)
    result['archives'] = [
        {**archive,
         **{key: str(queue_path(queue, archive[key])) for key in ('destination', 'catalog') if key in archive}}
        for archive in data['archives']
    ]
    if data.get('wait_for'):
        result['wait_for'] = {**data['wait_for'],
                              'destination': str(queue_path(queue, data['wait_for']['destination']))}
    return result


def portable_state(path, data):
    """Normalize only local-file fields inside archive metadata and queue references."""
    path = Path(path)
    archive = next((parent.parent for parent in path.parents if parent.name == '.download'), None)
    if archive is not None:
        def convert(value):
            if isinstance(value, dict):
                return {key: portable_local(archive, entry) if key == 'local' and isinstance(entry, str)
                        else convert(entry) for key, entry in value.items()}
            if isinstance(value, list):
                return [convert(entry) for entry in value]
            return value
        data = convert(data)
    if isinstance(data, dict) and isinstance(data.get('archives'), list):
        def ref(value):
            value = Path(value)
            return (os.path.relpath(value, path.parent)
                    if value.is_absolute() and value.is_relative_to(archive or path.parent)
                    else str(value))
        data = {**data, 'archives': [
            {**entry, **{key: ref(entry[key]) for key in ('destination', 'catalog') if key in entry}}
            for entry in data['archives']]}
        if data.get('wait_for') and 'destination' in data['wait_for']:
            data['wait_for'] = {**data['wait_for'], 'destination': ref(data['wait_for']['destination'])}
    return data


def mounts():
    result = subprocess.check_output(['findmnt', '--list', '-J', '-o', 'UUID,TARGET,OPTIONS'], text=True)
    return json.loads(result)['filesystems']


def select_mount(uuid, current):
    candidates = [Path(row['target']) for row in mounts()
                  if row.get('uuid') == uuid and 'rw' in row.get('options', '').split(',')]
    return Path(current) if Path(current) in candidates else next(iter(candidates), None)


def probe(root):
    try:
        with tempfile.NamedTemporaryFile(dir=root, prefix='.yds-storage-') as stream:
            stream.write(b'1'); stream.flush(); os.fsync(stream.fileno())
        return True
    except OSError as error:
        if not disk_error(error) and error.errno != errno.ENOENT:
            raise
        return False


def sync_directory(path):
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def sync_filesystem(path):
    try:
        subprocess.run(['sync', '-f', '--', str(path)], check=True, capture_output=True)
    except subprocess.CalledProcessError as error:
        raise StorageUnavailable(path) from error


def migrate(old, new, uuid, state, previous=(), expected_device=None):
    old, new, state = Path(old), Path(new), Path(state)
    device = new.stat().st_dev if expected_device is None else expected_device
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / (uuid + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not mounted_ok(new, device):
            raise StorageUnavailable(new)
        record = state / (uuid + '.json')
        try:
            saved = json.loads(record.read_text())
        except FileNotFoundError:
            saved = {}
        known = set(saved.get('previous', []))
        if saved.get('mount') == str(new) and set(previous).issubset(known):
            sync_directory(state)
            return
        prefixes = known | {str(old)} | set(previous)
        if saved.get('mount'):
            prefixes.add(saved['mount'])
        replacements = [prefix for prefix in sorted(prefixes,key=len,reverse=True) if prefix != str(new)]
        strings=re.compile(r'"(?:[^"\\]|\\.)*"')
        def rewrite(line):
            # Only replace complete JSON string paths, never URLs or diagnostic text.
            def replace(match):
                value=json.loads(match[0])
                for prefix in replacements:
                    if value==prefix or value.startswith(prefix+'/'):
                        return json.dumps(str(new)+value[len(prefix):],ensure_ascii=False)
                return match[0]
            return strings.sub(replace,line)

        def scan_error(error):
            raise error
        for directory, _, names in (os.walk(new, onerror=scan_error) if replacements else ()):
            if '.download' not in Path(directory).relative_to(new).parts:
                continue
            metadata = False
            for name in names:
                path = Path(directory) / name
                if path.name=='mount-history.json' and path.parent.name=='opencloud-uploads':
                    continue
                if path.suffix not in ('.json', '.jsonl') or not stat.S_ISREG(path.lstat().st_mode):
                    continue
                metadata = True
                # JSONL can be very large; never materialize the full journal in RAM.
                with path.open(encoding='utf-8') as source:
                    changed = any(rewrite(line) != line for line in source)
                if changed:
                    temporary = path.with_name(path.name + '.mount.tmp')
                    with path.open(encoding='utf-8') as source, temporary.open('w', encoding='utf-8') as output:
                        for line in source:
                            output.write(rewrite(line))
                        output.flush(); os.fsync(output.fileno())
                    temporary.chmod(path.stat().st_mode & 0o777)
                    temporary.replace(path)
            if Path(directory).name=='opencloud-uploads' and Path(directory).parent.name=='.download':
                from download import write_json
                history=Path(directory)/'mount-history.json'
                prior=json.loads(history.read_text()) if history.exists() else {}
                write_json(history,dict(mount=str(new),previous=sorted(prefixes | set(prior.get('previous',[])))))
                metadata=True
            if metadata and Path(directory).stat().st_dev != device:
                # Nested filesystems need their own directory durability barrier.
                sync_directory(directory)
        if not mounted_ok(new, device):
            raise StorageUnavailable(new)
        # Flush renamed metadata once, including unchanged files from interrupted attempts.
        sync_filesystem(new)
        if not mounted_ok(new, device):
            raise StorageUnavailable(new)
        temporary = record.with_suffix('.tmp')
        with temporary.open('w') as stream:
            json.dump(dict(mount=str(new), previous=sorted(prefixes)), stream)
            stream.flush(); os.fsync(stream.fileno())
        temporary.replace(record)
        sync_directory(state)


class BoundedLog(RotatingFileHandler):
    def emit(self, record):
        try:
            if shutil.disk_usage(Path(self.baseFilename).parent).free < 512 * 1024 ** 2:
                return
            super().emit(record)
        except OSError:
            pass


def drain(pipe, logger):
    with pipe:
        for line in iter(lambda: pipe.readline(65536), b''):
            logger.info(line.decode('utf-8', errors='replace').rstrip()[:4096])


def terminate(process, timeout):
    deadline = time.monotonic() + timeout
    try:
        os.killpg(process.pid, signal.SIGINT)
    except ProcessLookupError:
        process.wait()
        return
    while True:
        process.poll()
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            break
        time.sleep(min(remaining, 0.1))
    process.wait()


def supervise(uuid, root, command, state, logger, stopping, poll=5, stop_timeout=90):
    root = Path(root)
    while not stopping.is_set():
        candidate = None
        try:
            candidate = select_mount(uuid, root)
            if candidate is None:
                logger.warning('DISK OFFLINE: waiting for writable disk UUID %s', uuid)
                stopping.wait(poll)
                continue
            device = candidate.stat().st_dev
            if select_mount(uuid, candidate) != candidate or not mounted_ok(candidate, device):
                raise StorageUnavailable(candidate)
            if not probe(candidate):
                logger.warning('DISK OFFLINE: waiting for writable disk UUID %s', uuid)
                stopping.wait(poll)
                continue
            if not mounted_ok(candidate, device):
                raise StorageUnavailable(candidate)
            if candidate != root:
                logger.info('DISK MIGRATION: %s -> %s; updating metadata before resume', root, candidate)
            migrate(root, candidate, uuid, state, expected_device=device)
            if not mounted_ok(candidate, device):
                raise StorageUnavailable(candidate)
            old_root = str(root)
            current_command = [
                str(candidate) + arg[len(old_root):]
                if arg == old_root or arg.startswith(old_root + '/') else arg
                for arg in command
            ]
            env = dict(os.environ, YDS_DISK_ROOT=str(candidate), YDS_DISK_DEVICE=str(device))
            if stopping.is_set():
                return 130
            logger.info('DISK ONLINE: %s; starting child with saved progress', candidate)
            process = subprocess.Popen(current_command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       start_new_session=True)
        except OSError as error:
            if not disk_error(error) and not (error.errno == errno.ENOENT and candidate is not None and not candidate.exists()):
                raise
            logger.warning('DISK OFFLINE: startup interrupted; retrying')
            stopping.wait(poll)
            continue
        root = candidate; command = current_command
        reader = Thread(target=drain, args=(process.stdout, logger), daemon=True)
        reader.start()
        try:
            while process.poll() is None:
                if stopping.wait(min(poll, 1)):
                    break
                if not mounted_ok(root, device):
                    logger.warning('DISK OFFLINE: stopping child; file retries retained')
                    break
            retry = not mounted_ok(root, device)
        finally:
            terminate(process, stop_timeout)
            reader.join(timeout=2)
        if stopping.is_set():
            return 130
        if process.returncode == TEMPFAIL or retry:
            stopping.wait(poll)
            continue
        return process.returncode
    return 130


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--uuid', required=True)
    parser.add_argument('--mount-root', required=True, type=Path)
    parser.add_argument('--state-dir', type=Path, default=Path.home() / '.local/state/yds-download')
    parser.add_argument('--log-file', required=True, type=Path)
    parser.add_argument('--poll-interval', type=float, default=5)
    parser.add_argument('--stop-timeout', type=float, default=90)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    uuid_module.UUID(args.uuid)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or args.poll_interval <= 0 or args.stop_timeout <= 0:
        parser.error('Command and positive intervals are required')
    os.umask(0o077)
    args.log_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    logger = logging.getLogger('storage'); logger.setLevel(logging.INFO); logger.propagate = False
    handler = BoundedLog(args.log_file, maxBytes=4 * 1024 ** 2, backupCount=2, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s')); logger.addHandler(handler)
    stopping = Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: stopping.set())
    try:
        return supervise(args.uuid, args.mount_root, command, args.state_dir, logger, stopping,
                         args.poll_interval, args.stop_timeout)
    finally:
        handler.close()


if __name__ == '__main__':
    raise SystemExit(main())
