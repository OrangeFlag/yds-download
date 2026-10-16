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


def migrate(old, new, uuid, state):
    old, new, state = Path(old), Path(new), Path(state)
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / (uuid + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = state / (uuid + '.json')
        try:
            saved = json.loads(record.read_text())
        except FileNotFoundError:
            saved = {}
        if saved.get('mount') == str(new) or old == new and not saved:
            sync_directory(state)
            return
        prefixes = set(saved.get('previous', [])) | {str(old)}
        if saved.get('mount'):
            prefixes.add(saved['mount'])
        def scan_error(error):
            raise error
        for directory, _, names in os.walk(new, onerror=scan_error):
            if '.download' not in Path(directory).relative_to(new).parts:
                continue
            metadata = False
            for name in names:
                path = Path(directory) / name
                if path.suffix not in ('.json', '.jsonl') or not stat.S_ISREG(path.lstat().st_mode):
                    continue
                metadata = True
                text = path.read_text(); updated = text
                for prefix in sorted(prefixes, key=len, reverse=True):
                    if prefix != str(new):
                        updated = updated.replace(prefix + '/', str(new) + '/')
                if updated != text:
                    temporary = path.with_name(path.name + '.mount.tmp')
                    with temporary.open('w') as stream:
                        stream.write(updated); stream.flush(); os.fsync(stream.fileno())
                    temporary.chmod(path.stat().st_mode & 0o777)
                    temporary.replace(path)
            if metadata:
                # A prior attempt may have replaced files before its directory sync failed.
                sync_directory(directory)
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
            migrate(root, candidate, uuid, state)
            if not mounted_ok(candidate, device):
                raise StorageUnavailable(candidate)
            current_command = [arg.replace(str(root) + '/', str(candidate) + '/') for arg in command]
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
