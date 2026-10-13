"""Supervise downloads across loss, readonly remounts and reattachment of one disk."""
import argparse
import errno
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
from threading import Event, Thread
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
    except OSError:
        return False


def migrate(old, new, uuid, state):
    old, new, state = Path(old), Path(new), Path(state)
    if old == new:
        return
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / (uuid + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = state / (uuid + '.json')
        saved = json.loads(record.read_text()) if record.exists() else {}
        if saved.get('mount') == str(new):
            return
        prefixes = set(saved.get('previous', [])) | {str(old)}
        for directory in (new / 'YandexDisk').rglob('.download'):
            for path in directory.rglob('*'):
                if path.is_symlink() or not path.is_file() or path.suffix not in ('.json', '.jsonl'):
                    continue
                text = path.read_text(); updated = text
                for prefix in prefixes:
                    if prefix != str(new):
                        updated = updated.replace(prefix + '/', str(new) + '/')
                if updated != text:
                    temporary = path.with_name(path.name + '.mount.tmp')
                    with temporary.open('w') as stream:
                        stream.write(updated); stream.flush(); os.fsync(stream.fileno())
                    temporary.chmod(path.stat().st_mode & 0o777)
                    temporary.replace(path)
        temporary = record.with_suffix('.tmp')
        temporary.write_text(json.dumps(dict(mount=str(new), previous=sorted(prefixes))))
        temporary.replace(record)


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
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()


def supervise(uuid, root, command, state, logger, stopping, poll=5, stop_timeout=90):
    root = Path(root)
    while not stopping.is_set():
        candidate = select_mount(uuid, root)
        if candidate is None or not probe(candidate):
            logger.warning('DISK OFFLINE: waiting for writable disk UUID %s', uuid)
            stopping.wait(poll)
            continue
        try:
            migrate(root, candidate, uuid, state)
        except OSError:
            logger.warning('DISK OFFLINE: metadata migration interrupted; retrying')
            stopping.wait(poll)
            continue
        command = [arg.replace(str(root) + '/', str(candidate) + '/') for arg in command]
        root = candidate; device = root.stat().st_dev
        env = dict(os.environ, YDS_DISK_ROOT=str(root), YDS_DISK_DEVICE=str(device))
        logger.info('DISK ONLINE: %s; starting child with saved progress', root)
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   start_new_session=True)
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
