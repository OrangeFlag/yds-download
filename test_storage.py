import errno
import io
import logging
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, Mock
import storage
import download
import recovery


class StorageChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = dict(YDS_DISK_ROOT=str(self.root), YDS_DISK_DEVICE=str(self.root.stat().st_dev))

    def test_missing_mount_is_not_a_missing_video(self):
        item = dict(path='/Course/video.mp4', size=7, sha256='1' * 64, file='https://example.invalid/video')
        with patch.dict(os.environ, dict(self.env, YDS_DISK_DEVICE='-1')):
            with self.assertRaises(storage.StorageUnavailable):
                download.inspect(self.root, item, 'Course')

    def test_readonly_mount_blocks_state_writes(self):
        with patch.dict(os.environ, self.env), \
                patch('storage.os.statvfs', return_value=SimpleNamespace(f_flag=os.ST_RDONLY)):
            with self.assertRaises(storage.StorageUnavailable):
                download.write_json(self.root / 'state.json', {'state': 'finished'})
        self.assertFalse((self.root / 'state.json').exists())

    def test_disk_error_is_not_a_completed_recovery(self):
        event = dict(path='/Course/video.mp4', size=7, sha256='1' * 64,
                     course='Course', corrupted_path='corrupted/old/Course/video.mp4')
        with patch.object(recovery, 'retry_ranges', side_effect=OSError(errno.EIO, 'Disk disconnected')):
            with self.assertRaises(OSError):
                recovery._finish_recovery(self.root, event, dict(event, file='https://example.invalid/video'))
        self.assertNotIn('recovery_error', event)

    def test_selects_same_uuid_on_new_writable_mount(self):
        mounts = [dict(uuid='disk-uuid', target='/old', options='ro'),
                  dict(uuid='different-uuid', target='/other', options='rw'),
                  dict(uuid='disk-uuid', target='/new', options='rw')]
        with patch('storage.mounts', return_value=mounts):
            self.assertEqual(storage.select_mount('disk-uuid', '/old'), Path('/new'))

    def test_mount_migration_preserves_other_paths_and_is_idempotent(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        p = new / 'YandexDisk/Course/.download/tasks/task.json';p.parent.mkdir(parents=True)
        p.write_text(json.dumps({'local': str(old / 'YandexDisk/video.mp4'), 'other': '/home/user/file'}))
        storage.migrate(old, new, 'disk-uuid', state)
        storage.migrate(old, new, 'disk-uuid', state)
        data = json.loads(p.read_text())
        self.assertEqual(data['local'], str(new / 'YandexDisk/video.mp4'))
        self.assertEqual(data['other'], '/home/user/file')

    def test_cli_disk_error_has_retryable_exit_code(self):
        with patch('sys.argv', ['download.py', 'download', '--catalog', '/missing', '--destination', '/missing']), \
                patch.object(Path, 'read_text', side_effect=OSError(errno.EIO, 'Disk disconnected')):
            with self.assertRaises(SystemExit) as error:
                download.main()
        self.assertEqual(error.exception.code, 75)

    def test_ordinary_missing_file_is_not_disk_failure(self):
        self.assertFalse(storage.disk_error(FileNotFoundError(errno.ENOENT, 'Missing file')))


class SupervisorChecks(unittest.TestCase):
    def test_retryable_disk_exit_restarts_same_transfer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = Mock(returncode=75); first.poll.return_value = 75; first.stdout = io.BytesIO()
            second = Mock(returncode=0); second.poll.return_value = 0; second.stdout = io.BytesIO()
            stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
            with patch('storage.select_mount', return_value=root), \
                    patch('storage.subprocess.Popen', side_effect=[first, second]) as launch:
                code = storage.supervise('uuid', root, ['python3', str(root / 'queue.json')],
                                         root / 'state', Mock(), stop)
            self.assertEqual(code, 0)
            self.assertEqual(launch.call_count, 2)
            self.assertEqual(launch.call_args.kwargs['env']['YDS_DISK_ROOT'], str(root))

    def test_disk_loss_stops_child_and_resumes_at_new_mount(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); old = root / 'old'; new = root / 'new';old.mkdir();new.mkdir()
            offline = [True]
            first = Mock(returncode=None); first.stdout = io.BytesIO()
            first.poll.side_effect = lambda: first.returncode
            def stop_child(*args):
                first.returncode = 130; offline[0] = False
            first.send_signal.side_effect = stop_child
            second = Mock(returncode=0);second.poll.return_value = 0;second.stdout = io.BytesIO()
            stop = Mock();stop.is_set.return_value = False;stop.wait.return_value = False
            with patch('storage.select_mount', side_effect=[old, new]), \
                    patch('storage.mounted_ok', side_effect=lambda *args: not offline[0]), \
                    patch('storage.subprocess.Popen', side_effect=[first, second]) as launch:
                self.assertEqual(storage.supervise('uuid', old, ['python3', str(old / 'YandexDisk/queue.json')],
                                                    root / 'state', Mock(), stop), 0)
            first.send_signal.assert_called_once()
            self.assertEqual(launch.call_args.args[0][1], str(new / 'YandexDisk/queue.json'))

    def test_rotating_logs_are_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'service.log'
            handler = storage.BoundedLog(path, maxBytes=128, backupCount=2)
            try:
                for _ in range(30):
                    handler.handle(logging.LogRecord('test', logging.INFO, '', 0, 'x' * 32, (), None))
            finally:
                handler.close()
            files = list(Path(tmp).glob('service.log*'))
            self.assertLessEqual(len(files), 3)
            self.assertLessEqual(sum(p.stat().st_size for p in files), 3 * 128)

    def test_logs_stop_before_system_disk_is_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'service.log';handler = storage.BoundedLog(path, maxBytes=128, backupCount=2)
            try:
                with patch('storage.shutil.disk_usage', return_value=SimpleNamespace(free=1024)):
                    handler.handle(logging.LogRecord('test', logging.INFO, '', 0, 'message', (), None))
            finally:
                handler.close()
            self.assertEqual(path.read_text(), '')

    def test_failed_fsync_does_not_replace_previous_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'state.json';path.write_text('{"state": "running"}')
            with patch('download.os.fsync', side_effect=OSError(errno.EIO, 'Disk disconnected')):
                with self.assertRaises(OSError):
                    download.write_json(path, {'state': 'finished'})
            self.assertEqual(json.loads(path.read_text())['state'], 'running')
