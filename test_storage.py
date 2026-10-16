import errno
import io
import logging
import json
import os
import signal
import stat
import subprocess
import sys
from itertools import chain, repeat
from pathlib import Path
import tempfile
import time
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

    def test_overlapping_archive_uploaders_share_the_same_nonblocking_lock(self):
        for name in ('opencloud.lock', 'nextcloud.lock'):
            root = self.root / name
            (root / '.download').mkdir(parents=True)
            with storage.lock_archives([root], name):
                with self.assertRaises(BlockingIOError):
                    with storage.lock_archives([root, root.resolve()], name):
                        pass
            with storage.lock_archives([root], name):
                pass

    def test_archive_locks_release_all_roots_on_partial_acquisition_failure(self):
        roots = [self.root / 'a', self.root / 'b']
        for root in roots:
            (root / '.download').mkdir(parents=True)
        with storage.lock_archives([roots[1]], 'opencloud.lock'):
            with self.assertRaises(BlockingIOError):
                with storage.lock_archives(roots, 'opencloud.lock'):
                    pass
        with storage.lock_archives([roots[0]], 'opencloud.lock'):
            pass

    def test_late_archive_is_not_admitted_until_its_lock_is_held(self):
        root=self.root/'future'
        with storage.lock_archives([root],'opencloud.lock') as claim:
            self.assertEqual(claim([root]),[])
            (root/'.download').mkdir(parents=True)
            with storage.lock_archives([root],'opencloud.lock'):
                self.assertEqual(claim([root]),[])
            self.assertEqual(claim([root]),[root])
            with self.assertRaises(BlockingIOError):
                with storage.lock_archives([root],'opencloud.lock'):pass

    def test_admitted_roots_are_a_snapshot_and_locks_survive_between_passes(self):
        first=self.root/'first';late=self.root/'late'
        (first/'.download').mkdir(parents=True)
        with storage.lock_archives([first,late],'nextcloud.lock') as claim:
            admitted=claim([first,late])
            (late/'.download').mkdir(parents=True)
            self.assertEqual(admitted,[first])
            with self.assertRaises(BlockingIOError):
                with storage.lock_archives([first],'nextcloud.lock'):pass
            self.assertEqual(claim([first,late]),[first,late])
        with storage.lock_archives([first,late],'nextcloud.lock'):pass

    def test_both_uploader_loops_claim_late_archives_and_keep_ownership(self):
        import nextcloud
        import opencloud
        from threading import Event
        for name,module in [('nextcloud',nextcloud),('opencloud',opencloud)]:
            with self.subTest(service=name):
                directory=self.root/name;directory.mkdir()
                first=directory/'first';late=directory/'late'
                (first/'.download').mkdir(parents=True)
                queue=directory/'queue.json'
                download.write_json(queue,dict(archives=[dict(destination=str(p)) for p in (first,late)]))
                stop=Event();calls=[]
                def wait_for_pass(_):
                    if len(calls)>=2:stop.set()
                    return stop.is_set()
                def run_once(roots,*args):
                    calls.append(list(roots))
                    for root in roots:
                        with self.assertRaises(BlockingIOError):
                            with storage.lock_archives([root],name+'.lock'):pass
                    if len(calls)==1:(late/'.download').mkdir(parents=True)
                    return 0
                sync=SimpleNamespace(run_once=run_once,index=None,known_folders=set())
                argv=[name+'.py','--queue',str(queue)]
                if name=='nextcloud':
                    argv+=['--nextcloud-upload','https://example.invalid/s/upload']
                    constructor='Sync'
                else:
                    token=directory/'token';token.write_text('test-only token')
                    argv+=['--server','https://example.invalid','--username','test',
                           '--password-file',str(token),'--upload','--poll-interval','1']
                    constructor='Uploader'
                with patch.object(module,'Event',return_value=stop), \
                     patch.object(module,constructor,return_value=sync), \
                     patch.object(stop,'wait',side_effect=wait_for_pass), \
                     patch.object(module.signal,'signal'),patch.object(module.os,'umask'), \
                     patch.object(opencloud.Client,'discover'),patch('sys.argv',argv):
                    module.main()
                self.assertEqual(calls,[[first],[first,late]])

    def test_missing_mount_is_not_a_missing_video(self):
        item = dict(path='/Course/video.mp4', size=7, sha256='1' * 64, file='https://example.invalid/video')
        with patch.dict(os.environ, dict(self.env, YDS_DISK_DEVICE='-1')):
            with self.assertRaises(storage.StorageUnavailable):
                download.inspect(self.root, item, 'Course')

    def test_manual_recovery_refuses_a_missing_mount(self):
        (self.root / '.download').mkdir()
        download.write_json(self.root / '.download/source.json', {'source': 'test'})
        with patch.dict(os.environ, dict(self.env, YDS_DISK_DEVICE='-1')), \
                patch('sys.argv', ['download.py', 'recover', '--destination', str(self.root)]), \
                patch.object(recovery, 'recover_corrupted', side_effect=AssertionError('Recovery ran on an offline disk')):
            with self.assertRaises(SystemExit) as caught:
                download.main()
        self.assertEqual(caught.exception.code, storage.TEMPFAIL)

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

    def test_mount_migration_updates_only_actual_json_path_values(self):
        old=self.root/'old';new=self.root/'new';state=self.root/'state'
        path=new/'YandexDisk/.download/queue.json';path.parent.mkdir(parents=True)
        source=str(old/'YandexDisk/video.mp4')
        content={'local':source,'destination':str(old),
                 'description':'Previously at '+source,
                 'url':'https://example.invalid/'+source,
                 'nested':[{'local':source},source]}
        path.write_text(json.dumps(content))
        storage.migrate(old,new,'uuid',state)
        actual=json.loads(path.read_text())
        self.assertEqual(actual['local'],str(new/'YandexDisk/video.mp4'))
        self.assertEqual(actual['destination'],str(new))
        self.assertEqual(actual['nested'],[{'local':str(new/'YandexDisk/video.mp4')},str(new/'YandexDisk/video.mp4')])
        self.assertEqual(actual['description'],content['description'])
        self.assertEqual(actual['url'],content['url'])

    def test_mount_migration_streams_large_jsonl_instead_of_reading_entire_log(self):
        old=self.root/'old';new=self.root/'new';state=self.root/'state'
        path=new/'YandexDisk/.download/events.jsonl';path.parent.mkdir(parents=True)
        entries=[dict(path='/Course/video.mp4',local=str(old/f'video-{i}.mp4'),
                      details='a'*1024) for i in range(64)]
        path.write_text(''.join(json.dumps(row)+chr(10) for row in entries))
        original=Path.read_text
        def read(p,*args,**kwargs):
            if p==path:raise AssertionError('Materialized entire event journal')
            return original(p,*args,**kwargs)
        with patch.object(Path,'read_text',read):
            storage.migrate(old,new,'uuid',state)
        records=[json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual([row['local'] for row in records],[str(new/f'video-{i}.mp4') for i in range(64)])
        self.assertEqual([row['details'] for row in records],[row['details'] for row in entries])

    def test_mount_migration_preserves_other_paths_and_is_idempotent(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        p = new / 'YandexDisk/Course/.download/tasks/task.json';p.parent.mkdir(parents=True)
        p.write_text(json.dumps({'local': str(old / 'YandexDisk/video.mp4'), 'other': '/home/user/file'}))
        storage.migrate(old, new, 'disk-uuid', state)
        storage.migrate(old, new, 'disk-uuid', state)
        data = json.loads(p.read_text())
        self.assertEqual(data['local'], str(new / 'YandexDisk/video.mp4'))
        self.assertEqual(data['other'], '/home/user/file')

    def test_interrupted_scan_does_not_commit_mount(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        path = new / 'YandexDisk/Course/.download/queue.json'; path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'destination': str(old / 'YandexDisk/Course')}))
        failure = OSError(errno.EIO, 'Disk disconnected during scan')
        with patch('os.scandir', side_effect=failure):
            with self.assertRaises(OSError):
                storage.migrate(old, new, 'uuid', state)
        self.assertFalse((state / 'uuid.json').exists())
        storage.migrate(old, new, 'uuid', state)
        self.assertEqual(json.loads(path.read_text())['destination'], str(new / 'YandexDisk/Course'))

    def test_restart_at_original_mount_rewrites_previous_mount(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        path = new / 'YandexDisk/Course/.download/queue.json'; path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'destination': str(old / 'YandexDisk/Course')}))
        storage.migrate(old, new, 'uuid', state)
        new.rename(old)
        storage.migrate(old, old, 'uuid', state)
        path = old / 'YandexDisk/Course/.download/queue.json'
        self.assertEqual(json.loads(path.read_text())['destination'], str(old / 'YandexDisk/Course'))
        self.assertEqual(json.loads((state / 'uuid.json').read_text())['mount'], str(old))

    def test_migration_syncs_metadata_directory_before_mount_marker(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        path = new / 'YandexDisk/Course/.download/queue.json'; path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'destination': str(old / 'YandexDisk/Course')}))
        synced = []; original = os.fsync
        def sync(fd):
            synced.append((Path(os.readlink(f'/proc/self/fd/{fd}')), stat.S_ISDIR(os.fstat(fd).st_mode)))
            original(fd)
        with patch('storage.os.fsync', side_effect=sync):
            storage.migrate(old, new, 'uuid', state)
        self.assertIn((path.parent, True), synced)
        self.assertIn((state / 'uuid.tmp', False), synced)
        self.assertIn((state, True), synced)
        self.assertLess(synced.index((path.parent, True)), synced.index((state / 'uuid.tmp', False)))

    def test_retry_syncs_directory_replaced_before_previous_interruption(self):
        old = self.root / 'old'; new = self.root / 'new'; state = self.root / 'state'
        path = new / 'YandexDisk/Course/.download/queue.json'; path.parent.mkdir(parents=True)
        path.write_text(json.dumps({'destination': str(old / 'YandexDisk/Course')}))
        with patch('storage.sync_directory', side_effect=OSError(errno.EIO, 'Directory sync interrupted')):
            with self.assertRaises(OSError):
                storage.migrate(old, new, 'uuid', state)
        self.assertFalse((state / 'uuid.json').exists())
        self.assertEqual(json.loads(path.read_text())['destination'], str(new / 'YandexDisk/Course'))
        original = storage.sync_directory
        with patch('storage.sync_directory', side_effect=original) as synced:
            storage.migrate(old, new, 'uuid', state)
        self.assertTrue(any(Path(call.args[0]) == path.parent for call in synced.call_args_list))

    def test_cli_disk_error_has_retryable_exit_code(self):
        with patch('sys.argv', ['download.py', 'download', '--catalog', '/missing', '--destination', '/missing']), \
                patch.object(Path, 'read_text', side_effect=OSError(errno.EIO, 'Disk disconnected')):
            with self.assertRaises(SystemExit) as error:
                download.main()
        self.assertEqual(error.exception.code, 75)

    def test_ordinary_missing_file_is_not_disk_failure(self):
        self.assertFalse(storage.disk_error(FileNotFoundError(errno.ENOENT, 'Missing file')))


class SupervisorChecks(unittest.TestCase):
    def test_mount_fallback_after_probe_does_not_launch_on_system_disk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            device = [123]
            probes = [0]
            def probe(path):
                probes[0] += 1
                device[0] = 456 if probes[0] == 1 else 123
                return True
            process = Mock(returncode=0); process.poll.return_value = 0; process.stdout = io.BytesIO()
            stop = Mock(); stop.is_set.return_value = False
            def retry(delay):
                device[0] = 123
                return False
            stop.wait.side_effect = retry
            with patch('storage.select_mount', return_value=root), \
                    patch.object(Path, 'stat', side_effect=lambda: SimpleNamespace(st_dev=device[0])), \
                    patch('storage.mounted_ok', side_effect=lambda path, expected: device[0] == expected), \
                    patch('storage.probe', side_effect=probe), patch('storage.migrate') as migrate, \
                    patch('storage.subprocess.Popen', return_value=process) as launch, patch('storage.terminate'):
                self.assertEqual(storage.supervise('uuid', root, ['python3'], root / 'state', Mock(), stop), 0)
            self.assertEqual(probes[0], 2)
            self.assertEqual(migrate.call_count, 1)
            self.assertEqual(launch.call_count, 1)
            self.assertEqual(launch.call_args.kwargs['env']['YDS_DISK_DEVICE'], '123')

    def test_startup_storage_errors_retry_and_command_errors_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            process = Mock(returncode=0); process.poll.return_value = 0; process.stdout = io.BytesIO()
            stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
            for stage in ('select_mount', 'migrate', 'stat', 'Popen'):
                process.stdout = io.BytesIO()
                with self.subTest(stage=stage), \
                        patch('storage.select_mount', return_value=root) as discover, \
                        patch('storage.probe', return_value=True), patch('storage.migrate') as migrate, \
                        patch.object(Path, 'stat', return_value=SimpleNamespace(st_dev=123)) as stat_root, \
                        patch('storage.mounted_ok', return_value=True), \
                        patch('storage.subprocess.Popen', return_value=process) as launch, patch('storage.terminate'):
                    operation = dict(select_mount=discover, migrate=migrate, stat=stat_root, Popen=launch)[stage]
                    operation.side_effect = chain([OSError(errno.EIO, 'Disk disconnected')], repeat(operation.return_value))
                    self.assertEqual(storage.supervise('uuid', root, ['python3'], root / 'state', Mock(), stop), 0)
                    self.assertEqual(operation.call_count, 3 if stage == 'select_mount' else 2)
            with patch('storage.select_mount', return_value=root), patch('storage.probe', return_value=True), \
                    patch('storage.migrate'), patch('storage.subprocess.Popen', side_effect=FileNotFoundError(errno.ENOENT, 'Missing executable')):
                with self.assertRaises(FileNotFoundError):
                    storage.supervise('uuid', root, ['missing-program'], root / 'state', Mock(), stop)

    def test_missing_mount_after_probe_is_retryable(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            process = Mock(returncode=0); process.poll.return_value = 0; process.stdout = io.BytesIO()
            stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
            with patch('storage.select_mount', side_effect=[root / 'removed', root, root]), \
                    patch('storage.probe', return_value=True), patch('storage.migrate'), \
                    patch('storage.subprocess.Popen', return_value=process), patch('storage.terminate'):
                self.assertEqual(storage.supervise('uuid', root, ['python3'], root / 'state', Mock(), stop), 0)

    def test_stop_cleans_descendants_even_if_leader_exits_first(self):
        child = 'import os,signal,time; signal.signal(signal.SIGINT,signal.SIG_IGN); print(os.getpid(),flush=True); time.sleep(30)'
        leader = 'import signal,subprocess,sys,time; signal.signal(signal.SIGINT,lambda *_:sys.exit(130)); subprocess.Popen([sys.executable,"-c",sys.argv[1]]); time.sleep(30)'
        for exited in (False, True):
            with self.subTest(leader_already_exited=exited):
                process = subprocess.Popen([sys.executable, '-c', leader, child], start_new_session=True,
                                           stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
                try:
                    pid = int(process.stdout.readline())
                    if exited:
                        process.send_signal(signal.SIGINT); process.wait(timeout=2)
                    storage.terminate(process, 0.2)
                    status = Path(f'/proc/{pid}/stat')
                    deadline = time.monotonic() + 1
                    while status.exists() and status.read_text().split(') ', 1)[1].split()[0] != 'Z' and time.monotonic() < deadline:
                        time.sleep(0.01)
                    if status.exists():
                        self.assertEqual(status.read_text().split(') ', 1)[1].split()[0], 'Z')
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=2)
                    process.stdout.close()

    def test_retryable_disk_exit_restarts_same_transfer(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = Mock(returncode=75); first.poll.return_value = 75; first.stdout = io.BytesIO()
            second = Mock(returncode=0); second.poll.return_value = 0; second.stdout = io.BytesIO()
            stop = Mock(); stop.is_set.return_value = False; stop.wait.return_value = False
            with patch('storage.select_mount', return_value=root), \
                    patch('storage.subprocess.Popen', side_effect=[first, second]) as launch, \
                    patch('storage.terminate'):
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
            def stop_child(process, timeout):
                if process is first:
                    first.returncode = 130; offline[0] = False
            second = Mock(returncode=0);second.poll.return_value = 0;second.stdout = io.BytesIO()
            stop = Mock();stop.is_set.return_value = False;stop.wait.return_value = False
            with patch('storage.select_mount', side_effect=[old, old, new, new]), \
                    patch('storage.mounted_ok', side_effect=lambda *args: launch.call_count == 0 or not offline[0]), \
                    patch('storage.subprocess.Popen', side_effect=[first, second]) as launch, \
                    patch('storage.terminate', side_effect=stop_child) as terminate:
                url='https://example.invalid'+str(old)+'/image'
                self.assertEqual(storage.supervise('uuid', old,
                                                   ['python3', str(old), str(old / 'YandexDisk/queue.json'), url],
                                                   root / 'state', Mock(), stop), 0)
            terminate.assert_any_call(first, 90)
            self.assertEqual(launch.call_args.args[0][1:], [
                str(new), str(new / 'YandexDisk/queue.json'), url])

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
