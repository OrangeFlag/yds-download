"""Background work must not hold up the next course's transfer."""
import hashlib
from collections import namedtuple
import json
import os
import signal
from pathlib import Path
import tempfile
import sys
import subprocess
from threading import Event
import unittest
from unittest.mock import patch, Mock

import download
import recovery
import pipeline


class PipelineChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.content = b'lecture'
        self.data = dict(source='https://disk.yandex.ru/d/test', title='test')
        self.items = {course: dict(path=f'/{course}/video.mp4', size=7,
                                  sha256=hashlib.sha256(self.content).hexdigest(),
                                  file='https://example.invalid/video')
                      for course in ('First', 'Second')}
        self.second_started = Event()
        self.calls = []
        self.addCleanup(self.second_started.set)

    def transfer(self, command, **kwargs):
        text = (self.root / '.download/queue.txt').read_text()
        course = 'Second' if '/Second' in text else 'First'
        self.calls.append(course)
        if course == 'Second':
            self.second_started.set()
        local = self.root / course / 'video.mp4'
        local.parent.mkdir(exist_ok=True)
        local.write_bytes(self.content)
        return 0

    def run_download(self, **kwargs):
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=self.transfer):
            return download.download(self.data, ['First', 'Second'], self.root, retries=0, **kwargs)

    def test_sha_does_not_block_next_course(self):
        original = download.inspect
        def inspect(root, item, course, *args, **kwargs):
            if course == 'First':
                self.assertTrue(self.second_started.wait(2), 'SHA blocked the next transfer')
            return original(root, item, course, *args, **kwargs)
        with patch.object(download, 'inspect', side_effect=inspect):
            self.assertEqual(self.run_download(), 0)
        self.assertTrue(json.loads((self.root / '.download/01-status.json').read_text())['complete'])

    def test_completed_course_is_published_during_next_transfer(self):
        original_popen = subprocess.Popen
        original_inspect = download.inspect
        observed = self.root / 'observed'
        first = self.root / 'First/video.mp4'
        first.parent.mkdir()
        first.write_bytes(self.content)
        def inspect(root, item, course, *args, **kwargs):
            if course == 'First':
                self.assertTrue(self.second_started.wait(2))
            return original_inspect(root, item, course, *args, **kwargs)
        def popen(command, **kwargs):
            self.second_started.set()
            script = ("import pathlib,time; root=pathlib.Path(" + repr(str(self.root)) + "); "
                      "deadline=time.monotonic()+8\n"
                      "while time.monotonic()<deadline:\n"
                      " q=root/'.download/02-status.json'\n"
                      " if q.exists() and '\"state\": \"finished\"' in q.read_text():\n"
                      "  (root/'premature').touch()\n"
                      " p=root/'.download/01-status.json'\n"
                      " if p.exists() and '\"state\": \"finished\"' in p.read_text():\n"
                      "  (root/'observed').touch(); break\n"
                      " time.sleep(.05)\n"
                      "p=root/'Second/video.mp4'; p.parent.mkdir(exist_ok=True); p.write_bytes(b'lecture')\n")
            return original_popen([sys.executable, '-c', script], **kwargs)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch.object(download, 'inspect', side_effect=inspect), \
                patch.object(subprocess, 'Popen', side_effect=popen):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0), 0)
        self.assertTrue(observed.exists(), 'Finished course waited for the entire next transfer')
        self.assertFalse((self.root / 'premature').exists(), 'Active download was finalized early')

    def test_cli_interrupt_is_quiet_and_preserves_exit_code(self):
        with patch('builtins.print') as message:
            self.assertEqual(download.cli(Mock(side_effect=KeyboardInterrupt)), 130)
        message.assert_called_once_with('STOPPED: interrupted; resume state retained', flush=True)

    def test_repeated_sigint_does_not_interrupt_cleanup_or_stop_message(self):
        cleaned = []
        previous = signal.getsignal(signal.SIGINT)
        def entrypoint():
            try:
                os.kill(os.getpid(), signal.SIGINT)
            finally:
                os.kill(os.getpid(), signal.SIGINT)
                cleaned.append(True)
        def stop_message(*args, **kwargs):
            os.kill(os.getpid(), signal.SIGINT)
        with patch('builtins.print', side_effect=stop_message):
            self.assertEqual(download.cli(entrypoint), 130)
        self.assertEqual(cleaned, [True])
        self.assertEqual(signal.getsignal(signal.SIGINT), previous)

    def test_queue_interrupt_stops_and_reaps_download_process(self):
        process = Mock()
        process.wait.side_effect = [KeyboardInterrupt, 130]
        process.poll.return_value = None
        with patch.object(subprocess, 'Popen', return_value=process):
            with self.assertRaises(KeyboardInterrupt):
                pipeline.transfer_process(['download.py'], env={}, on_tick=lambda: None, stop_timeout=80)
        process.send_signal.assert_called_once_with(pipeline.signal.SIGINT)
        self.assertEqual(process.wait.call_args.kwargs, {'timeout': 80})

    def test_callback_failure_stops_and_reaps_transfer(self):
        process = Mock()
        process.wait.side_effect = [subprocess.TimeoutExpired('aria2c', 5), 130]
        process.poll.return_value = None
        with patch.object(subprocess, 'Popen', return_value=process):
            with self.assertRaisesRegex(RuntimeError, 'background failed'):
                pipeline.transfer_process(['aria2c'], env={}, on_tick=Mock(side_effect=RuntimeError('background failed')))
        process.send_signal.assert_called_once_with(pipeline.signal.SIGINT)
        self.assertEqual(process.wait.call_count, 2)

    def test_restart_applies_saved_sha_before_earlier_course_transfer(self):
        state = self.root / '.download'
        state.mkdir()
        second = self.root / 'Second/video.mp4'
        second.parent.mkdir()
        second.write_bytes(self.content)
        background = pipeline.Background(self.root, self.data['source'], 0, pipeline.resolve())
        background.checkpoint(self.items['Second'], 'Second', 1, 'verifying')
        background.submit('sha', self.items['Second'], 'Second', 1)
        next(iter(background.futures.values())).result()
        background.close()
        original = self.transfer
        def transfer(command, **kwargs):
            kwargs['on_tick']()
            proofs = list(download.read_events(state / 'events.jsonl'))
            self.assertTrue(any(row['path'] == self.items['Second']['path'] and row['status'] == 'valid' for row in proofs),
                            'Saved result waited for its course to be reached')
            return original(command, **kwargs)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0), 0)

    def test_range_does_not_block_next_course(self):
        self.items['First']['size'] = 20
        def ranges(root, event, item, *args, **kwargs):
            self.assertTrue(self.second_started.wait(2), 'Range blocked the next transfer')
        with patch.object(recovery, 'retry_ranges', side_effect=ranges):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(self.calls, ['First', 'Second'])
        self.assertEqual(json.loads((self.root / '.download/01-status.json').read_text())['state'], 'finished')

    def test_deferred_range_leaves_durable_work_without_waiting(self):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Range blocked archive handoff')):
            self.assertEqual(self.run_download(defer_range=True), 1)
        state = self.root / '.download'
        jobs = [json.loads(path.read_text()) for path in (state / 'tasks').glob('*.json')]
        self.assertEqual([(job['course'], job['state']) for job in jobs if job['kind'] == 'range'], [('First', 'pending')])
        self.assertEqual(json.loads((state / '01-status.json').read_text())['state'], 'recovering')
        self.assertTrue(json.loads((state / 'summary.json').read_text())['downloads_complete'])
        with patch('pipeline.transfer_process', side_effect=AssertionError('Ordinary download repeated')):
            with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]):
                self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0, defer_range=True), 1)

    def test_sha_mismatch_has_bounded_fresh_downloads(self):
        bad = self.root / 'First/video.mp4'
        bad.parent.mkdir()
        bad.write_bytes(b'garbage')
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(bad.read_bytes(), self.content)
        self.assertTrue(list((self.root / 'corrupted').glob('**/video.mp4')))

    def test_restart_keeps_retry_after_invalid_body_was_archived(self):
        local = self.root / 'First/video.mp4'
        local.parent.mkdir()
        local.write_bytes(b'garbage')
        original = pipeline.Background.retry_ready
        def interrupt(background):
            background.poll()
            if background.retry:
                raise KeyboardInterrupt
            return original(background)
        with patch.object(pipeline.Background, 'retry_ready', side_effect=interrupt, autospec=True):
            with self.assertRaises(KeyboardInterrupt):
                self.run_download()
        self.calls.clear()
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Remaining download retry was lost')):
            self.assertEqual(self.run_download(), 0)
        self.assertIn('First', self.calls)
        self.assertEqual(local.read_bytes(), self.content)

    def test_restart_only_verifies_a_completed_unverified_transfer(self):
        original = download.inspect
        def interrupt(root, item, course, *args, **kwargs):
            if course == 'First':
                raise KeyboardInterrupt
            return original(root, item, course, *args, **kwargs)
        with patch.object(download, 'inspect', side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.run_download()
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=AssertionError('Completed file transferred again')):
            self.assertEqual(download.download(self.data, ['First'], self.root, retries=0), 0)

    def test_external_original_survives_a_newer_missing_root_event(self):
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        external = self.root / 'external.mp4'
        external.write_bytes(self.content)
        event = download.record(state, item, 'First', 0, 'valid', 'SHA verified', local=external)
        download.write_json(state / 'local-copies.json', [dict(event, source=self.data['source'])])
        download.record(state, item, 'First', 0, 'missing', 'Root absent')
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.calls, ['Second'])

    def test_bad_downloads_exhaust_exactly_the_configured_retries(self):
        original = self.transfer
        def transfer(command, **kwargs):
            code = original(command, **kwargs)
            if self.calls[-1] == 'First':
                (self.root / 'First/video.mp4').write_bytes(b'garbage')
            return code
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer), \
                patch.object(recovery, 'retry_ranges', return_value=None), \
                patch.object(download.time, 'sleep'):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=1), 1)
            self.assertEqual(self.calls.count('First'), 2)
            self.assertEqual(self.calls.count('Second'), 1)
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=1), 1)
            self.assertEqual(self.calls.count('First'), 2)

    def interrupted_archive(self, split_move):
        self.items['First']['size'] = 20
        original_transfer = self.transfer
        def transfer(command, **kwargs):
            code = original_transfer(command, **kwargs)
            if self.calls[-1] == 'First':
                local = self.root / 'First/video.mp4'
                local.write_bytes(b'partial-data')
                Path(str(local) + '.aria2').write_bytes(b'control')
                download.write_json(Path(str(local) + '.ranges.json'),
                                    dict(size=20, sha256=self.items['First']['sha256'], ranges=[[0, 12]]))
            return code
        original_manifest = pipeline.failure_manifests
        original_rename = Path.rename
        def manifest(root, event):
            original_manifest(root, event)
            raise KeyboardInterrupt
        def rename(path, destination):
            if str(path).endswith('/First/video.mp4.aria2'):
                raise KeyboardInterrupt
            return original_rename(path, destination)
        interruption = patch.object(Path, 'rename', rename) if split_move else patch.object(pipeline, 'failure_manifests', side_effect=manifest)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer), interruption:
            with self.assertRaises(KeyboardInterrupt):
                download.download(self.data, ['First'], self.root, retries=0)
        def ranges(root, event, item, *args, **kwargs):
            self.assertEqual(recovery.saved_ranges(recovery.body_path(root, event), 20, item['sha256']), [[0, 12]])
        with patch.object(recovery, 'retry_ranges', side_effect=ranges):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual((self.root / 'First/video (partially available).mp4').read_bytes(), b'partial-data')

    def test_restart_repairs_manifest_before_move(self):
        self.interrupted_archive(False)

    def test_restart_repairs_body_before_control_move(self):
        self.interrupted_archive(True)

    def test_sparse_resume_requires_space_for_unreceived_bytes(self):
        item = self.items['First']
        item['size'] = 5 * 1024 ** 3
        local = self.root / 'First/video.mp4'
        local.parent.mkdir()
        with local.open('wb') as stream:
            stream.truncate(item['size'])
        Path(str(local) + '.aria2').write_bytes(b'control')
        state = self.root / '.download'
        state.mkdir()
        download.write_json(state / '01-manifest.json', [item])
        usage = namedtuple('Usage', 'total used free')(10 * 1024 ** 3, 8 * 1024 ** 3, 2 * 1024 ** 3)
        with patch.object(download.shutil, 'disk_usage', return_value=usage):
            with self.assertRaisesRegex(RuntimeError, 'Insufficient free space'):
                self.run_download(tuning=dict(free_space_mib=0))
        self.assertEqual(self.calls, [])

    def test_local_range_failure_remains_pending_for_restart(self):
        self.local_range_failure(RuntimeError('Insufficient disk space'))

    def test_range_enospc_remains_pending_for_restart(self):
        self.local_range_failure(OSError(28, 'Insufficient disk space'))

    def local_range_failure(self, error):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=error):
            with self.assertRaisesRegex(type(error), 'Insufficient disk space'):
                self.run_download()
        state = self.root / '.download'
        ledger = json.loads((state / 'recovery-state.json').read_text())
        self.assertEqual(ledger['/First/video.mp4']['state'], 'running')
        with patch.object(recovery, 'retry_ranges', return_value=None):
            self.assertEqual(self.run_download(), 1)
        ledger = json.loads((state / 'recovery-state.json').read_text())
        self.assertEqual(ledger['/First/video.mp4']['state'], 'finished')


if __name__ == '__main__':
    unittest.main()
