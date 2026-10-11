"""Background work must not hold up the next course's transfer."""
import hashlib
from collections import namedtuple
import json
from pathlib import Path
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

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
                patch.object(download.subprocess, 'call', side_effect=self.transfer):
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

    def test_range_does_not_block_next_course(self):
        self.items['First']['size'] = 20
        def ranges(root, event, item, *args, **kwargs):
            self.assertTrue(self.second_started.wait(2), 'Range blocked the next transfer')
        with patch.object(recovery, 'retry_ranges', side_effect=ranges):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(self.calls, ['First', 'Second'])
        self.assertEqual(json.loads((self.root / '.download/01-status.json').read_text())['state'], 'finished')

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
                patch.object(download.subprocess, 'call', side_effect=AssertionError('Completed file transferred again')):
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
                patch.object(download.subprocess, 'call', side_effect=transfer), \
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
                patch.object(download.subprocess, 'call', side_effect=transfer), interruption:
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
