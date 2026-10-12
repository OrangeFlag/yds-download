"""Resume interrupted archives and bound listing retries without live requests."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import download
import recovery
import semester_queue
import settings
from test_regressions import Response


class ResumeChecks(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / '.download'
        self.state.mkdir()
        self.source = 'https://disk.yandex.ru/d/test'
        self.content = b'A' * 128
        self.item = dict(path='/Course/video.mp4', size=128,
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/video')
        self.event = dict(path=self.item['path'], size=128, sha256=self.item['sha256'],
                          course='Course', corrupted_path='corrupted/old/Course/video.mp4',
                          time='2026-01-01', status='partial', attempt=3, detail='interrupted')
        self.body = self.root / self.event['corrupted_path']
        self.body.parent.mkdir(parents=True)
        self.body.write_bytes(self.content[:64])
        download.write_json(Path(str(self.body) + '.ranges.json'),
                            dict(size=128, sha256=self.item['sha256'], ranges=[[0, 64]]))
        download.write_json(self.body.parents[1] / 'failed.json', [self.event])
        download.write_json(self.state / 'source.json', dict(source=self.source, title='test'))
        self.status('running')
        self.requests = []
        self.real_api = download.api
        for target, kwargs in (
                ('download.api', dict(return_value=self.item)),
                ('download.files', dict(return_value=[self.item])),
                ('download.time.sleep', {}),
                ('pipeline.transfer_process', dict(side_effect=AssertionError('aria2 restarted an archived file')))):
            context = patch(target, **kwargs)
            context.start()
            self.addCleanup(context.stop)

    def status(self, state):
        download.write_json(self.state / 'course-history.json', {'Course': dict(
            course='Course', source=self.source, state=state, total=1, verified=0,
            failed=1, complete=False, time='2026-01-02')})

    def run_download(self, **kwargs):
        return download.download(dict(source=self.source, title='test'), ['Course'], self.root, retries=0, **kwargs)

    def serve(self, request, timeout):
        start, last = map(int, request.get_header('Range')[6:].split('-'))
        self.requests.append((start, last + 1))
        return Response(start, last + 1, 128, self.content[start:last + 1])

    def fail_range(self, request, timeout):
        self.requests.append(request.get_header('Range'))
        raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)

    def test_running_archive_resumes_saved_ranges_before_aria2(self):
        with patch.object(download.OPENER, 'open', side_effect=self.serve):
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests, [(64, 128)])
        self.assertEqual((self.root / 'Course/video.mp4').read_bytes(), self.content)
        self.assertEqual(self.body.read_bytes(), self.content)
        self.assertEqual(json.loads((self.state / 'summary.json').read_text())['verified'], 1)

    def test_incomplete_running_archive_is_not_retried_twice(self):
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery, 'promote_partial', return_value=False):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.body.read_bytes(), self.content[:64])
        self.assertEqual(json.loads((self.state / '01-status.json').read_text())['failed'], 1)

    def test_explicit_retry_does_not_repeat_running_range_recovery(self):
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery, 'promote_partial', return_value=False):
            self.assertEqual(self.run_download(retry_check=True), 1)
        self.assertEqual(len(self.requests), 2)

    def test_finished_archive_needs_explicit_retry(self):
        self.status('finished')
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Finished archive retried')):
            self.assertEqual(self.run_download(), 1)

    def test_completed_attempt_is_skipped_after_running_restart(self):
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery, 'promote_partial', return_value=False):
            self.assertEqual(self.run_download(), 1)
            first = len(self.requests)
            self.status('running')
            self.assertEqual(self.run_download(), 1)
            self.assertEqual(len(self.requests), first)
            self.assertEqual(self.run_download(retry_check=True), 1)
            self.assertGreater(len(self.requests), first)

    def test_interrupted_recovery_remains_resumable(self):
        with patch.object(recovery, 'retry_ranges', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                recovery.finish_recovery(self.root, self.event, self.item, 0)
        ledger = json.loads((self.state / 'recovery-state.json').read_text())
        self.assertEqual(ledger[self.item['path']]['state'], 'running')
        with patch.object(download.OPENER, 'open', side_effect=self.serve):
            self.assertEqual(self.run_download(), 0)

    def test_legacy_completed_ranges_not_replayed(self):
        self.event['range_attempts'] = [dict(status='failed')]
        download.write_json(self.body.parents[1] / 'failed.json', [self.event])
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Legacy replay')):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(json.loads((self.state / 'recovery-state.json').read_text())[self.item['path']]['state'], 'finished')

    def test_deep_only_checks_local_missing_without_ranges(self):
        self.status('finished')
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Deep fetched content')):
            self.assertEqual(self.run_download(deep=True), 1)
        self.assertFalse((self.state / 'recovery-state.json').exists())

    def test_deep_external_copy_checks_sha_and_updates_registry(self):
        external = self.root / 'external.mp4'
        external.write_bytes(b'B' * 128)
        copy = dict(self.event, source=self.source, local=str(external), status='valid',
                    mtime_ns=external.stat().st_mtime_ns)
        download.write_json(self.state / 'local-copies.json', [copy])
        self.status('finished')
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Deep fetched content')):
            self.assertEqual(self.run_download(deep=True), 1)
        self.assertEqual(json.loads((self.state / 'local-copies.json').read_text())[0]['status'], 'invalid')
        self.assertEqual(external.read_bytes(), b'B' * 128)
        external.write_bytes(self.content)
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Deep fetched content')):
            self.assertEqual(self.run_download(deep=True), 0)
        repaired = json.loads((self.state / 'local-copies.json').read_text())[0]
        self.assertEqual(repaired['status'], 'valid')
        self.assertEqual(repaired['local'], str(external))

    def test_deep_missing_unstarted_course_stays_pending(self):
        (self.state / 'course-history.json').unlink()
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Deep fetched content')):
            self.assertEqual(self.run_download(deep=True), 1)
        history = json.loads((self.state / 'course-history.json').read_text())
        self.assertEqual(history['Course']['state'], 'pending')

    def test_retry_check_does_not_rehash_ready_original(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(self.content)
        download.record(self.state, self.item, 'Course', 0, 'valid', 'verified')
        self.status('finished')
        with patch.object(download.hashlib, 'file_digest', side_effect=AssertionError('Unexpected SHA read')), \
                patch.object(download.OPENER, 'open', side_effect=AssertionError('Ready file downloaded')):
            self.assertEqual(self.run_download(retry_check=True), 0)

    def test_queue_lock_is_closed_after_success(self):
        queue = self.root / 'queue.json'
        download.write_json(queue, dict(archives=[dict(
            title='test', catalog='catalog.json', destination=str(self.root), state='pending')]))
        opened = []
        original_open = Path.open

        def track(path, *args, **kwargs):
            stream = original_open(path, *args, **kwargs)
            if path.name == 'queue.lock':
                opened.append(stream)
            return stream

        with patch.object(Path, 'open', track), \
                patch('sys.argv', ['semester_queue.py', str(queue)]), \
                patch.object(semester_queue.subprocess, 'call', return_value=0):
            semester_queue.main()
        self.assertTrue(opened)
        self.assertTrue(all(stream.closed for stream in opened))

    def test_deep_download_lock_is_closed_after_success(self):
        opened = []
        original_open = Path.open

        def track(path, *args, **kwargs):
            stream = original_open(path, *args, **kwargs)
            if path.name == 'download.lock':
                opened.append(stream)
            return stream

        with patch.object(Path, 'open', track):
            self.assertEqual(self.run_download(deep=True), 1)
        self.assertTrue(opened)
        self.assertTrue(all(stream.closed for stream in opened))

    def test_queue_flags_are_independent_and_deep_preserves_state(self):
        queue = self.root / 'queue.json'
        for deep, retry in ((True, False), (False, True), (True, True)):
            job = dict(title='test', catalog='catalog.json', destination=str(self.root), state='pending')
            download.write_json(queue, dict(archives=[job], deep=deep, retry_check=retry))
            calls = []
            with patch('sys.argv', ['semester_queue.py', str(queue)]), \
                    patch.object(semester_queue.subprocess, 'call', side_effect=lambda cmd: calls.append(cmd) or 1):
                semester_queue.main()
            self.assertEqual('--deep' in calls[0], deep)
            self.assertEqual('--retry-check' in calls[0], retry)
            self.assertEqual(json.loads(queue.read_text())['archives'][0]['state'], 'pending' if deep and not retry else 'finished')

    def test_changed_identity_does_not_use_completed_recovery(self):
        old = dict(self.event, sha256='0' * 64)
        download.write_json(self.state / 'recovery-state.json', {
            self.item['path']: dict(state='finished', event=old)})
        with patch.object(download.OPENER, 'open', side_effect=self.serve):
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests, [(64, 128)])

    def test_tuning_reaches_aria2_and_saved_state(self):
        self.status('pending')
        calls = []
        def transfer(command, **kwargs):
            calls.append(command)
            local = self.root / 'Course/video.mp4'
            local.parent.mkdir(exist_ok=True)
            local.write_bytes(self.content)
            return 0
        tuning = dict(download_concurrency=1, connections=2, split=3, disk_cache_mib=128,
                      bandwidth_limit=1048576, auto_save_interval=15, timeout=45,
                      auto_concurrency=True, auto_concurrency_a=1.0, auto_concurrency_b=2.0)
        with patch('pipeline.transfer_process', side_effect=transfer):
            self.assertEqual(self.run_download(tuning=tuning), 0)
        for flag in ('--max-concurrent-downloads=1', '--max-connection-per-server=2',
                     '--split=3', '--disk-cache=128M', '--max-overall-download-limit=1048576',
                     '--auto-save-interval=15', '--timeout=45', '--optimize-concurrent-downloads=1:2'):
            self.assertIn(flag, calls[0])
        saved = json.loads((self.state / 'settings.json').read_text())['tuning']
        for key, value in tuning.items():
            self.assertEqual(saved[key], value)

    def test_range_tuning_limits_probes_and_blocks(self):
        self.content = b'A' * 8192
        self.item.update(size=8192, sha256=hashlib.sha256(self.content).hexdigest())
        self.event.update(size=8192, sha256=self.item['sha256'])
        download.write_json(Path(str(self.body) + '.ranges.json'),
                            dict(size=8192, sha256=self.item['sha256'], ranges=[[0, 64]]))
        tuning = dict(range_block_size_kib=2, range_min_block_size_kib=1,
                      range_linear_block_size_kib=1, range_linear_probes=3,
                      range_failure_limit=0, range_max_passes=2, range_workers=1,
                      range_timeout=7, range_retry_delay=0)
        timeouts = []
        def fail(request, timeout):
            timeouts.append(timeout)
            return self.fail_range(request, timeout)
        with patch.object(download.OPENER, 'open', side_effect=fail):
            recovery.retry_ranges(self.root, self.event, self.item, 0, tuning)
        attempts = self.event['range_attempts']
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(attempts[1]['ranges']), 3)
        self.assertTrue(attempts[1]['fine'])
        self.assertTrue(all(row['end'] - row['start'] <= 1024 for row in attempts[1]['ranges']))
        self.assertEqual(set(timeouts), {7})

    def test_invalid_tuning_is_rejected(self):
        for values in (dict(range_workers=0), dict(connections=17), dict(timeout=-1),
                       dict(range_rps=float('nan')), dict(range_rps=float('inf')),
                       dict(download_concurrency=True), dict(split=1.5), dict(auto_concurrency=1),
                       dict(auto_concurrency_a=0), dict(auto_concurrency_b=-1),
                       dict(range_linear_block_size_kib=2048), dict(unknown=3)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                settings.resolve(values)

    def test_queue_tuning_overrides_global_values(self):
        queue = self.root / 'queue.json'
        job = dict(title='test', catalog='catalog.json', destination=str(self.root),
                   tuning=dict(range_workers=1), state='pending')
        download.write_json(queue, dict(archives=[job], tuning=dict(range_workers=2, disk_cache_mib=128, auto_concurrency=True)))
        calls = []
        with patch('sys.argv', ['semester_queue.py', str(queue)]), \
                patch.object(semester_queue.subprocess, 'call', side_effect=lambda cmd: calls.append(cmd) or 0):
            semester_queue.main()
        self.assertIn('--range-workers=1', calls[0])
        self.assertIn('--disk-cache-mib=128', calls[0])
        self.assertIn('--auto-concurrency', calls[0])
        self.assertNotIn('--no-auto-concurrency', calls[0])
        self.assertNotIn('--range-workers=2', calls[0])

    def test_no_transfer_does_not_require_reserved_disk_space(self):
        original = self.root / 'Course/video.mp4'
        original.parent.mkdir()
        original.write_bytes(self.content)
        download.record(self.state, self.item, 'Course', 0, 'valid', 'verified')
        with patch.object(download.shutil, 'disk_usage', return_value=type('Space', (), {'free': 100})()), \
                patch.object(download.OPENER, 'open', side_effect=AssertionError('No transfer needed')):
            self.assertEqual(self.run_download(deep=True), 0)

    def test_low_rps_rate_wait_is_interruptible_in_short_sleeps(self):
        sleeps = []
        with patch.object(download.OPENER, 'open', side_effect=self.fail_range), \
                patch.object(recovery.time, 'sleep', side_effect=sleeps.append):
            recovery.retry_ranges(self.root, self.event, self.item, 0,
                                  dict(range_rps=0.01, range_retry_delay=0))
        self.assertGreater(len(sleeps), 10)
        self.assertTrue(all(0 <= duration <= 0.25 for duration in sleeps))

    def test_local_only_recover_preserves_unfinished_network_attempt(self):
        download.write_json(self.state / 'recovery-state.json', {
            self.item['path']: dict(state='running', event=self.event)})
        with patch.object(download.OPENER, 'open', side_effect=AssertionError('Offline made request')):
            recovery.recover_corrupted(self.root, self.source)
        self.assertEqual(json.loads((self.state / 'recovery-state.json').read_text())[self.item['path']]['state'], 'running')
        with patch.object(download.OPENER, 'open', side_effect=self.serve):
            self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.requests, [(64, 128)])

    def test_api_does_not_retry_permanent_404(self):
        error = urllib.error.HTTPError('https://example.invalid', 404, 'Gone', {}, None)
        attempts = []
        def fail(*args, **kwargs):
            attempts.append(1)
            raise error
        with patch.object(download.OPENER, 'open', side_effect=fail):
            with self.assertRaises(urllib.error.HTTPError):
                self.real_api(self.source, '/Course')
        self.assertEqual(len(attempts), 1)

    def test_listing_does_not_retry_permanent_404(self):
        error = urllib.error.HTTPError('https://example.invalid', 404, 'Gone', {}, None)
        with patch.object(download, 'files', side_effect=[error, AssertionError('Permanent error retried')]):
            with self.assertRaises(urllib.error.HTTPError):
                download.list_course(self.source, 'Course')

    def test_transient_listing_failure_is_bounded(self):
        error = urllib.error.HTTPError('https://example.invalid', 503, 'Unavailable', {}, None)
        with patch.object(download, 'files', side_effect=[error, error, error, AssertionError('Unbounded retries')]):
            with self.assertRaises(urllib.error.HTTPError):
                download.list_course(self.source, 'Course')


if __name__ == '__main__':
    unittest.main()
