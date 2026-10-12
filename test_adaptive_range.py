"""Adaptive salvage must find distant islands without claiming unprobed holes."""
from contextlib import contextmanager
from threading import Event
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

import download
import recovery
from progress import saved_ranges
from test_regressions import Response


class AdaptiveRangeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / '.download').mkdir()
        download.write_json(self.root / '.download/source.json', {'source': 'https://disk.yandex.ru/d/test'})
        self.content = b'A' * (128 * 1024)
        self.item = dict(path='/Course/test.mp4', size=len(self.content),
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/file')
        self.event = dict(self.item, course='Course', corrupted_path='corrupted/test/test.mp4')
        self.body = self.root / self.event['corrupted_path']
        self.tuning = dict(range_block_size_kib=16, range_min_block_size_kib=2,
                           range_linear_block_size_kib=1, range_linear_probes=16,
                           range_failure_limit=0, range_max_passes=2, range_workers=1,
                           range_rps=100000, range_retry_delay=0)

    def run_ranges(self, serve):
        with patch.object(download, 'api', return_value=self.item), \
                patch.object(download.OPENER, 'open', side_effect=serve):
            return recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning)

    def test_shared_budget_stop_retains_other_completed_response(self):
        stopping = Event()
        reading = Event()
        class Budget:
            calls = 0
            @contextmanager
            def request(self, _):
                self.calls += 1
                if self.calls == 2:
                    self_reading = reading.wait(2)
                    stopping.set()
                    if not self_reading:
                        raise AssertionError('First request did not start')
                    raise InterruptedError('Stopped while waiting for budget')
                yield
        class SlowResponse(Response):
            def read1(response, size):
                reading.set()
                if not stopping.wait(2):
                    raise AssertionError('Stop did not arrive')
                return super(SlowResponse, response).read1(size)
        def serve(request, timeout):
            return SlowResponse(0, 64 * 1024, len(self.content), self.content[:64 * 1024])
        tuning = dict(self.tuning, range_workers=2, range_block_size_kib=64, range_max_passes=1)
        with patch.object(download, 'api', return_value=self.item), patch.object(download.OPENER, 'open', side_effect=serve):
            with self.assertRaises(InterruptedError):
                recovery.retry_ranges(self.root, self.event, self.item, 2, tuning, stopping=stopping, request_budget=Budget())
        self.assertEqual(saved_ranges(self.body, len(self.content), self.item['sha256']), [[0, 64 * 1024]])
        self.assertEqual(self.body.read_bytes(), self.content[:64 * 1024])

    def test_distant_island_is_discovered_and_expanded_with_fixed_budget(self):
        calls = []
        def serve(request, timeout):
            start, end = map(int, request.get_header('Range')[6:].split('-'))
            end += 1
            calls.append((start, end))
            if not 100 * 1024 <= start < 111 * 1024:
                raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
            return Response(start, end, len(self.content), self.content[start:min(end, 111 * 1024)])
        ranges = self.run_ranges(serve)
        fine = self.event['range_attempts'][-1]
        self.assertTrue(ranges, 'Linear prefix probes cannot find this distant island')
        self.assertGreater(sum(b - a for a, b in ranges), 1024)
        self.assertLessEqual(len(fine['ranges']), 16)
        self.assertTrue(any(r['end'] - r['start'] > 1024 and r['saved_bytes'] for r in fine['ranges']))
        self.assertEqual(ranges, saved_ranges(self.body, len(self.content), self.item['sha256']))
        self.assertEqual(len(calls), len(set(calls)))
        for start, end in ranges:
            self.assertGreaterEqual(start, 100 * 1024)
            self.assertLessEqual(end, 111 * 1024)
            with self.body.open('rb') as stream:
                stream.seek(start)
                self.assertEqual(stream.read(end - start), self.content[start:end])

    def test_linear_cursor_advances_without_skipping_unsampled_bytes(self):
        def unavailable(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
        self.run_ranges(unavailable)
        journal = Path(str(self.body) + '.ranges.json')
        first = json.loads(journal.read_text())['salvage_cursor']
        self.assertEqual(first, 9 * 1024)
        first_rows = self.event['range_attempts'][-1]['ranges']
        self.assertGreater(max(r['start'] for r in first_rows), 100 * 1024)
        self.run_ranges(unavailable)
        second = json.loads(journal.read_text())['salvage_cursor']
        self.assertGreater(second, first)
        self.assertGreater(self.event['range_attempts'][-1]['unprobed'], 0)

    def test_unavailable_coarse_scan_stops_at_failure_limit_before_fine_search(self):
        self.tuning.update(range_failure_limit=2, range_workers=2)
        def unavailable(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
        self.run_ranges(unavailable)
        first, fine = self.event['range_attempts']
        self.assertLessEqual(len(first['ranges']), 4)
        self.assertGreater(first['unprobed'], 0)
        self.assertTrue(fine['fine'])
        self.assertGreater(max(r['start'] for r in fine['ranges']), 100 * 1024)

    def test_expansion_refines_known_failures_without_repeating_small_probes(self):
        rows = [dict(start=1024, end=2048, saved_bytes=1024),
                dict(start=2048, end=4096, saved_bytes=0),
                dict(start=0, end=1024, saved_bytes=0)]
        planned = recovery.expand_probes([[1024, 2048]], 4096, rows, 2048, 4, 1024)
        self.assertEqual(planned, [(2048, 3072)])

    def test_one_probe_budget_remains_linear_and_resumable(self):
        self.tuning['range_linear_probes'] = 1
        def unavailable(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
        self.run_ranges(unavailable)
        rows = self.event['range_attempts'][-1]['ranges']
        self.assertEqual([(r['start'], r['end']) for r in rows], [(0, 1024)])
        self.assertEqual(json.loads(Path(str(self.body) + '.ranges.json').read_text())['salvage_cursor'], 1024)

    def test_recovery_fetches_only_this_file_once_for_multiple_passes(self):
        def unavailable(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
        with patch.object(download, 'api', return_value=dict(self.item, type='file')) as metadata, \
                patch.object(download, 'files', side_effect=AssertionError('Directory was enumerated')), \
                patch.object(download.OPENER, 'open', side_effect=unavailable):
            recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning)
        metadata.assert_called_once_with('https://disk.yandex.ru/d/test', self.item['path'])

    def test_expired_download_link_refreshes_only_this_file(self):
        fresh = dict(self.item, file='https://example.invalid/fresh')
        urls = []
        def serve(request, timeout):
            urls.append(request.full_url)
            if request.full_url == self.item['file']:
                raise urllib.error.HTTPError(request.full_url, 403, 'Expired', {}, None)
            start, last = map(int, request.get_header('Range')[6:].split('-'))
            return Response(start, last + 1, len(self.content), self.content[start:last + 1])
        with patch.object(download, 'api', side_effect=[self.item, fresh]) as metadata, \
                patch.object(download, 'files', side_effect=AssertionError('Directory was enumerated')), \
                patch.object(download.OPENER, 'open', side_effect=serve):
            ranges = recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning)
        self.assertEqual(metadata.call_count, 2)
        self.assertIn(fresh['file'], urls)
        self.assertTrue(ranges)

    def test_changed_metadata_refuses_range_writes(self):
        fresh = dict(self.item, sha256='0' * 64)
        with patch.object(download, 'api', return_value=fresh):
            with self.assertRaisesRegex(ValueError, 'Source file changed'):
                recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning)
        self.assertFalse(self.body.exists())

    def test_single_public_file_uses_root_metadata_after_path_404(self):
        item = dict(self.item, path='/test.mp4')
        event = dict(self.event, path='/test.mp4', course='.')
        missing = urllib.error.HTTPError('hidden', 404, 'Not found', {}, None)
        def unavailable(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 500, 'Unavailable', {}, None)
        with patch.object(download, 'api', side_effect=[missing, dict(item, type='file', name='test.mp4')]) as metadata, \
                patch.object(download.OPENER, 'open', side_effect=unavailable):
            recovery.retry_ranges(self.root, event, item, 2, self.tuning)
        self.assertEqual(metadata.call_count, 2)
        self.assertEqual(metadata.call_args.args, ('https://disk.yandex.ru/d/test',))
