"""Adaptive salvage must find distant islands without claiming unprobed holes."""
from contextlib import contextmanager, redirect_stdout
from threading import Event
import hashlib
import io
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
        import network
        self.actual_retry = network.RequestRetry.retry
        connectivity = patch('network.RequestRetry.retry', return_value=False)
        connectivity.start()
        self.addCleanup(connectivity.stop)
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

    def test_local_outage_preserves_prefix_and_resumes_missing_bytes(self):
        requests = []
        def serve(request, **kwargs):
            requests.append(request.headers['Range'])
            if len(requests) == 1:
                return Response(0, len(self.content), len(self.content), self.content[:16384], timeout=True)
            return Response(16384, len(self.content), len(self.content), self.content[16384:])
        self.tuning['range_block_size_kib'] = 128
        with patch('network.RequestRetry.retry', return_value=True):
            ranges = self.run_ranges(serve)
        self.assertEqual(requests, ['bytes=0-131071', 'bytes=16384-131071'])
        self.assertEqual(ranges, [[0, len(self.content)]])
        self.assertEqual(self.body.read_bytes(), self.content)

    def test_local_outage_retries_same_range_without_spending_pass(self):
        requests = []
        def serve(request, **kwargs):
            requests.append(request.headers['Range'])
            if len(requests) == 1:
                raise urllib.error.URLError('No route to host')
            return Response(0, len(self.content), len(self.content), self.content)
        self.tuning['range_block_size_kib'] = 128
        with patch('network.RequestRetry.retry', return_value=True):
            ranges = self.run_ranges(serve)
        self.assertEqual(requests, ['bytes=0-131071', 'bytes=0-131071'])
        self.assertEqual(ranges, [[0, len(self.content)]])
        self.assertEqual(len(self.event['range_attempts']), 1)

    def test_route_cause_is_preserved_and_success_is_not_retried(self):
        import errno
        requests = []
        def serve(request, **kwargs):
            requests.append(request.get_header('Range'))
            if len(requests) == 1:
                raise urllib.error.URLError(OSError(errno.EHOSTUNREACH, 'No route to host'))
            return Response(0, len(self.content), len(self.content), self.content)
        self.tuning['range_block_size_kib'] = 128
        self.tuning['range_max_passes'] = 1
        # The suite disables retries for framing tests; exercise the real policy here.
        import network
        with patch.object(network.RequestRetry, 'retry', new=self.actual_retry), \
                patch('network.GUARD', new=network.NetworkGuard()), \
                patch.object(network.socket, 'create_connection', side_effect=AssertionError('No probes')):
            self.assertEqual(self.run_ranges(serve), [[0, len(self.content)]])
        row = self.event['range_attempts'][0]['ranges'][0]
        self.assertEqual(row['request_attempts'], 2)
        self.assertEqual(row['retry_reasons'], {'routing': 1})
        self.assertEqual(row['request_errors']['connect:routing:113:None'], 1)
        self.assertEqual(len(requests), 2)

    def test_stop_before_budget_admission_does_not_duplicate_route_failure(self):
        import errno, network
        stopping = Event()
        rows = []
        class Budget:
            calls = 0
            @contextmanager
            def request(self, stop):
                self.calls += 1
                if self.calls == 2:
                    stop.set()
                    raise InterruptedError('Budget wait stopped')
                yield
        metrics = type('Metrics', (), {'range_request': lambda _, row: rows.append(row)})()
        with patch.object(network.RequestRetry, 'retry', new=self.actual_retry), \
                patch.object(download, 'api', return_value=self.item), \
                patch.object(download.OPENER, 'open', side_effect=urllib.error.URLError(OSError(errno.EHOSTUNREACH, 'Filtered'))):
            with self.assertRaises(InterruptedError):
                recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning,
                                      stopping=stopping, request_budget=Budget(), metrics=metrics)
        self.assertEqual(rows[0]['request_attempts'], 1)
        self.assertEqual(rows[0]['request_errors'], {'connect:routing:113:None': 1})

    def test_request_measurements_distinguish_wait_network_and_save(self):
        def serve(request, timeout):
            start, end = map(int, request.get_header('Range')[6:].split('-'))
            response = Response(start, end + 1, len(self.content), self.content[start:end + 1])
            response.headers['Content-Length'] = f' {end - start + 1} '
            return response
        self.run_ranges(serve)
        attempt = self.event['range_attempts'][0]
        self.assertGreaterEqual(attempt['seconds'], 0)
        for row in attempt['ranges']:
            self.assertGreaterEqual(row['save_seconds'], 0)
            self.assertGreaterEqual(row['seconds'], row['request_seconds'])
            self.assertGreaterEqual(row['request_seconds'], row['first_byte_seconds'])
            self.assertGreaterEqual(row['first_byte_seconds'], 0)

    def test_malformed_content_length_does_not_confirm_range_bytes(self):
        for content_length in ('16385', '16383', 'invalid', '-1'):
            with self.subTest(content_length=content_length):
                def serve(request, timeout):
                    start, last = map(int, request.get_header('Range')[6:].split('-'))
                    response = Response(start, last + 1, len(self.content), self.content[start:last + 1])
                    response.headers['Content-Length'] = content_length
                    return response
                with redirect_stdout(io.StringIO()):
                    ranges = self.run_ranges(serve)
                self.assertEqual(ranges, [])
                self.assertFalse(self.body.exists())

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
        metadata.assert_called_once_with('https://disk.yandex.ru/d/test', self.item['path'], stopping=unittest.mock.ANY)

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
