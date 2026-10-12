"""Adaptive salvage must find distant islands without claiming unprobed holes."""
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
        with patch.object(download, 'files', return_value=[self.item]), \
                patch.object(download.OPENER, 'open', side_effect=serve):
            return recovery.retry_ranges(self.root, self.event, self.item, 2, self.tuning)

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
