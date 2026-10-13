"""Response framing and completed recovery proofs, without live archives or HTTP."""
from contextlib import redirect_stdout
import hashlib
import http.client
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import download
import recovery
from test_regressions import Response


class RangeIntegrityTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / '.download').mkdir()
        download.write_json(self.root / '.download/source.json', dict(source='https://disk.yandex.ru/d/test'))
        self.content = b'A' * 1024
        self.item = dict(path='/Course/video.mp4', size=1024,
                         sha256=hashlib.sha256(self.content).hexdigest(), file='https://example.invalid/file')
        self.event = dict(self.item, course='Course', corrupted_path='corrupted/old/Course/video.mp4',
                          time='2026-01-01', status='partial', attempt=1)
        self.body = self.root / self.event['corrupted_path']
        self.local = self.root / 'Course/video.mp4'
        self.tuning = dict(range_block_size_kib=1, range_min_block_size_kib=1,
                           range_linear_block_size_kib=1, range_max_passes=1,
                           range_workers=1, range_rps=100000)

    def ranges(self, response):
        with patch.object(download, 'api', return_value=self.item), \
                patch.object(download.OPENER, 'open', return_value=response), redirect_stdout(io.StringIO()):
            return recovery.retry_ranges(self.root, self.event, self.item, 0, self.tuning)

    def test_unbounded_surplus_is_rejected(self):
        self.assertEqual(self.ranges(Response(0, 1024, 1024, self.content + b'EXTRA')), [])
        self.assertFalse(self.body.exists())

    def test_chunked_surplus_is_rejected_by_real_http_parser(self):
        raw = (b'HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-1023/1024\r\n'
               b'Transfer-Encoding: chunked\r\n\r\n405\r\n' + self.content + b'EXTRA\r\n0\r\n\r\n')
        class Socket:
            def makefile(self, *args, **kwargs):
                return io.BytesIO(raw)
        response = http.client.HTTPResponse(Socket())
        response.begin()
        self.assertEqual(self.ranges(response), [])

    def test_valid_unbounded_eof_is_saved(self):
        self.assertEqual(self.ranges(Response(0, 1024, 1024, self.content)), [[0, 1024]])

    def test_eof_timeout_retains_received_bytes(self):
        self.assertEqual(self.ranges(Response(0, 1024, 1024, self.content, timeout=True)), [[0, 1024]])

    def test_bounded_response_does_not_probe_eof(self):
        response = Response(0, 1024, 1024, self.content, timeout=True)
        response.headers['Content-Length'] = '1024'
        self.assertEqual(self.ranges(response), [[0, 1024]])
        self.assertIsNone(self.event['range_attempts'][0]['ranges'][0]['error'])

    def test_conflicting_framing_is_rejected(self):
        response = Response(0, 1024, 1024, self.content)
        response.headers.update({'Content-Length': '1024', 'Transfer-Encoding': 'chunked'})
        self.assertEqual(self.ranges(response), [])

    def test_unsupported_transfer_encoding_is_rejected(self):
        for encoding in ('gzip', 'gzip, chunked', 'identity'):
            with self.subTest(encoding=encoding):
                response = Response(0, 1024, 1024, self.content)
                response.headers['Transfer-Encoding'] = encoding
                self.assertEqual(self.ranges(response), [])

    def complete_ledger(self):
        self.body.parent.mkdir(parents=True)
        self.body.write_bytes(self.content)
        download.write_json(Path(str(self.body) + '.ranges.json'),
                            dict(size=1024, sha256=self.item['sha256'], ranges=[[0, 1024]]))
        self.event.update(range_recovered_complete=True, recovered_stat=[1, 1024, 1])
        download.write_json(self.root / 'corrupted/old/failed.json', [self.event])
        recovery.recovery_state(self.root, self.event, 'finished')

    def retry(self):
        with patch.object(download, 'files', side_effect=AssertionError('Unexpected source listing')), \
                patch.object(download.OPENER, 'open', side_effect=AssertionError('Unexpected HTTP')), \
                redirect_stdout(io.StringIO()):
            return recovery.recover_corrupted(self.root, 'https://disk.yandex.ru/d/test', retry=True)

    def test_finished_full_ledger_restores_missing_copy_locally(self):
        self.complete_ledger()
        results = self.retry()
        self.assertEqual(self.local.read_bytes(), self.content)
        self.assertTrue(results[0]['range_recovered_complete'])

    def test_finished_full_ledger_does_not_replace_changed_copy(self):
        self.complete_ledger()
        self.local.parent.mkdir(parents=True)
        self.local.write_bytes(b'changed')
        results = self.retry()
        self.assertEqual(self.local.read_bytes(), b'changed')
        self.assertFalse(results[0].get('range_recovered_complete'))

    def test_direct_recovery_clears_stale_completion_before_failure(self):
        self.complete_ledger()
        self.local.parent.mkdir(parents=True)
        self.local.write_bytes(b'changed')
        with self.assertRaises(FileExistsError):
            recovery.finish_recovery(self.root, self.event, self.item)
        self.assertFalse(self.event.get('range_recovered_complete'))
