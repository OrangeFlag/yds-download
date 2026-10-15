"""Range planning and failure summaries must not scale with every remote block."""
import json
import hashlib
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

import download
from file_state import FileState
import pipeline
import recovery
import test_resume


class RangeMemoryChecks(unittest.TestCase):
    def test_lazy_chunks_keep_alignment_and_cursor_order(self):
        known = [[3,7],[11,16]]
        expected = [(0,3),(7,8),(8,11),(16,24),(24,25)]
        chunks = recovery.missing_chunks(known,25,8)
        self.assertNotIsInstance(chunks,list)
        self.assertEqual(list(chunks),expected)
        self.assertEqual(chunks[-1],expected[-1])
        for cursor,pivot in [(0,0),(8,2),(9,3),(99,0)]:
            rotated=recovery.missing_chunks(known,25,8,cursor)
            self.assertEqual(list(rotated),expected[pivot:]+expected[:pivot])
            self.assertTrue(all(bounds in rotated for bounds in expected))
            self.assertNotIn((8,12),rotated)
            self.assertNotIn((16,17),rotated)
        self.assertEqual(list(recovery.missing_chunks([[0,25]],25,8)),[])

    def test_huge_file_plans_only_the_selected_probes(self):
        self.assertNotIsInstance(recovery.missing_chunks([],16,8),list)
        tracemalloc.start()
        try:
            plan = recovery.missing_chunks([],1<<50,65536)
            probes = recovery.spread_probes(plan,128)
            self.assertEqual(len(plan),(1<<50)//65536)
            self.assertEqual(probes[0],(0,65536))
            self.assertEqual(probes[-1],((1<<50)-65536,1<<50))
            self.assertLess(tracemalloc.get_traced_memory()[1],1024**2)
        finally:
            tracemalloc.stop()

    def test_failure_summaries_compact_existing_rows_without_mutating_details(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'.download').mkdir();(root/'corrupted').mkdir()
            events=[dict(path=f'/Course/{n}.mp4',course='Course',size=100,sha256=str(n)*64,
                         corrupted_path=f'corrupted/old/Course/{n}.mp4',downloaded_bytes=40,
                         range_attempts=[dict(attempt=1,ranges=[dict(start=0,end=40,saved_bytes=40)])]) for n in (1,2)]
            for event in events:recovery.recovery_state(root,event,'finished')
            download.write_json(root/'corrupted/failed.json',events)
            pipeline.failure_manifests_batch(root,[events[0]])
            summaries=json.loads((root/'corrupted/failed.json').read_text())
            for row in summaries:
                self.assertNotIn('range_attempts',row)
                self.assertTrue(row['range_attempted'])
                self.assertEqual(row['downloaded_bytes'],40)
            for event in events:
                self.assertEqual(FileState(root/'.download','recovery-state').get(event['path'])['event']['range_attempts'],event['range_attempts'])


class CompactResumeChecks(unittest.TestCase):
    setUp=test_resume.ResumeChecks.setUp
    status=test_resume.ResumeChecks.status

    def test_legacy_compact_failure_keeps_completed_attempt_marker(self):
        self.event['range_attempts']=[dict(attempt=1,ranges=[])]
        download.write_failures(self.body.parents[1]/'failed.json',[self.event])
        with patch.object(download.OPENER,'open',side_effect=AssertionError('Retried finished legacy recovery')):
            results=recovery.recover_corrupted(self.root,self.source,retry=True)
        self.assertEqual(len(results),1)
        self.assertTrue(results[0]['range_attempted'])

    def test_pipeline_compact_legacy_recovery_is_not_replayed(self):
        self.event['range_attempts']=[dict(attempt=1,ranges=[])]
        download.write_failures(self.body.parents[1]/'failed.json',[self.event])
        with patch.object(download.OPENER,'open',side_effect=AssertionError('Pipeline retried finished legacy recovery')):
            code=download.download(dict(source=self.source,title='test'),['Course'],self.root,retries=0)
        self.assertEqual(code,1)
        self.assertEqual(FileState(self.state,'recovery-state').get(self.item['path'])['state'],'finished')


class RangeBufferChecks(unittest.TestCase):
    def test_saved_blocks_are_released_before_next_request(self):
        block = 2 * 1024 ** 2
        total = 4 * block
        class StreamingResponse:
            status = 206
            def __init__(self, start, end):
                self.headers = {'Content-Range': f'bytes {start}-{end-1}/{total}',
                                'Content-Length': str(end-start)}
                self.remaining = end-start
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read1(self, size):
                size = min(size, self.remaining)
                self.remaining -= size
                return b'x' * size
        def serve(request, **kwargs):
            start, end = map(int, request.get_header('Range')[6:].split('-'))
            return StreamingResponse(start, end+1)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'.download').mkdir()
            download.write_json(root/'.download/source.json', {'source': 'https://example.invalid/source'})
            item = dict(path='/Course/video.mp4', size=total, sha256='a'*64, file='https://example.invalid/video')
            event = dict(item, course='Course', corrupted_path='corrupted/video.mp4')
            with patch.object(download, 'api', return_value=item), patch.object(download.OPENER, 'open', side_effect=serve), redirect_stdout(io.StringIO()):
                tracemalloc.start()
                try:
                    ranges = recovery.retry_ranges(root, event, item, 0,
                        dict(range_workers=1, range_max_passes=1, range_block_size_kib=block//1024, range_rps=100000))
                    peak = tracemalloc.get_traced_memory()[1]
                finally:
                    tracemalloc.stop()
            self.assertEqual(ranges, [[0,total]])
            self.assertEqual(hashlib.sha256((root/'corrupted/video.mp4').read_bytes()).digest(),
                             hashlib.sha256(b'x'*total).digest())
            self.assertLess(peak, 2*block, f'Previous or duplicate block buffers retained: {peak}')
