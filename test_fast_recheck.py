"""Cheap availability probes preserve ordinary recovery and resume guarantees."""
import datetime
import errno
import hashlib
import json
from pathlib import Path
import unittest
import urllib.error
from unittest.mock import patch

import download
from file_state import FileState
import pipeline
import recovery
import settings
import test_resume
from test_regressions import Response


class FastRecheckChecks(unittest.TestCase):
    setUp = test_resume.ResumeChecks.setUp
    status = test_resume.ResumeChecks.status
    def serve(self, request, timeout):
        start, last = map(int, request.get_header('Range')[6:].split('-'))
        self.requests.append((start, last + 1))
        return Response(start, last + 1, len(self.content), self.content[start:last + 1])

    def prepare(self):
        self.content = b'A' * 8192
        self.item.update(size=len(self.content), sha256=hashlib.sha256(self.content).hexdigest())
        self.event.update(size=self.item['size'], sha256=self.item['sha256'], downloaded_bytes=64)
        download.write_json(Path(str(self.body)+'.ranges.json'), dict(size=self.item['size'], sha256=self.item['sha256'], ranges=[[0,64]]))
        recovery.recovery_state(self.root, self.event, 'finished')
        bg = pipeline.Background(self.root, self.source, 2,
            settings.resolve(dict(range_linear_block_size_kib=1, range_retry_delay=0)),
            defer_range=True, recheck_probes=4, recheck_full_interval=604800)
        self.addCleanup(bg.close)
        bg.submit('range', self.item, 'Course', 3, self.event, force=True, yandex_recheck=True)
        return bg, next(iter(bg.jobs))

    def unavailable(self, request, timeout):
        start, last = map(int, request.get_header('Range')[6:].split('-'))
        self.requests.append((start,last+1))
        raise urllib.error.HTTPError(request.full_url,500,'Unavailable',{},None)

    def test_empty_probe_is_bounded_and_next_recheck_rotates(self):
        bg,path = self.prepare()
        with patch.object(download.OPENER,'open',side_effect=self.unavailable):
            job = bg.work(path,bg.jobs[path])
        first = list(self.requests)
        self.assertLessEqual(len(first),4)
        self.assertEqual(job['result']['event']['range_attempts'][0]['attempt'],0)
        self.assertFalse(job['result']['event']['recheck_full_pass'])
        self.assertIn('yandex_checked_at',job)
        deadline = job['yandex_full_recheck_at']
        self.requests.clear()
        bg.submit('range',self.item,'Course',3,self.event,force=True,yandex_recheck=True)
        with patch.object(download.OPENER,'open',side_effect=self.unavailable):
            second = bg.work(path,bg.jobs[path])
        self.assertNotEqual(first,self.requests)
        self.assertEqual(second['yandex_full_recheck_at'],deadline)
        self.assertEqual(self.body.read_bytes(),self.content[:64])

    def test_successful_coarse_probe_finishes_original_with_sha(self):
        bg,path = self.prepare()
        with patch.object(download.OPENER,'open',side_effect=self.serve):
            job = bg.work(path,bg.jobs[path])
        self.assertEqual(self.requests,[(64,8192)])
        self.assertEqual((self.root/'Course/video.mp4').read_bytes(),self.content)
        self.assertTrue(job['result']['event']['range_recovered_complete'])
        self.assertNotIn('recheck_continue',job['result']['event'])

    def test_small_probe_discovery_expands_to_regular_recovery(self):
        bg,path = self.prepare()
        def serve(request,timeout):
            start,last = map(int,request.get_header('Range')[6:].split('-'))
            return self.unavailable(request,timeout) if last+1-start>1024 else self.serve(request,timeout)
        with patch.object(download.OPENER,'open',side_effect=serve):
            job = bg.work(path,bg.jobs[path])
        self.assertGreater(len(self.requests),4)
        self.assertTrue(job['result']['event']['recheck_full_pass'])
        self.assertEqual((self.root/'Course/video.mp4').read_bytes(),self.content)

    def test_periodic_full_pass_bypasses_probe(self):
        bg,path = self.prepare()
        bg.jobs[path]['yandex_full_recheck_at'] = (datetime.datetime.now().astimezone()-datetime.timedelta(seconds=1)).isoformat()
        with patch.object(download.OPENER,'open',side_effect=self.unavailable):
            job = bg.work(path,bg.jobs[path])
        self.assertEqual(job['result']['event']['range_attempts'][0]['attempt'],1)
        self.assertGreater(datetime.datetime.fromisoformat(job['yandex_full_recheck_at']),datetime.datetime.now().astimezone())

    def test_interrupted_discovery_resumes_full_recovery(self):
        bg,path = self.prepare()
        original = recovery.recovery_state
        def checkpoint(root,event,state):
            original(root,event,state)
            if event.get('recheck_continue'):
                raise InterruptedError('Stop after durable discovery')
        with patch.object(download.OPENER,'open',side_effect=self.serve),patch.object(recovery,'recovery_state',side_effect=checkpoint):
            with self.assertRaises(InterruptedError):bg.work(path,bg.jobs[path])
        saved = FileState(self.state,'recovery-state').get(self.item['path'])
        self.assertTrue(saved['event']['recheck_continue'])
        bg.submit('range',self.item,'Course',3,self.event,yandex_recheck=True)
        with patch.object(download.OPENER,'open',side_effect=self.serve):
            job = bg.work(path,bg.jobs[path])
        self.assertTrue(job['result']['event']['range_recovered_complete'])
        self.assertTrue(job['result']['event']['recheck_full_pass'])

    def test_interrupted_legacy_recheck_resumes_full_workflow(self):
        bg,path = self.prepare()
        legacy = bg.jobs[path]
        legacy.update(state='running', yandex_recheck=True)
        legacy.pop('yandex_full_recheck_at', None)
        download.write_json(path, legacy)
        bg.submit('range',self.item,'Course',3,self.event,yandex_recheck=True)
        with patch.object(download.OPENER,'open',side_effect=self.unavailable):
            job = bg.work(path,bg.jobs[path])
        self.assertEqual(job['result']['event']['range_attempts'][0]['attempt'],1)
        self.assertTrue(job['result']['event']['recheck_full_pass'])

    def test_route_error_is_retried_inside_probe_without_losing_file(self):
        bg,path = self.prepare()
        calls = 0
        def serve(request,timeout):
            nonlocal calls
            calls += 1
            if calls==1:raise urllib.error.URLError(OSError(errno.ENETUNREACH,'Network unreachable'))
            return self.serve(request,timeout)
        with patch.object(download.OPENER,'open',side_effect=serve),patch('network.RequestRetry.retry',return_value=True) as retry:
            job = bg.work(path,bg.jobs[path])
        retry.assert_called()
        self.assertTrue(job['result']['event']['range_recovered_complete'])

    def test_source_change_refuses_to_probe_old_body(self):
        bg,path = self.prepare()
        with patch.object(download,'api',return_value=dict(self.item,sha256='b'*64)),patch.object(download.OPENER,'open') as opened:
            job = bg.work(path,bg.jobs[path])
        opened.assert_not_called()
        self.assertIn('yandex_checked_at',job)
        self.assertEqual(self.body.read_bytes(),self.content[:64])

    def test_probe_planning_is_bounded_for_huge_sparse_files(self):
        total = 1<<50
        ranges = [[0,1024],[total//2,total//2+2048]]
        probes = recovery.recheck_probe_ranges(ranges,total,65536,16*1024**2,16,0)
        self.assertLessEqual(len(probes),16)
        for start,end in probes:
            self.assertTrue(0<=start<end<=total)
            self.assertFalse(any(start<b and end>a for a,b in ranges))
