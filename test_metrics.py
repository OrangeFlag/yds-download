"""Metrics stay incremental, omit signed URLs, and never break downloads."""
import json
from pathlib import Path
import tempfile
from threading import Event
import unittest
from unittest.mock import patch

from metrics import Metrics, AriaMetrics


class MetricsChecks(unittest.TestCase):
    def test_range_pass_counts_bytes_errors_and_wait_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            m = Metrics(Path(directory)/'metrics.jsonl', 'test', interval=60, start=False)
            m.range_pass([dict(saved_bytes=7,error=None,seconds=3,request_seconds=2,save_seconds=.1,first_byte_seconds=.4),
                          dict(saved_bytes=0,error='HTTP 500',seconds=2,request_seconds=1,save_seconds=0,first_byte_seconds=None)], 4)
            m.snapshot()
            row=json.loads(m.path.read_text().splitlines()[-1])
            self.assertEqual(row['counters']['range_received_bytes'],7)
            self.assertEqual(row['counters']['range_requests'],2)
            self.assertEqual(row['counters']['range_wait_seconds'],2)
            self.assertEqual(row['errors']['range'], {'HTTP 500':1})
            m.close()

    def test_aria_reads_each_line_once_and_counts_resumed_bytes_correctly(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.write_text('old Download complete: old\n')
            m=Metrics(root/'metrics.jsonl','test',start=False)
            aria=AriaMetrics(log,m);body=root/'video.mp4';aria.begin({str(body):(10,6)})
            with log.open('a') as f:
                f.write('Download complete: '+str(body)+'\n')
                f.write('Download aborted. URI=https://secret.invalid/?token=HIDDEN\nerrorCode=2 Timeout.\n')
            aria.sample();aria.sample();m.snapshot()
            row=json.loads(m.path.read_text().splitlines()[-1])
            self.assertEqual(row['counters']['aria_completed_files'],1)
            self.assertEqual(row['counters']['aria_received_bytes'],4)
            self.assertEqual(row['counters']['aria_aborted_files'],1)
            self.assertNotIn('HIDDEN',m.path.read_text());m.close()

    def test_btrfs_virtual_device_uses_mountinfo_backing_disk(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            mountinfo=f'1 0 0:114 / {root} rw - btrfs /dev/sda1 rw\n'
            diskstats='8 0 sda 0 0 2 0 0 0 4 0 0 10 0 0 0 0\n'
            def resolve(path,*args,**kwargs):
                return Path('/mock/sda/sda1') if str(path)=='/sys/class/block/sda1' else path
            def read(path,*args,**kwargs):
                return mountinfo if str(path)=='/proc/self/mountinfo' else diskstats
            with patch.object(Path,'resolve',resolve),patch.object(Path,'exists',lambda p:str(p)=='/mock/sda/sda1/partition'), \
                 patch.object(Path,'read_text',read):
                m=Metrics(root/'metrics.jsonl','test',start=False)
                self.assertEqual(m.disk_name,'sda')
                self.assertEqual(m.host_previous['disk_read_bytes'],1024)
                self.assertEqual(m.host_previous['disk_busy_ms'],10)
            m.close()

    def test_existing_body_without_resume_proof_does_not_invent_new_traffic(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.touch()
            m=Metrics(root/'metrics.jsonl','test',start=False);aria=AriaMetrics(log,m)
            aria.begin({str(root/'existing.mp4'):(10,None)})
            log.write_text('Download complete: '+str(root/'existing.mp4')+'\n')
            aria.sample();m.close()
            self.assertEqual(m.counters['aria_received_bytes'],0)
            self.assertEqual(m.counters['aria_initial_unmeasured_bytes'],10)

    def test_shared_pool_keeps_metrics_open_until_inflight_result_is_recorded(self):
        from concurrent.futures import ThreadPoolExecutor
        from pipeline import Background
        from settings import resolve
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'.download').mkdir();release=Event()
            with ThreadPoolExecutor(max_workers=1) as pool:
                bg=Background(root,'https://disk.yandex.ru/d/test',0,resolve(),range_pool=pool)
                def work():
                    release.wait(2);bg.metrics.add(range_received_bytes=7)
                future=pool.submit(work);bg.futures[root/'task.json']=future
                try:
                    bg.close();self.assertFalse(bg.metrics.closed)
                finally:
                    release.set()
                future.result(timeout=2);bg.metrics.close()
            row=json.loads((root/'.download/metrics-latest.json').read_text())
            self.assertEqual(row['counters']['range_received_bytes'],7)

    def test_periodic_snapshot_and_write_failure_are_nonfatal(self):
        with tempfile.TemporaryDirectory() as directory:
            p=Path(directory)/'metrics.jsonl';m=Metrics(p,'test',interval=.02)
            ready=Event();m.sample=lambda:ready.set()
            self.assertTrue(ready.wait(2));m.close()
            self.assertGreaterEqual(len(p.read_text().splitlines()),2)
            with patch.object(Path,'open',side_effect=OSError('disk full')),patch('builtins.print'):
                m.snapshot()
