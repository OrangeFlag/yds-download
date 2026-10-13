"""Metrics stay incremental, omit signed URLs, and never break downloads."""
import json
from pathlib import Path
import tempfile
from threading import Event, Thread
import unittest
from unittest.mock import patch

from metrics import Metrics, AriaMetrics


class MetricsChecks(unittest.TestCase):
    def test_slow_latest_metrics_write_does_not_block_download_state_writes(self):
        from download import write_json
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            m = Metrics(root/'metrics.jsonl', 'test', start=False)
            entered, release, saved = Event(), Event(), Event()
            original = Path.write_text
            def write(path, *args, **kwargs):
                if path.name == 'metrics-latest.json.tmp':
                    entered.set()
                    release.wait(2)
                return original(path, *args, **kwargs)
            def checkpoint():
                write_json(root/'task.json', {'state': 'done'})
                saved.set()
            with patch.object(Path, 'write_text', write):
                snapshot = Thread(target=m.snapshot)
                snapshot.start()
                self.assertTrue(entered.wait(2))
                worker = Thread(target=checkpoint)
                worker.start()
                try:
                    self.assertTrue(saved.wait(.2), 'Metrics I/O blocked download state writes')
                finally:
                    release.set()
                    snapshot.join(2)
                    worker.join(2)
            self.assertEqual(json.loads((root/'task.json').read_text()), {'state': 'done'})
            m.close()

    def test_slow_snapshot_write_does_not_block_counters_or_share_mutable_values(self):
        with tempfile.TemporaryDirectory() as directory:
            m = Metrics(Path(directory)/'metrics.jsonl', 'test', start=False)
            entered, release, added = Event(), Event(), Event()
            rows = []
            original = Path.write_text
            def write(path, data, *args, **kwargs):
                if path.name == 'metrics-latest.json.tmp':
                    entered.set()
                    release.wait(2)
                    rows.append(json.loads(data))
                return original(path, data, *args, **kwargs)
            def update():
                m.add(range_requests=1)
                m.eof_retry(dict(path='/v',sha256='a'*64,attempt=2,start=0,end=7,
                                 accepted_bytes=7,request_seconds=.5,previous_eof_bytes=0,error=None))
                m.gauge('archives', {'finished': 1})
                m.range_request(dict(saved_bytes=0, error='HTTP 500', seconds=1,
                                     request_seconds=1, save_seconds=0, first_byte_seconds=None))
                added.set()
            with patch.object(Path, 'write_text', write):
                snapshot = Thread(target=m.snapshot)
                snapshot.start()
                self.assertTrue(entered.wait(2))
                worker = Thread(target=update)
                worker.start()
                try:
                    self.assertTrue(added.wait(.2), 'Metrics filesystem I/O blocked task updates')
                finally:
                    release.set()
                    snapshot.join(2)
                    worker.join(2)
            self.assertNotIn('archives', rows[0]['gauges'])
            self.assertNotIn('range', rows[0]['errors'])
            m.snapshot()
            latest = json.loads(m.path.with_name('metrics-latest.json').read_text())
            self.assertEqual(latest['interval_counters']['range_requests'], 2)
            self.assertEqual(len(latest['range_eof_retry_details']), 1)
            m.close()

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

    def test_eof_detail_buffer_is_bounded_and_counters_keep_all_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            m=Metrics(Path(directory)/'metrics.jsonl','test',start=False)
            for index in range(1030):
                m.eof_retry(dict(path='/v',sha256='a'*64,attempt=index+2,start=index,end=index+7,
                                 accepted_bytes=7,request_seconds=.5,previous_eof_bytes=0,error=None))
            self.assertEqual(len(m.eof_retries),1024)
            m.snapshot()
            row=json.loads(m.path.read_text().splitlines()[-1])
            self.assertEqual(row['counters']['range_eof_retry_accepted_bytes'],7210)
            self.assertEqual(row['counters']['range_eof_retry_buffer_overwrites'],6)
            self.assertEqual(len(row['range_eof_retry_details']),1024)
            self.assertFalse(m.eof_retries)
            m.close()

    def test_failed_metrics_write_retains_eof_details_and_accepts_new_data(self):
        with tempfile.TemporaryDirectory() as directory:
            m=Metrics(Path(directory)/'metrics.jsonl','test',start=False)
            detail=dict(path='/v',sha256='a'*64,attempt=2,start=0,end=7,
                        accepted_bytes=7,request_seconds=.5,previous_eof_bytes=0,error=None)
            m.eof_retry(detail)
            with patch.object(Path,'write_text',side_effect=OSError('Disk unavailable')):
                m.snapshot()
            self.assertEqual(len(m.eof_retries),1)
            m.eof_retry(detail)
            m.snapshot()
            self.assertFalse(m.eof_retries)
            row=json.loads(m.path.with_name('metrics-latest.json').read_text())
            self.assertEqual(row['counters']['range_eof_retry_attempts'],2)
            self.assertEqual([v['sequence'] for v in row['range_eof_retry_details']],[1,2])
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
