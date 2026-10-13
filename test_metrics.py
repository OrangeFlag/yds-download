"""Metrics stay incremental, omit signed URLs, and never break downloads."""
import json
from pathlib import Path
import tempfile
from threading import Event, Thread
import unittest
from unittest.mock import patch

from metrics import Metrics, AriaMetrics


class MetricsChecks(unittest.TestCase):
    def test_aria_times_interleaved_connections_and_late_headers_without_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.touch()
            m=Metrics(root/'metrics.jsonl','test',start=False);aria=AriaMetrics(log,m)
            aria.begin({str(root/'a.mp4'):(100,None),str(root/'b.mp4'):(100,None)})
            log.write_text('''2026-10-13 12:00:00.000000 [INFO] CUID#7 - Connecting to 127.0.0.1:80
2026-10-13 12:00:00.500000 [INFO] CUID#8 - Connecting to 127.0.0.1:80
2026-10-13 12:00:01.000000 [INFO] CUID#7 - Requesting:
GET /file?filename=a.mp4&token=HIDDEN HTTP/1.1
2026-10-13 12:00:02.000000 [INFO] CUID#8 - Requesting:
GET /file?filename=b.mp4&token=HIDDEN HTTP/1.1
2026-10-13 12:00:16.900000 [INFO] CUID#8 - Restarting the download. URI=https://secret.invalid/?filename=b.mp4&token=HIDDEN
Exception: errorCode=2 Timeout.
2026-10-13 12:00:17.000000 [INFO] CUID#8 - 1 times attempted, but no success. Download aborted.
2026-10-13 12:00:17.000000 [ERROR] CUID#8 - Download aborted. URI=https://secret.invalid/?filename=b.mp4&token=HIDDEN
Exception: errorCode=2 Timeout.
2026-10-13 12:01:05.000000 [INFO] CUID#7 - Response received:
HTTP/1.1 200 OK
2026-10-13 12:01:08.000000 [INFO] CUID#7 - The download for one segment completed successfully.
'''+f'2026-10-13 12:01:08.100000 [NOTICE] Download complete: {root}/a.mp4\n')
            aria.sample();aria.sample();m.snapshot()
            row=json.loads(m.path.read_text().splitlines()[-1]);records=row.get('aria_attempt_details',[])
            self.assertEqual(len(records),2)
            by_path={r['path']:r for r in records}
            success=by_path[str(root/'a.mp4')];timeout=by_path[str(root/'b.mp4')]
            self.assertEqual(success['connect_ready_seconds'],1)
            self.assertEqual(success['response_headers_seconds'],64)
            self.assertAlmostEqual(success['body_phase_seconds'],3.1,places=5)
            self.assertFalse(success['censored'])
            self.assertEqual(timeout['phase'],'response_headers')
            self.assertEqual(timeout['stage_seconds'],15)
            self.assertIsNone(timeout['response_headers_seconds'])
            self.assertTrue(timeout['censored'])
            self.assertEqual(row['counters']['aria_timeout_connections'],1)
            self.assertEqual(row['counters']['aria_aborted_files'],1)
            self.assertEqual(row['errors']['aria_error_messages']['2'],1)
            self.assertNotIn('HIDDEN',json.dumps(row));m.close()

    def test_aria_body_timeout_after_segment_keeps_response_timing(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.touch()
            m=Metrics(root/'metrics.jsonl','test',start=False);aria=AriaMetrics(log,m);m.sample=aria.sample
            aria.begin({str(root/'a.mp4'):(100,None)})
            log.write_text('''2026-10-13 12:00:00.000000 [INFO] CUID#7 - Connecting to 127.0.0.1:80
2026-10-13 12:00:01.000000 [INFO] CUID#7 - Requesting:
GET /file?filename=a.mp4 HTTP/1.1
2026-10-13 12:00:02.000000 [INFO] CUID#7 - Response received:
HTTP/1.1 200 OK
2026-10-13 12:00:03.000000 [INFO] CUID#7 - The download for one segment completed successfully.
2026-10-13 12:00:04.000000 [INFO] CUID#7 - The download for one segment completed successfully.
2026-10-13 12:01:04.000000 [ERROR] CUID#7 - Download aborted. URI=https://example.invalid/?filename=a.mp4
Exception: errorCode=2 Timeout.
''')
            m.close();row=json.loads(m.path.read_text().splitlines()[-1]);records=row['aria_attempt_details']
            self.assertEqual(len(records),1)
            self.assertEqual(records[0]['phase'],'body')
            self.assertEqual(records[0]['body_phase_seconds'],62)
            self.assertEqual(records[0]['segments_completed'],2)
            self.assertEqual(records[0]['seconds_since_last_segment'],60)
            self.assertEqual(row['counters']['aria_connect_ready_samples'],1)
            self.assertEqual(row['counters']['aria_response_headers_samples'],1)
            self.assertEqual(row['counters']['aria_timeout_connections'],1)

    def test_aria_error_chain_split_across_reads_is_recorded_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.touch()
            m=Metrics(root/'metrics.jsonl','test',start=False);aria=AriaMetrics(log,m)
            aria.begin({str(root/'a.mp4'):(100,None)})
            log.write_text('''2026-10-13 12:00:00.000000 [INFO] CUID#7 - Connecting to 127.0.0.1:80
2026-10-13 12:00:01.000000 [INFO] CUID#7 - Requesting:
GET /file?filename=a.mp4 HTTP/1.1
2026-10-13 12:00:02.000000 [INFO] CUID#7 - Response received:
HTTP/1.1 206 Partial Content
2026-10-13 12:00:20.000000 [ERROR] CUID#7 - Download aborted. URI=https://secret.invalid/?filename=a.mp4
Exception: errorCode=''')
            aria.sample();m.snapshot()
            self.assertFalse(json.loads(m.path.read_text().splitlines()[-1]).get('aria_attempt_details'))
            with log.open('a') as stream:stream.write('2 Timeout.\n2026-10-13 12:00:20.100000 [NOTICE] Done\n')
            aria.sample();aria.sample();m.snapshot()
            row=json.loads(m.path.read_text().splitlines()[-1]);records=row.get('aria_attempt_details',[])
            self.assertEqual(len(records),1)
            self.assertEqual(records[0]['phase'],'body')
            self.assertEqual(records[0]['body_phase_seconds'],18)
            self.assertEqual(records[0]['outcome'],'timeout')
            self.assertEqual(row['counters']['aria_timeout_connections'],1);m.close()

    def test_aria_log_reset_reused_cuid_and_shutdown_keep_separate_observations(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);log=root/'aria2.log';log.touch()
            m=Metrics(root/'metrics.jsonl','test',start=False);aria=AriaMetrics(log,m);m.sample=aria.sample
            aria.begin({str(root/'short.mp4'):(100,None)},
                       {str(root/'short.mp4'):'https://example.invalid/?filename=original.mp4&hid=h&fsize=100'})
            first='''2026-10-13 12:00:00.000000 [INFO] CUID#7 - Connecting to 127.0.0.1:80
2026-10-13 12:00:01.000000 [INFO] CUID#7 - Requesting:
GET /?filename=original.mp4&hid=h&fsize=100 HTTP/1.1
'''
            log.write_text(first+'x'*1000+'\n');aria.sample()
            log.write_text('''2026-10-13 12:01:00.000000 [INFO] CUID#7 - Connecting to 127.0.0.1:80
2026-10-13 12:01:02.000000 [ERROR] CUID#7 - Download aborted. URI=https://example.invalid/?filename=original.mp4&hid=h&fsize=100
Exception: errorCode=2 Timeout.
''');aria.sample();m.close()
            rows=[json.loads(line) for line in m.path.read_text().splitlines()]
            details=[d for row in rows for d in row['aria_attempt_details']]
            self.assertEqual([d['outcome'] for d in details],['log_reset','timeout'])
            self.assertTrue(all(d['path']==str(root/'short.mp4') for d in details))
            self.assertEqual(details[-1]['phase'],'connect')
            self.assertEqual(details[-1]['stage_seconds'],2)
            self.assertEqual(m.counters['aria_timeout_connections'],1)

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
                m.aria_attempt(dict(path='/v',cuid='7',outcome='timeout',phase='body',
                                    connect_ready_seconds=1,response_headers_seconds=2,body_phase_seconds=3))
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
            self.assertEqual(len(latest['aria_attempt_details']), 1)
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

    def test_aria_detail_buffer_is_bounded_and_survives_failed_metrics_write(self):
        with tempfile.TemporaryDirectory() as directory:
            m=Metrics(Path(directory)/'metrics.jsonl','test',start=False)
            for index in range(1030):
                m.aria_attempt(dict(path='/v',cuid=str(index),outcome='timeout',phase='response_headers',
                                    connect_ready_seconds=1,response_headers_seconds=None,body_phase_seconds=None))
            with patch.object(Path,'write_text',side_effect=OSError('Disk unavailable')):m.snapshot()
            self.assertEqual(len(m.aria_attempts),1024)
            m.snapshot();row=json.loads(m.path.with_name('metrics-latest.json').read_text())
            self.assertEqual(row['counters']['aria_timeout_connections'],1030)
            self.assertEqual(row['counters']['aria_attempt_buffer_overwrites'],6)
            self.assertEqual(len(row['aria_attempt_details']),1024)
            self.assertFalse(m.aria_attempts);m.close()

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
