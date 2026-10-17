"""Background work must not hold up the next course's transfer."""
from file_state import FileState
import hashlib
from collections import namedtuple
import json
import os
import signal
from pathlib import Path
import tempfile
import sys
import subprocess
from threading import Event
import unittest
from unittest.mock import patch, Mock

import download
import recovery
import pipeline


class PipelineChecks(unittest.TestCase):
    def test_daily_recheck_bypasses_saved_finished_and_fallback_only(self):
        from settings import resolve
        state=self.root/'.download';state.mkdir()
        item=self.items['First'];event=dict(item,course='First',attempt=3,status='missing',corrupted_path='corrupted/old/First/video.mp4')
        download.write_json(state/'recovery-state.json',{item['path']:dict(state='finished',event=event)})
        bg=pipeline.Background(self.root,self.data['source'],2,resolve(),defer_range=True)
        self.addCleanup(bg.close)
        bg.submit('range',item,'First',3,event,fallback_only=True)
        path=next(iter(bg.jobs));bg.submit('range',item,'First',3,event,force=True,yandex_recheck=True)
        with patch.object(recovery,'finish_recovery') as recover:
            job=bg.work(path,bg.jobs[path])
        recover.assert_called_once()
        self.assertFalse(job.get('fallback_only',False))
        self.assertTrue(job['yandex_recheck'])
        checked=job['yandex_checked_at']
        bg.submit('range',item,'First',3,event,force=True,fallback_only=True)
        self.assertEqual(bg.jobs[path]['yandex_checked_at'],checked)

    def test_yandex_recheck_keeps_better_task_archive_than_stale_recovery_ledger(self):
        from settings import resolve
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        old = dict(item, course='First', attempt=3, status='partial',
                   corrupted_path='corrupted/old/First/video.mp4')
        better = dict(old, corrupted_path='corrupted/new/First/video.mp4')
        archive = self.root / better['corrupted_path']
        archive.parent.mkdir(parents=True)
        archive.write_bytes(b'lec')
        download.write_json(Path(str(archive) + '.ranges.json'),
                            dict(size=item['size'], sha256=item['sha256'], ranges=[[0, 3]]))
        recovery.recovery_state(self.root, old, 'finished')
        self.assertFalse(recovery.prefer_saved_event(self.root, better, old))
        bg = pipeline.Background(self.root, self.data['source'], 2, resolve(), defer_range=True)
        self.addCleanup(bg.close)
        bg.submit('range', item, 'First', 3, better, force=True, yandex_recheck=True)
        path = next(iter(bg.jobs))
        with patch.object(recovery, 'finish_recovery') as recover:
            bg.work(path, bg.jobs[path])
        self.assertEqual(recover.call_args.args[1]['corrupted_path'], better['corrupted_path'])

    def test_interrupted_daily_task_is_not_satisfied_by_old_finished_partial(self):
        from settings import resolve
        state=self.root/'.download';state.mkdir()
        item=self.items['First'];event=dict(item,course='First',attempt=3,status='missing',corrupted_path='corrupted/old/First/video.mp4')
        download.write_json(state/'recovery-state.json',{item['path']:dict(state='finished',event=event)})
        first=pipeline.Background(self.root,self.data['source'],2,resolve(),defer_range=True)
        first.submit('range',item,'First',3,event,force=True,yandex_recheck=True);first.close()
        resumed=pipeline.Background(self.root,self.data['source'],2,resolve(),defer_range=True);self.addCleanup(resumed.close)
        resumed.submit('range',item,'First',3,event)
        path=next(iter(resumed.jobs));self.assertEqual(resumed.jobs[path]['state'],'pending')
        with patch.object(recovery,'finish_recovery') as recover:resumed.work(path,resumed.jobs[path])
        recover.assert_called_once()

    def setUp(self):
        connectivity = patch('network.RequestRetry.retry', return_value=False)
        connectivity.start()
        self.addCleanup(connectivity.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.content = b'lecture'
        self.data = dict(source='https://disk.yandex.ru/d/test', title='test')
        self.items = {course: dict(path=f'/{course}/video.mp4', size=7,
                                  sha256=hashlib.sha256(self.content).hexdigest(),
                                  file='https://example.invalid/video')
                      for course in ('First', 'Second')}
        self.second_started = Event()
        self.calls = []
        self.addCleanup(self.second_started.set)

    def transfer(self, command, **kwargs):
        self.assertIn('--disable-ipv6=true', command)
        text = (self.root / '.download/queue.txt').read_text()
        course = 'Second' if '/Second' in text else 'First'
        self.calls.append(course)
        if course == 'Second':
            self.second_started.set()
        local = self.root / course / 'video.mp4'
        local.parent.mkdir(exist_ok=True)
        local.write_bytes(self.content)
        return 0

    def run_download(self, **kwargs):
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=self.transfer):
            return download.download(self.data, ['First', 'Second'], self.root, retries=0, **kwargs)

    def test_downloader_streams_event_history_without_bulk_jsonl_read(self):
        with patch.object(download, 'read_events',
                          side_effect=AssertionError('Whole event journal materialized')):
            self.assertEqual(self.run_download(), 0)
        events = list(download.iter_events(self.root / '.download/events.jsonl'))
        self.assertTrue(events)
        self.assertTrue(all(event['status'] == 'valid' for event in events))

    def test_pipeline_uses_compact_index_for_range_history_and_avoids_full_rescans(self):
        (self.root / '.download').mkdir()
        event=dict(self.items['First'],status='invalid',course='First',time='2026-10-01',
                   range_attempts=['X'*4096]*200)
        download.append_events(self.root/'.download/events.jsonl',[event])
        with patch.object(download,'iter_events',side_effect=AssertionError('Full event dictionary rebuilt')):
            self.assertEqual(self.run_download(),0)
        index=download.EventIndex(self.root/'.download/events.jsonl')
        proofs=index.refresh()
        self.assertNotIn('range_attempts',proofs[event['path']])
        self.assertEqual(proofs[event['path']]['sha256'],event['sha256'])
        self.assertEqual(proofs[event['path']]['status'],'valid')
        self.assertLess(len(str(proofs[event['path']])),500)

    def test_foreground_shared_budget_uses_configured_rps(self):
        from request_budget import ProcessRangeBudget
        budget = self.root / 'http-budget'
        ProcessRangeBudget(budget, rps=4)
        self.assertEqual(self.run_download(defer_range=True, range_budget=budget,
                                          tuning={'range_rps': 4}), 0)
        self.assertEqual(json.loads((budget / 'config.json').read_text())['rps'], 4)

    def test_file_timing_separates_queue_and_processing_across_interrupt(self):
        from datetime import datetime,timedelta,timezone
        clock={'seconds':0}
        stamp=lambda:(datetime(2026,10,13,tzinfo=timezone.utc)+timedelta(seconds=clock['seconds'])).isoformat()
        (self.root/'.download').mkdir()
        with patch.object(pipeline,'now',side_effect=stamp), \
                patch.object(pipeline.time,'monotonic',side_effect=lambda:clock['seconds']):
            bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
            self.addCleanup(bg.close)
            item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
            bg.submit('range',item,'First',0,event)
            path,job=next(iter(bg.jobs.items()))
            clock['seconds']=10
            def stop(*args,**kwargs):
                clock['seconds']=15
                raise InterruptedError('original interruption')
            with patch.object(recovery,'finish_recovery',side_effect=stop):
                with self.assertRaisesRegex(InterruptedError,'original interruption'):
                    bg.work(path,job)
            saved=json.loads(path.read_text())
            self.assertEqual(saved['timing']['processing_seconds'],5)
            self.assertEqual(saved['timing']['queue_seconds'],10)
            clock['seconds']=30
            bg.submit('range',item,'First',0,event)
            job=bg.jobs[path];clock['seconds']=40
            def finish(*args,**kwargs):clock['seconds']=50
            with patch.object(recovery,'finish_recovery',side_effect=finish):bg.work(path,job)
            saved=json.loads(path.read_text())
            self.assertEqual(saved['timing']['processing_seconds'],15)
            self.assertEqual(saved['timing']['queue_seconds'],35)
            self.assertEqual(saved['timing']['runs'],2)
            clock['seconds']=60
            bg.submit('range',item,'First',0,event,force=True)
            job=bg.jobs[path];clock['seconds']=65
            def finish_again(*args,**kwargs):clock['seconds']=70
            with patch.object(recovery,'finish_recovery',side_effect=finish_again):bg.work(path,job)
            saved=json.loads(path.read_text())
            self.assertEqual(saved['timing']['processing_seconds'],20)
            self.assertEqual(saved['timing']['queue_seconds'],40)
            self.assertEqual(saved['timing']['runs'],3)

    def test_unclean_stop_is_marked_once_without_inventing_processing_time(self):
        (self.root/'.download').mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
        bg.submit('range',item,'First',0,event)
        path,job=next(iter(bg.jobs.items()))
        job.update(state='running',timing=dict(started_at='2026-10-13T00:00:00+00:00',
                   processing_seconds=5,queue_seconds=3,coverage_since='2026-10-12T00:00:00+00:00'))
        download.write_json(path,job)
        bg.submit('range',item,'First',0,event)
        bg.submit('range',item,'First',0,event)
        saved=json.loads(path.read_text())['timing']
        self.assertEqual(saved['unmeasured_interrupts'],1)
        self.assertEqual(saved['processing_seconds'],5)
        self.assertEqual(saved['queue_seconds'],3)

    def test_finished_recovery_proof_keeps_timing_without_repeating_work(self):
        (self.root/'.download').mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
        bg.submit('range',item,'First',0,event)
        path,job=next(iter(bg.jobs.items()))
        job.update(state='running',timing=dict(started_at='2026-10-13T00:00:00+00:00',
                   processing_seconds=5,queue_seconds=10,runs=2))
        download.write_json(path,job)
        recovery.recovery_state(self.root,event,'finished')
        with patch.object(recovery,'prefer_saved_event',return_value=True), \
                patch.object(bg,'range_result_current',return_value=True):
            bg.submit('range',item,'First',0,event)
        saved=json.loads(path.read_text())
        self.assertEqual(saved['state'],'done')
        self.assertEqual(saved['timing']['processing_seconds'],5)
        self.assertEqual(saved['timing']['queue_seconds'],10)
        self.assertEqual(saved['timing']['unmeasured_interrupts'],1)
        self.assertNotIn('started_at',saved['timing'])
        self.assertNotIn('queued_since',saved['timing'])

    def test_forced_job_ignores_unreadable_optional_timing_history(self):
        (self.root/'.download').mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
        bg.submit('range',item,'First',0,event)
        path=next(iter(bg.jobs))
        for content in ('{truncated', '[]'):
            path.write_text(content)
            bg.submit('range',item,'First',0,event,force=True)
            self.assertEqual(json.loads(path.read_text())['state'],'pending')

    def test_timing_and_optional_checkpoint_errors_do_not_mask_interruption(self):
        (self.root/'.download').mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
        bg.submit('range',item,'First',0,event)
        path,job=next(iter(bg.jobs.items()))
        original=download.write_json
        def write(path,job):
            if job.get('timing',{}).get('stopped_at'):
                raise OSError('Checkpoint unavailable')
            return original(path,job)
        with patch.object(recovery,'finish_recovery',side_effect=InterruptedError('original interruption')), \
                patch.object(download,'write_json',side_effect=write):
            with self.assertRaisesRegex(InterruptedError,'original interruption'):
                bg.work(path,job)

    def test_broken_optional_timing_does_not_fail_successful_work(self):
        (self.root/'.download').mkdir()
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve(),defer_range=True)
        self.addCleanup(bg.close)
        item=self.items['First'];event=dict(item,course='First',corrupted_path='corrupted/v')
        bg.submit('range',item,'First',0,event)
        path,job=next(iter(bg.jobs.items()))
        with patch.object(recovery,'finish_recovery'), \
                patch.object(bg.metrics,'begin_file',side_effect=ValueError('Bad metrics')), \
                patch.object(bg.metrics,'finish_file',side_effect=ValueError('Bad metrics')):
            result=bg.work(path,job)
        self.assertEqual(result['state'],'done')
        self.assertEqual(json.loads(path.read_text())['state'],'done')

    def ready_range_jobs(self, count):
        from concurrent.futures import Future
        (self.root / '.download').mkdir()
        background = pipeline.Background(self.root, self.data['source'], 0, pipeline.resolve())
        self.addCleanup(background.close)
        paths = []
        for index in range(count):
            item = dict(self.items['First'], path=f'/First/file{index}.mp4')
            event = dict(item, course='First', attempt=1, status='partial', detail='Missing bytes',
                         time='2026-01-01', corrupted_path=f'corrupted/old/First/file{index}.mp4')
            job = dict(kind='range', source=self.data['source'], item=item, course='First',
                       attempt=1, state='done', event=event, result=dict(event=event))
            path = background.directory / f'{index}.json'
            download.write_json(path, job)
            future = Future(); future.set_result(job)
            background.futures[path] = future
            background.jobs[path] = job
            paths.append(path)
        return background, paths

    def test_completed_range_drops_duplicate_large_event_from_transfer_ledger(self):
        background, paths = self.ready_range_jobs(1)
        event = background.jobs[paths[0]]['result']['event']
        event['range_attempts'] = ['history' * 5000 for _ in range(20)]
        with patch.object(background, 'range_result_current', return_value=True):
            background.poll()
        ledger = background.transfer_state
        self.assertEqual(ledger.get(event['path'])['phase'], 'finished')
        self.assertNotIn('event', ledger.get(event['path']))
        self.assertEqual(json.loads(paths[0].read_text())['result']['event']['range_attempts'],
                         event['range_attempts'])

    def test_legacy_finished_transfer_event_is_compacted_without_dropping_active_resume(self):
        from settings import resolve
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        finished = dict(source=self.data['source'], size=item['size'], sha256=item['sha256'],
                        course='First', attempt=3, phase='finished',
                        event={'range_attempts': ['x' * 50000]})
        pending = dict(finished, phase='range', event=dict(corrupted_path='corrupted/old'))
        path = state / 'transfer-state.json'
        download.write_json(path, {item['path']: finished, '/Second/video.mp4': pending})
        bg = pipeline.Background(self.root, self.data['source'], 2, resolve(), defer_range=True)
        self.addCleanup(bg.close)
        self.assertNotIn('event', bg.transfer_state.get(item['path']))
        self.assertEqual(bg.transfer_state.get('/Second/video.mp4')['event'], pending['event'])
        bg.checkpoint(item, 'First', 3, 'finished')
        saved = bg.transfer_state
        self.assertNotIn('event', saved.get(item['path']))
        self.assertEqual(saved.get('/Second/video.mp4')['event'], pending['event'])

    def test_poll_batches_ledger_and_failure_manifest_before_applied_jobs(self):
        background, paths = self.ready_range_jobs(70)
        original = download.write_json
        writes = []
        def write(path, data):
            if path.is_relative_to(background.transfer_state.directory) or path == self.root / 'corrupted/failed.json':
                writes.append(path)
            if path in paths and data['state'] == 'applied':
                ledger = background.transfer_state
                failures = json.loads((self.root / 'corrupted/failed.json').read_text())
                self.assertEqual(ledger.get(data['item']['path'])['phase'], 'finished')
                self.assertIn(data['item']['path'], [row['path'] for row in failures])
            original(path, data)
        with patch.object(background, 'range_result_current', return_value=True),                 patch.object(download, 'write_json', side_effect=write):
            background.poll()
            self.assertEqual(len(background.futures), 6)
            self.assertEqual(sum(path.is_relative_to(background.transfer_state.directory) for path in writes), 64)
            self.assertEqual(writes.count(self.root / 'corrupted/failed.json'), 1)
            background.poll()
        self.assertFalse(background.futures)
        self.assertEqual(sum(path.is_relative_to(background.transfer_state.directory) for path in writes), 70)
        self.assertEqual(len(json.loads((self.root / 'corrupted/failed.json').read_text())), 70)

    def test_interrupted_batch_retains_done_tasks_for_reapplication(self):
        background, paths = self.ready_range_jobs(3)
        original = download.write_json
        def interrupt(path, data):
            if path.is_relative_to(background.transfer_state.directory):
                raise KeyboardInterrupt
            original(path, data)
        with patch.object(background, 'range_result_current', return_value=True):
            with patch.object(download, 'write_json', side_effect=interrupt):
                with self.assertRaises(KeyboardInterrupt):
                    background.poll()
            self.assertTrue(all(json.loads(path.read_text())['state'] == 'done' for path in paths))
            background.poll()
        self.assertTrue(all(json.loads(path.read_text())['state'] == 'applied' for path in paths))

    def test_deep_transfer_uses_ipv4(self):
        with patch.object(download, 'list_course', return_value=[self.items['First']]),                 patch('pipeline.transfer_process', side_effect=self.transfer):
            self.assertEqual(download.download(self.data, ['First'], self.root, retries=0,
                                               deep=True, retry_check=True), 0)

    def test_transfer_batches_ledger_before_transfer_and_sha(self):
        state = self.root / '.download'
        state.mkdir()
        background = pipeline.Background(self.root, self.data['source'], 0, pipeline.resolve())
        self.addCleanup(background.close)
        items = [dict(self.items['First'], path=f'/First/file{i}.mp4') for i in range(20)]
        original = download.write_json
        writes = []
        def write(path, data):
            if path.is_relative_to(background.transfer_state.directory):
                writes.append(dict(data))
            original(path, data)
        def expect_phase(phase):
            saved = {item['path']: background.transfer_state.get(item['path']) for item in items}
            self.assertTrue(all(saved.values()))
            self.assertEqual({row['phase'] for row in saved.values()}, {phase})
        def transfer(*args, **kwargs):
            expect_phase('downloading')
            return 0
        def submit(*args, **kwargs):
            expect_phase('verifying')
        with patch.object(download, 'write_json', side_effect=write), \
                patch.object(pipeline, 'transfer_process', side_effect=transfer), \
                patch.object(background, 'submit', side_effect=submit):
            background.transfer(items, 'First')
        self.assertEqual(len(writes), 2*len(items))
        background.checkpoint(items[0], 'First', 1, 'finished')
        self.assertEqual(background.transfer_state.get(items[0]['path'])['phase'], 'finished')

    def test_deep_preserves_deferred_range_handoff(self):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Unexpected Range')):
            self.assertEqual(self.run_download(defer_range=True), 1)
        state = self.root / '.download'
        before = json.loads((state / 'summary.json').read_text())
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0, deep=True), 1)
        after = json.loads((state / 'summary.json').read_text())
        self.assertTrue(after['downloads_complete'])
        self.assertEqual(after['pending_courses'], before['pending_courses'])
        self.assertFalse(after['complete'])

    def test_restart_restores_finished_recovery_before_course_finalization(self):
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        event = dict(item, course='First', attempt=1, status='missing', detail='No local file',
                     time='2026-01-01', corrupted_path='corrupted/old/First/video.mp4',
                     range_recovered_complete=True, recovered_stat=[1, 7, 1])
        body = self.root / event['corrupted_path']
        body.parent.mkdir(parents=True)
        body.write_bytes(self.content)
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=7, sha256=item['sha256'], ranges=[[0, 7]]))
        download.write_json(self.root / 'corrupted/old/failed.json', [event])
        download.write_json(state / 'course-history.json', {'First': dict(
            source=self.data['source'], state='recovering', verified=0, failed=0)})
        download.write_json(state / 'transfer-state.json', {item['path']: dict(
            source=self.data['source'], size=7, sha256=item['sha256'], course='First',
            attempt=1, phase='finished', event=event)})
        download.write_json(state / 'recovery-state.json', {item['path']: dict(state='finished', event=event)})
        with patch.object(download, 'list_course', return_value=[item]), \
                patch.object(recovery, 'retry_ranges'), \
                patch('pipeline.transfer_process', side_effect=AssertionError('Unexpected transfer')):
            self.assertEqual(download.download(self.data, ['First'], self.root, retries=0), 0)
        self.assertEqual((self.root / 'First/video.mp4').read_bytes(), self.content)

    def test_deep_retry_rejects_changed_or_missing_source_before_resuming(self):
        item = self.items['First']
        for fresh in ([dict(item, sha256='0' * 64, file='https://example.invalid/new')],
                      [dict(item, size=8, file='https://example.invalid/new')], []):
            with self.subTest(fresh=fresh), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                def partial(*args, **kwargs):
                    local = root / 'First/video.mp4'
                    local.parent.mkdir(exist_ok=True)
                    local.write_bytes(self.content[:3])
                    Path(str(local) + '.aria2').write_bytes(b'control')
                    return 1
                with patch.object(download, 'list_course', side_effect=[[item], fresh]), \
                        patch.object(recovery, 'recover_corrupted', return_value=[]), \
                        patch.object(recovery, 'finish_recovery'), \
                        patch.object(pipeline, 'transfer_process', side_effect=partial) as transfer:
                    with self.assertRaisesRegex(ValueError, 'Source changed'):
                        download.download(self.data, ['First'], root, retries=1, deep=True,
                                          retry_check=True, tuning={'retry_delay': 0})
                    self.assertEqual(transfer.call_count, 1)
                self.assertEqual((root / 'First/video.mp4').read_bytes(), self.content[:3])

    def test_scoped_deep_preserves_shared_queue_selection_settings_and_slots(self):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Unexpected Range')):
            self.assertEqual(self.run_download(defer_range=True), 1)
        state = self.root / '.download'
        settings = json.loads((state / 'settings.json').read_text())
        settings['retries'] = 5
        settings['tuning']['range_workers'] = 3
        download.write_json(state / 'settings.json', settings)
        saved = {name: (state / name).read_bytes() for name in (
            'courses.json', 'catalog.json', 'settings.json', '01-manifest.json',
            '02-manifest.json', '01-status.json')}
        self.items['Third'] = dict(self.items['Second'], path='/Third/video.mp4')
        local = self.root / 'Third/video.mp4'
        local.parent.mkdir()
        local.write_bytes(self.content)
        for course in ('Second', 'Third'):
            with self.subTest(course=course), patch.object(download, 'list_course', return_value=[self.items[course]]):
                self.assertEqual(download.download(self.data, [course], self.root, retries=0, deep=True), 0)
            for name, before in saved.items():
                self.assertEqual((state / name).read_bytes(), before, name)
            self.assertEqual(json.loads((state / '02-status.json').read_text())['course'], 'Second')
            summary = json.loads((state / 'summary.json').read_text())
            self.assertEqual(summary['verified'], 1)
            self.assertEqual(summary['pending_courses'], ['First'])
            self.assertFalse(summary['complete'])

    def test_sha_does_not_block_next_course(self):
        original = download.inspect
        def inspect(root, item, course, *args, **kwargs):
            if course == 'First':
                self.assertTrue(self.second_started.wait(2), 'SHA blocked the next transfer')
            return original(root, item, course, *args, **kwargs)
        with patch.object(download, 'inspect', side_effect=inspect):
            self.assertEqual(self.run_download(), 0)
        self.assertTrue(json.loads((self.root / '.download/01-status.json').read_text())['complete'])

    def test_completed_course_is_published_during_next_transfer(self):
        original_popen = subprocess.Popen
        original_inspect = download.inspect
        observed = self.root / 'observed'
        first = self.root / 'First/video.mp4'
        first.parent.mkdir()
        first.write_bytes(self.content)
        def inspect(root, item, course, *args, **kwargs):
            if course == 'First':
                self.assertTrue(self.second_started.wait(2))
            return original_inspect(root, item, course, *args, **kwargs)
        def popen(command, **kwargs):
            self.second_started.set()
            script = ("import pathlib,time; root=pathlib.Path(" + repr(str(self.root)) + "); "
                      "deadline=time.monotonic()+8\n"
                      "while time.monotonic()<deadline:\n"
                      " q=root/'.download/02-status.json'\n"
                      " if q.exists() and '\"state\": \"finished\"' in q.read_text():\n"
                      "  (root/'premature').touch()\n"
                      " p=root/'.download/01-status.json'\n"
                      " if p.exists() and '\"state\": \"finished\"' in p.read_text():\n"
                      "  (root/'observed').touch(); break\n"
                      " time.sleep(.05)\n"
                      "p=root/'Second/video.mp4'; p.parent.mkdir(exist_ok=True); p.write_bytes(b'lecture')\n")
            return original_popen([sys.executable, '-c', script], **kwargs)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch.object(download, 'inspect', side_effect=inspect), \
                patch.object(subprocess, 'Popen', side_effect=popen):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0), 0)
        self.assertTrue(observed.exists(), 'Finished course waited for the entire next transfer')
        self.assertFalse((self.root / 'premature').exists(), 'Active download was finalized early')

    def test_cli_interrupt_is_quiet_and_preserves_exit_code(self):
        with patch('builtins.print') as message:
            self.assertEqual(download.cli(Mock(side_effect=KeyboardInterrupt)), 130)
        message.assert_called_once_with('STOPPED: interrupted; resume state retained', flush=True)

    def test_repeated_sigint_does_not_interrupt_cleanup_or_stop_message(self):
        cleaned = []
        previous = signal.getsignal(signal.SIGINT)
        def entrypoint():
            try:
                os.kill(os.getpid(), signal.SIGINT)
            finally:
                os.kill(os.getpid(), signal.SIGINT)
                cleaned.append(True)
        def stop_message(*args, **kwargs):
            os.kill(os.getpid(), signal.SIGINT)
        with patch('builtins.print', side_effect=stop_message):
            self.assertEqual(download.cli(entrypoint), 130)
        self.assertEqual(cleaned, [True])
        self.assertEqual(signal.getsignal(signal.SIGINT), previous)

    def test_queue_interrupt_stops_and_reaps_download_process(self):
        process = Mock()
        process.wait.side_effect = [KeyboardInterrupt, 130]
        process.poll.return_value = None
        with patch.object(subprocess, 'Popen', return_value=process):
            with self.assertRaises(KeyboardInterrupt):
                pipeline.transfer_process(['download.py'], env={}, on_tick=lambda: None, stop_timeout=80)
        process.send_signal.assert_called_once_with(pipeline.signal.SIGINT)
        self.assertEqual(process.wait.call_args.kwargs, {'timeout': 80})

    def test_callback_failure_stops_and_reaps_transfer(self):
        process = Mock()
        process.wait.side_effect = [subprocess.TimeoutExpired('aria2c', 5), 130]
        process.poll.return_value = None
        with patch.object(subprocess, 'Popen', return_value=process):
            with self.assertRaisesRegex(RuntimeError, 'background failed'):
                pipeline.transfer_process(['aria2c'], env={}, on_tick=Mock(side_effect=RuntimeError('background failed')))
        process.send_signal.assert_called_once_with(pipeline.signal.SIGINT)
        self.assertEqual(process.wait.call_count, 2)

    def test_restart_applies_saved_sha_before_earlier_course_transfer(self):
        state = self.root / '.download'
        state.mkdir()
        second = self.root / 'Second/video.mp4'
        second.parent.mkdir()
        second.write_bytes(self.content)
        background = pipeline.Background(self.root, self.data['source'], 0, pipeline.resolve())
        background.checkpoint(self.items['Second'], 'Second', 1, 'verifying')
        background.submit('sha', self.items['Second'], 'Second', 1)
        next(iter(background.futures.values())).result()
        background.close()
        original = self.transfer
        def transfer(command, **kwargs):
            kwargs['on_tick']()
            proofs = list(download.read_events(state / 'events.jsonl'))
            self.assertTrue(any(row['path'] == self.items['Second']['path'] and row['status'] == 'valid' for row in proofs),
                            'Saved result waited for its course to be reached')
            return original(command, **kwargs)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0), 0)

    def test_restart_starts_unfinished_sha_before_earlier_course_transfer(self):
        base=self.root
        for state_name in ('pending','running'):
            with self.subTest(state=state_name):
                self.root=base/state_name;state=self.root/'.download';state.mkdir(parents=True)
                local=self.root/'Second/video.mp4';local.parent.mkdir();local.write_bytes(self.content)
                bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve())
                bg.checkpoint(self.items['Second'],'Second',1,'verifying')
                bg.submit('sha',self.items['Second'],'Second',1)
                next(iter(bg.futures.values())).result();bg.close()
                task=next((state/'tasks').glob('*.json'));job=json.loads(task.read_text())
                job.update(state=state_name);job.pop('result',None);download.write_json(task,job)
                started=Event();real_inspect=download.inspect
                def inspect(root,item,*args,**kwargs):
                    if item['path']==self.items['Second']['path']:started.set()
                    return real_inspect(root,item,*args,**kwargs)
                def transfer(command,**kwargs):
                    self.assertTrue(started.wait(2),'Unfinished SHA waited behind earlier course')
                    return self.transfer(command,**kwargs)
                with patch.object(download,'list_course',side_effect=lambda source,course,*args:[self.items[course]]), \
                        patch.object(download,'inspect',side_effect=inspect), \
                        patch('pipeline.transfer_process',side_effect=transfer):
                    self.assertEqual(download.download(self.data,['First','Second'],self.root,retries=0),0)

    def test_restart_never_submits_sha_for_a_superseded_resume_binding(self):
        state=self.root/'.download';state.mkdir()
        local=self.root/'Second/video.mp4';local.parent.mkdir();local.write_bytes(self.content)
        bg=pipeline.Background(self.root,self.data['source'],0,pipeline.resolve())
        bg.checkpoint(self.items['Second'],'Second',1,'verifying')
        bg.submit('sha',self.items['Second'],'Second',1)
        next(iter(bg.futures.values())).result();bg.close()
        changed=b'changed';local.write_bytes(changed)
        download.write_json(state/'resume-identities.json',{self.items['Second']['path']:
                            dict(size=len(changed),sha256=hashlib.sha256(changed).hexdigest())})
        real_submit=pipeline.Background.submit
        def submit(background,kind,item,*args,**kwargs):
            self.assertFalse(kind=='sha' and item['sha256']==self.items['Second']['sha256'],
                             'Obsolete SHA task submitted against a replacement')
            return real_submit(background,kind,item,*args,**kwargs)
        class StartupChecked(Exception):pass
        with patch.object(download,'list_course',side_effect=lambda source,course,*args:[self.items[course]]), \
                patch.object(pipeline.Background,'submit',submit), \
                patch('pipeline.transfer_process',side_effect=StartupChecked):
            with self.assertRaises(StartupChecked):
                download.download(self.data,['First','Second'],self.root,retries=0)
        self.assertEqual(local.read_bytes(),changed)

    def test_completed_old_sha_cannot_archive_a_new_download(self):
        self.check_superseded_sha(False)

    def test_failed_old_sha_cannot_abort_a_new_download(self):
        self.check_superseded_sha(True)

    def check_superseded_sha(self, raises):
        state = self.root / '.download'
        state.mkdir()
        item = self.items['Second']
        local = self.root / 'Second/video.mp4'
        local.parent.mkdir()
        local.write_bytes(self.content)
        changed = b'changed'
        replacement = dict(item, sha256=hashlib.sha256(changed).hexdigest())
        download.bind_resume(self.root, [item], {}, {}, False)
        started, release = Event(), Event()
        bg = pipeline.Background(self.root, self.data['source'], 0, pipeline.resolve())
        bg.bindings = {item['path']: item}
        self.addCleanup(bg.close)
        self.addCleanup(release.set)
        bg.checkpoint(item, 'Second', 1, 'verifying')
        def inspect(*args, **kwargs):
            started.set()
            self.assertTrue(release.wait(2))
            if raises:
                raise FileNotFoundError('Old source body was archived')
            return 'invalid', 'File changed during SHA verification'
        with patch.object(download, 'inspect', side_effect=inspect):
            bg.submit('sha', item, 'Second', 1)
            self.assertTrue(started.wait(2))
            download.bind_resume(self.root, [replacement], {item['path']: item}, {}, False)
            bg.bindings.update({replacement['path']: replacement})
            bg.checkpoint(replacement, 'Second', 1, 'downloading')
            local.write_bytes(changed)
            control = Path(str(local) + '.aria2')
            control.write_bytes(b'resume')
            release.set()
            future = next(iter(bg.futures.values()))
            if raises:
                with self.assertRaises(FileNotFoundError):
                    future.result()
            else:
                future.result()
            bg.poll()
        self.assertEqual(local.read_bytes(), changed)
        self.assertTrue(control.exists())
        self.assertEqual(bg.continuation(replacement)['phase'], 'downloading')
        self.assertFalse(bg.jobs)
        self.assertFalse(list(download.read_events(state / 'events.jsonl')))

    def test_range_does_not_block_next_course(self):
        self.items['First']['size'] = 20
        def ranges(root, event, item, *args, **kwargs):
            self.assertTrue(self.second_started.wait(2), 'Range blocked the next transfer')
        with patch.object(recovery, 'retry_ranges', side_effect=ranges):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual(self.calls, ['First', 'Second'])
        self.assertEqual(json.loads((self.root / '.download/01-status.json').read_text())['state'], 'finished')

    def test_deferred_range_leaves_durable_work_without_waiting(self):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Range blocked archive handoff')):
            self.assertEqual(self.run_download(defer_range=True), 1)
        state = self.root / '.download'
        jobs = [json.loads(path.read_text()) for path in (state / 'tasks').glob('*.json')]
        self.assertEqual([(job['course'], job['state']) for job in jobs if job['kind'] == 'range'], [('First', 'pending')])
        self.assertEqual(json.loads((state / '01-status.json').read_text())['state'], 'recovering')
        self.assertTrue(json.loads((state / 'summary.json').read_text())['downloads_complete'])
        with patch('pipeline.transfer_process', side_effect=AssertionError('Ordinary download repeated')):
            with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]):
                self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=0, defer_range=True), 1)

    def test_sha_mismatch_has_bounded_fresh_downloads(self):
        bad = self.root / 'First/video.mp4'
        bad.parent.mkdir()
        bad.write_bytes(b'garbage')
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(bad.read_bytes(), self.content)
        self.assertTrue(list((self.root / 'corrupted').glob('**/video.mp4')))

    def test_restart_keeps_retry_after_invalid_body_was_archived(self):
        local = self.root / 'First/video.mp4'
        local.parent.mkdir()
        local.write_bytes(b'garbage')
        original = pipeline.Background.retry_ready
        def interrupt(background):
            background.poll()
            if background.retry:
                raise KeyboardInterrupt
            return original(background)
        with patch.object(pipeline.Background, 'retry_ready', side_effect=interrupt, autospec=True):
            with self.assertRaises(KeyboardInterrupt):
                self.run_download()
        self.calls.clear()
        with patch.object(recovery, 'retry_ranges', side_effect=AssertionError('Remaining download retry was lost')):
            self.assertEqual(self.run_download(), 0)
        self.assertIn('First', self.calls)
        self.assertEqual(local.read_bytes(), self.content)

    def test_restart_only_verifies_a_completed_unverified_transfer(self):
        original = download.inspect
        def interrupt(root, item, course, *args, **kwargs):
            if course == 'First':
                raise KeyboardInterrupt
            return original(root, item, course, *args, **kwargs)
        with patch.object(download, 'inspect', side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.run_download()
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=AssertionError('Completed file transferred again')):
            self.assertEqual(download.download(self.data, ['First'], self.root, retries=0), 0)

    def test_external_original_survives_a_newer_missing_root_event(self):
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        external = self.root / 'external.mp4'
        external.write_bytes(self.content)
        event = download.record(state, item, 'First', 0, 'valid', 'SHA verified', local=external)
        download.write_json(state / 'local-copies.json', [dict(event, source=self.data['source'])])
        download.record(state, item, 'First', 0, 'missing', 'Root absent')
        self.assertEqual(self.run_download(), 0)
        self.assertEqual(self.calls, ['Second'])

    def test_bad_downloads_exhaust_exactly_the_configured_retries(self):
        original = self.transfer
        def transfer(command, **kwargs):
            code = original(command, **kwargs)
            if self.calls[-1] == 'First':
                (self.root / 'First/video.mp4').write_bytes(b'garbage')
            return code
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer), \
                patch.object(recovery, 'retry_ranges', return_value=None), \
                patch.object(download.time, 'sleep'):
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=1), 1)
            self.assertEqual(self.calls.count('First'), 2)
            self.assertEqual(self.calls.count('Second'), 1)
            self.assertEqual(download.download(self.data, ['First', 'Second'], self.root, retries=1), 1)
            self.assertEqual(self.calls.count('First'), 2)

    def interrupted_archive(self, split_move):
        self.items['First']['size'] = 20
        original_transfer = self.transfer
        def transfer(command, **kwargs):
            code = original_transfer(command, **kwargs)
            if self.calls[-1] == 'First':
                local = self.root / 'First/video.mp4'
                local.write_bytes(b'partial-data')
                Path(str(local) + '.aria2').write_bytes(b'control')
                download.write_json(Path(str(local) + '.ranges.json'),
                                    dict(size=20, sha256=self.items['First']['sha256'], ranges=[[0, 12]]))
            return code
        original_manifest = pipeline.failure_manifests
        original_rename = Path.rename
        def manifest(root, event):
            original_manifest(root, event)
            raise KeyboardInterrupt
        def rename(path, destination):
            if str(path).endswith('/First/video.mp4.aria2'):
                raise KeyboardInterrupt
            return original_rename(path, destination)
        interruption = patch.object(Path, 'rename', rename) if split_move else patch.object(pipeline, 'failure_manifests', side_effect=manifest)
        with patch.object(download, 'list_course', side_effect=lambda source, course, *args: [self.items[course]]), \
                patch('pipeline.transfer_process', side_effect=transfer), interruption:
            with self.assertRaises(KeyboardInterrupt):
                download.download(self.data, ['First'], self.root, retries=0)
        def ranges(root, event, item, *args, **kwargs):
            self.assertEqual(recovery.saved_ranges(recovery.body_path(root, event), 20, item['sha256']), [[0, 12]])
        with patch.object(recovery, 'retry_ranges', side_effect=ranges):
            self.assertEqual(self.run_download(), 1)
        self.assertEqual((self.root / 'First/video (partially available).mp4').read_bytes(), b'partial-data')

    def test_restart_repairs_manifest_before_move(self):
        self.interrupted_archive(False)

    def test_restart_repairs_body_before_control_move(self):
        self.interrupted_archive(True)

    def interrupted_recovery_copy(self, changed=False):
        state = self.root / '.download'
        state.mkdir()
        item = self.items['First']
        event = dict(item, course='First', attempt=1, status='partial', detail='Partial body',
                     time='2026-01-01', corrupted_path='corrupted/old/First/video.mp4')
        body = self.root / event['corrupted_path']
        body.parent.mkdir(parents=True)
        body.write_bytes(self.content)
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=item['size'], sha256=item['sha256'], ranges=[[0, item['size']]]))
        download.write_json(self.root / 'corrupted/old/failed.json', [event])
        download.write_json(state / 'course-history.json', {'First': dict(
            source=self.data['source'], state='recovering', verified=0, failed=0)})
        download.write_json(state / 'transfer-state.json', {item['path']: dict(
            source=self.data['source'], size=item['size'], sha256=item['sha256'],
            course='First', attempt=1, phase='range', event=event)})
        with patch.object(download, 'record', side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                recovery.finish_recovery(self.root, event, item, 0, background=True)
        local = self.root / 'First/video.mp4'
        if changed:
            local.write_bytes(b'changed')
        with patch.object(download, 'list_course', return_value=[item]), \
                patch.object(download.OPENER, 'open', side_effect=AssertionError('Unexpected HTTP')), \
                patch.object(pipeline, 'transfer_process', side_effect=AssertionError('Unexpected transfer')):
            self.assertEqual(download.download(self.data, ['First'], self.root, retries=0), int(changed))
        self.assertEqual(local.read_bytes(), b'changed' if changed else self.content)
        self.assertEqual(body.read_bytes(), self.content)
        self.assertEqual(json.loads((state / 'summary.json').read_text())['complete'], not changed)

    def test_restart_reuses_copy_created_before_recovery_proof(self):
        self.interrupted_recovery_copy()

    def test_restart_preserves_changed_copy_created_before_recovery_proof(self):
        self.interrupted_recovery_copy(changed=True)

    def test_sparse_resume_requires_space_for_unreceived_bytes(self):
        item = self.items['First']
        item['size'] = 5 * 1024 ** 3
        local = self.root / 'First/video.mp4'
        local.parent.mkdir()
        with local.open('wb') as stream:
            stream.truncate(item['size'])
        Path(str(local) + '.aria2').write_bytes(b'control')
        state = self.root / '.download'
        state.mkdir()
        download.write_json(state / '01-manifest.json', [item])
        usage = namedtuple('Usage', 'total used free')(10 * 1024 ** 3, 8 * 1024 ** 3, 2 * 1024 ** 3)
        with patch.object(download.shutil, 'disk_usage', return_value=usage):
            with self.assertRaisesRegex(OSError, 'Insufficient free space'):
                self.run_download(tuning=dict(free_space_mib=0))
        self.assertEqual(self.calls, [])

    def test_local_range_failure_remains_pending_for_restart(self):
        self.local_range_failure(RuntimeError('Insufficient disk space'))

    def test_range_enospc_remains_pending_for_restart(self):
        self.local_range_failure(OSError(28, 'Insufficient disk space'))

    def completed_range(self):
        from settings import resolve
        state = self.root / '.download'
        state.mkdir()
        download.write_json(state / 'source.json', {'source': self.data['source']})
        item = self.items['First']
        event = dict(item, course='First', attempt=1, time='2026-01-01',
                     corrupted_path='corrupted/old/First/video.mp4')
        body = self.root / event['corrupted_path']
        body.parent.mkdir(parents=True)
        body.write_bytes(self.content)
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=item['size'], sha256=item['sha256'], ranges=[[0, item['size']]]))
        background = pipeline.Background(self.root, self.data['source'], 0, resolve())
        self.addCleanup(background.close)
        background.submit('range', item, 'First', 1, event)
        path, future = next(iter(background.futures.items()))
        future.result(timeout=3)
        return background, path, event

    def test_range_done_and_finished_ledger_restore_removed_copy_without_http(self):
        background, path, event = self.completed_range()
        local = self.root / 'First/video.mp4'
        background.close()
        for state in ('done', 'applied'):
            with self.subTest(state=state):
                local.unlink(missing_ok=True)
                job = json.loads(path.read_text())
                job['state'] = state
                download.write_json(path, job)
                from settings import resolve
                restarted = pipeline.Background(self.root, self.data['source'], 0, resolve())
                try:
                    with patch.object(download.OPENER, 'open', side_effect=AssertionError('Confirmed bytes were requested again')):
                        restarted.submit('range', self.items['First'], 'First', 1, event)
                        next(iter(restarted.futures.values())).result(timeout=3)
                        restarted.poll()
                    self.assertTrue(local.exists())
                    self.assertEqual(local.read_bytes(), self.content)
                finally:
                    restarted.close()

    def test_range_full_result_changed_before_poll_is_revalidated(self):
        background, path, event = self.completed_range()
        local = self.root / 'First/video.mp4'
        local.write_bytes(b'changed')
        background.poll()
        if background.futures:
            next(iter(background.futures.values())).result(timeout=3)
            background.poll()
        result = json.loads(path.read_text())['result']['event']
        self.assertFalse(result.get('range_recovered_complete'))
        self.assertEqual(local.read_bytes(), b'changed')
        self.assertTrue(json.loads((self.root / 'corrupted/failed.json').read_text()))

    def test_range_submit_prefers_better_archive_to_done_task_and_finished_ledger(self):
        from settings import resolve
        background, path, event = self.completed_range()
        background.close()
        (self.root / 'First/video.mp4').unlink()
        old = self.root / event['corrupted_path']
        old.write_bytes(self.content[:2])
        download.write_json(Path(str(old) + '.ranges.json'),
                            dict(size=7, sha256=event['sha256'], ranges=[[0, 2]]))
        better = dict(event, corrupted_path='corrupted/new/First/video.mp4')
        body = self.root / better['corrupted_path']
        body.parent.mkdir(parents=True)
        body.write_bytes(self.content[:5])
        download.write_json(Path(str(body) + '.ranges.json'),
                            dict(size=7, sha256=event['sha256'], ranges=[[0, 5]]))
        old_job = json.loads(path.read_text())
        for state in ('done', 'applied'):
            with self.subTest(state=state):
                old_job['state'] = state
                download.write_json(path, old_job)
                restarted = pipeline.Background(self.root, self.data['source'], 0, resolve())
                try:
                    with patch.object(recovery, 'finish_recovery') as recover:
                        restarted.submit('range', self.items['First'], 'First', 1, dict(better))
                        next(iter(restarted.futures.values())).result(timeout=3)
                        restarted.poll()
                    self.assertEqual(recover.call_args.args[1]['corrupted_path'], better['corrupted_path'])
                finally:
                    restarted.close()

    def local_range_failure(self, error):
        self.items['First']['size'] = 20
        with patch.object(recovery, 'retry_ranges', side_effect=error):
            with self.assertRaisesRegex(type(error), 'Insufficient disk space'):
                self.run_download()
        state = self.root / '.download'
        ledger = FileState(state, 'recovery-state')
        self.assertEqual(ledger.get('/First/video.mp4')['state'], 'running')
        with patch.object(recovery, 'retry_ranges', return_value=None):
            self.assertEqual(self.run_download(), 1)
        ledger = FileState(state, 'recovery-state')
        self.assertEqual(ledger.get('/First/video.mp4')['state'], 'finished')


if __name__ == '__main__':
    unittest.main()
