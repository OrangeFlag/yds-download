"""Shared Range budgets, archive handoff and durable results."""
from concurrent.futures import ThreadPoolExecutor
from file_state import FileState
import fcntl
import hashlib
import io
import json
from pathlib import Path
import tempfile
from threading import Event, Lock
import time
import unittest
from unittest.mock import patch

import download as d
import range_queue as q
import semester_queue
from settings import resolve


class DiscoveryProfileChecks(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        self.root=Path(tmp.name)
        self.metrics=q.Metrics(self.root/'metrics.jsonl','test',start=False)
        self.addCleanup(self.metrics.close)
        self.profile=q.DiscoveryProfile(self.metrics)

    def test_event_profile_counts_only_consumed_tail_without_an_extra_scan(self):
        path=self.root/'events.jsonl'
        row=dict(path='/video.mp4',status='invalid')
        d.append_events(path,[row,dict(row,status='valid')])
        index=d.EventIndex(path)
        with patch.object(d,'scan_events',wraps=d.scan_events) as scan:
            self.assertEqual(self.profile.events(index)['/video.mp4']['status'],'valid')
            original_size=path.stat().st_size
            self.profile.events(index)
            d.append_events(path,[row]);self.profile.events(index)
            self.assertEqual(scan.call_count,3)
        counters=self.metrics.counters
        self.assertEqual(counters['range_discovery_events_records'],3)
        self.assertEqual(counters['range_discovery_events_bytes'],path.stat().st_size)
        self.assertGreater(path.stat().st_size,original_size)
        self.assertEqual(self.metrics.gauges['range_discovery_events_journal_bytes'],path.stat().st_size)
        old=path.with_suffix('.old');path.rename(old)
        d.append_events(path,[dict(row,status='missing')])
        self.assertEqual(self.profile.events(index)['/video.mp4']['status'],'missing')
        self.assertEqual(counters['range_discovery_events_records'],4)
        self.assertEqual(counters['range_discovery_events_bytes'],old.stat().st_size+path.stat().st_size)

    def test_observing_unfinished_event_does_not_repair_a_read_only_journal(self):
        path=self.root/'events.jsonl';body=b'{"path":"/video.mp4","status":"valid"}'
        path.write_bytes(body);index=d.EventIndex(path,repair=False)
        self.assertEqual(self.profile.events(index),{})
        self.assertEqual(path.read_bytes(),body)
        self.assertEqual(self.metrics.counters['range_discovery_events_records'],0)
        with path.open('ab') as f:f.write(b'\n')
        self.assertEqual(self.profile.events(index)['/video.mp4']['status'],'valid')
        self.assertEqual(self.metrics.counters['range_discovery_events_records'],1)

    def test_task_profile_reuses_the_existing_batched_scan(self):
        directory=self.root/'tasks';directory.mkdir()
        for i in range(5):
            d.write_json(directory/f'{i}.json',dict(kind='range',state='pending',course='Course',source='test',attempt=0,
                item=dict(path=f'/Course/{i}.mp4',size=4,sha256='a'*64)))
        cache=q.JsonCache(self.profile)
        self.assertEqual(len(cache.tasks(directory,limit=2)),2)
        self.assertEqual(len(cache.tasks(directory,limit=2)),4)
        self.assertEqual(len(cache.tasks(directory,limit=2)),5)
        self.assertTrue(cache.tasks_complete(directory))
        cache.tasks(directory,limit=2)
        self.assertEqual(self.metrics.counters['range_discovery_task_enumeration_calls'],1)
        self.assertEqual(self.metrics.counters['range_discovery_task_enumeration_entries'],5)
        self.assertEqual(self.metrics.counters['range_discovery_tasks_json_reads'],5)

    def test_manifest_profile_does_not_reload_unchanged_json(self):
        d.write_json(self.root/'courses.json',['Course'])
        item=dict(path='/Course/video.mp4',size=4,sha256='a'*64)
        d.write_json(self.root/'01-manifest.json',[item])
        cache=q.JsonCache(self.profile)
        with patch.object(Path,'read_text',autospec=True,side_effect=Path.read_text) as read:
            self.assertEqual(self.profile.manifests(cache,self.root),{('Course',item['path']):(4,'a'*64)})
            self.profile.manifests(cache,self.root)
            self.assertEqual(read.call_count,2)
        self.assertEqual(self.metrics.counters['range_discovery_manifests_json_reads'],2)

    def test_observation_failure_preserves_errors_and_nested_phase(self):
        with patch('builtins.print',side_effect=OSError('log unavailable')):
            with self.assertRaisesRegex(InterruptedError,'original stop'):
                with self.profile.stage('tasks',self.root):
                    with self.profile.stage('task_enumeration',self.root):
                        self.assertEqual(self.metrics.gauges['range_discovery_stage'],'task_enumeration')
                    self.assertEqual(self.metrics.gauges['range_discovery_stage'],'tasks')
                    raise InterruptedError('original stop')
        self.assertEqual(self.metrics.gauges['range_discovery_stage'],'idle')
        self.assertEqual(self.metrics.counters['range_discovery_tasks_failures'],1)

    def test_manifest_profile_keeps_completed_reads_when_a_later_file_is_invalid(self):
        d.write_json(self.root/'courses.json',['Course','Other'])
        d.write_json(self.root/'01-manifest.json',[dict(path='/Course/video.mp4',size=4,sha256='a'*64)])
        (self.root/'02-manifest.json').write_text('not JSON')
        with self.assertRaises(json.JSONDecodeError):
            self.profile.manifests(q.JsonCache(self.profile),self.root)
        self.assertEqual(self.metrics.counters['range_discovery_manifests_json_reads'],2)
        self.assertEqual(self.metrics.counters['range_discovery_manifests_failures'],1)

    def test_startup_milestones_are_reported_once(self):
        with patch('sys.stdout',new_callable=io.StringIO) as output:
            self.profile.milestone('first_range_submit')
            first=self.metrics.gauges['range_startup_first_range_submit_seconds']
            self.profile.milestone('first_range_submit')
            self.profile.milestone('first_pass')
        self.assertEqual(self.metrics.gauges['range_startup_first_range_submit_seconds'],first)
        self.assertEqual(output.getvalue().count('RANGE STARTUP:'),2)
        self.assertFalse(self.profile.first_pass)


class SharedRangeChecks(unittest.TestCase):
    def test_daily_recheck_cooldown_and_verified_exclusions(self):
        from datetime import datetime, timezone
        stamp = datetime.now(timezone.utc)
        job = dict(kind='range', state='applied', result={'event': {'status': 'missing'}})
        self.assertTrue(q.recheck_due(job, 86400, stamp.timestamp()))
        self.assertFalse(q.recheck_due(job, 0, stamp.timestamp()))
        job['yandex_checked_at'] = stamp.isoformat()
        self.assertFalse(q.recheck_due(job, 86400, stamp.timestamp()+86399))
        self.assertTrue(q.recheck_due(job, 86400, stamp.timestamp()+86401))
        job['result']['event']['range_recovered_complete'] = True
        self.assertFalse(q.recheck_due(job, 86400, stamp.timestamp()+86401))
        job['result']['event'] = {'status': 'valid'}
        self.assertFalse(q.recheck_due(job, 86400, stamp.timestamp()+86401))
        job['result']['event'] = {'status': 'invalid'}
        job['superseded_by_manifest'] = True
        self.assertFalse(q.recheck_due(job, 86400, stamp.timestamp()+86401))

    def test_daily_recheck_does_not_rewrite_tasks_superseded_by_verified_copies(self):
        task = dict(kind='range', state='applied',
                    result={'event': {'status': 'missing'}})
        for reason in ('superseded_by_manifest', 'superseded_by_nextcloud',
                       'superseded_by_local_copy'):
            with self.subTest(reason=reason):
                self.assertFalse(q.recheck_due(dict(task, **{reason: True}), 86400))
                with tempfile.TemporaryDirectory() as folder:
                    directory = Path(folder)
                    d.write_json(directory/'task.json', dict(task, course='Course', source='test', attempt=3,
                        item=dict(path='/Course/video.mp4', size=7, sha256='a'*64), **{reason: True}))
                    _, cached = q.JsonCache().tasks(directory)[0]
                    self.assertFalse(q.recheck_due(cached, 86400))

    def test_recheck_timestamp_survives_compact_cache_restart(self):
        from datetime import datetime, timezone
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder);stamp=datetime.now(timezone.utc)
            d.write_json(directory/'task.json',dict(kind='range',state='applied',course='Course',source='test',attempt=3,
                item=dict(path='/Course/video.mp4',size=7,sha256='a'*64),
                yandex_recheck=True,yandex_checked_at=stamp.isoformat(),result={'event':{'status':'invalid'}}))
            _,job=q.JsonCache().tasks(directory)[0]
            self.assertFalse(q.recheck_due(job,86400,stamp.timestamp()+60))
            self.assertTrue(q.recheck_due(job,86400,stamp.timestamp()+86401))

    def test_legacy_tasks_are_seeded_once_using_latest_saved_body(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);state=root/'.download';directory=state/'tasks';directory.mkdir(parents=True)
            item=dict(path='/Course/video.mp4',size=7,sha256=hashlib.sha256(b'lecture').hexdigest())
            event=dict(item,course='Course',attempt=3,status='invalid',corrupted_path='corrupted/old/Course/video.mp4')
            (root/'corrupted').mkdir()
            d.write_json(root/'corrupted/failed.json',[event])
            d.write_json(state/'courses.json',['Course']);d.write_json(state/'01-manifest.json',[item])
            latest=dict(event,recovered_path='Course/video (partially available).mp4')
            d.write_json(state/'recovery-state.json',{item['path']:dict(state='finished',event=latest)})
            bg=SimpleNamespace(root=root,state=state,directory=directory,source='https://disk.yandex.ru/d/test',retries=2)
            q.seed_legacy_tasks(bg);q.seed_legacy_tasks(bg)
            paths=list(directory.glob('*.json'));self.assertEqual(len(paths),1)
            job=json.loads(paths[0].read_text());self.assertTrue(q.recheck_due(job,86400))
            self.assertEqual(job['event']['recovered_path'],latest['recovered_path'])
            self.assertEqual(job['item']['sha256'],item['sha256'])

    def test_legacy_seed_resumes_after_interruption(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);state=root/'.download';directory=state/'tasks';directory.mkdir(parents=True);(root/'corrupted').mkdir()
            entries=[dict(path=f'/Course/{i}.mp4',size=7,sha256=hashlib.sha256(b'lecture').hexdigest()) for i in range(3)]
            events=[dict(item,course='Course',attempt=3,status='invalid',corrupted_path='corrupted/old'+item['path']) for item in entries]
            d.write_json(root/'corrupted/failed.json',events);d.write_json(state/'courses.json',['Course']);d.write_json(state/'01-manifest.json',entries)
            bg=SimpleNamespace(root=root,state=state,directory=directory,source='https://disk.yandex.ru/d/test',retries=2)
            original=d.write_json;calls=[]
            def interrupted(path,row):
                if path.parent==directory:
                    calls.append(path)
                    if len(calls)==2:raise InterruptedError('seed interrupted')
                return original(path,row)
            with patch.object(d,'write_json',side_effect=interrupted):
                with self.assertRaises(InterruptedError):q.seed_legacy_tasks(bg)
            self.assertEqual(len(list(directory.glob('*.json'))),1)
            source=bg.source;d.write_json(state/'source.json',dict(source=source));d.write_json(state/'catalog.json',dict(source=source))
            d.write_json(state/'settings.json',dict(retries=2,tuning=resolve()));d.write_json(state/'summary.json',dict(downloads_complete=True,pending_courses=[]))
            d.write_json(state/'course-history.json',{'Course':dict(state='finished',failed=3)})
            queue=state/'queue.json';d.write_json(queue,dict(archives=[],wait_for={'destination':str(root)}));stopping=Event()
            with patch('recovery.finish_recovery',side_effect=lambda *a,**kw:stopping.set()):
                q.run([queue],files=1,stopping=stopping,recheck_interval=86400,recheck_files=1)
            self.assertEqual(len(list(directory.glob('*.json'))),3)
            self.assertTrue(json.loads((state/'legacy-recheck-seeded.json').read_text())['complete'])

    def test_never_checked_file_precedes_older_hash_sorted_retry(self):
        from datetime import datetime,timezone,timedelta
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);state=root/'.download';(state/'tasks').mkdir(parents=True);source='https://disk.yandex.ru/d/test';stopping=Event()
            d.write_json(state/'source.json',dict(source=source));d.write_json(state/'catalog.json',dict(source=source))
            d.write_json(state/'settings.json',dict(retries=2,tuning=resolve()));d.write_json(state/'courses.json',['Course'])
            d.write_json(state/'summary.json',dict(downloads_complete=True,pending_courses=[]));d.write_json(state/'course-history.json',{'Course':dict(state='finished',failed=2)})
            entries=[dict(path=f'/Course/{i}.mp4',size=7,sha256=hashlib.sha256(b'lecture').hexdigest(),file='https://example.invalid/video') for i in range(2)]
            keys=[hashlib.sha256(json.dumps([source,item['path'],item['size'],item['sha256'],'range']).encode()).hexdigest() for item in entries]
            old_index=keys.index(min(keys));never=entries[1-old_index]['path']
            for i,item in enumerate(entries):
                event=dict(item,course='Course',attempt=3,status='missing',corrupted_path='corrupted/old'+item['path'])
                job=dict(kind='range',state='applied',source=source,item=item,course='Course',attempt=3,event=event,result={'event':event})
                if i==old_index:job['yandex_checked_at']=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
                d.write_json(state/'tasks'/(keys[i]+'.json'),job)
            d.write_json(state/'01-manifest.json',entries);queue=state/'queue.json';d.write_json(queue,dict(archives=[dict(destination=str(root),catalog=str(state/'catalog.json'))]))
            selected=[]
            def recover(root,event,item,*args,**kwargs):selected.append(item['path']);stopping.set()
            with patch('recovery.finish_recovery',side_effect=recover):q.run([queue],files=1,stopping=stopping,recheck_interval=86400,recheck_files=1)
            self.assertEqual(selected,[never])

    def test_recheck_slots_are_added_and_split_between_personal_and_courses(self):
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder); queues = []; stopping = Event(); gate = Event(); mutex = Lock(); started = []
            content = b'lecture'; digest = hashlib.sha256(content).hexdigest()
            for group in ('personal', 'courses'):
                root = base/group; state = root/'.download'; (state/'tasks').mkdir(parents=True)
                source = 'https://disk.yandex.ru/d/'+group
                d.write_json(state/'source.json', dict(source=source))
                d.write_json(state/'catalog.json', dict(source=source))
                d.write_json(state/'settings.json', dict(retries=2, tuning=resolve()))
                d.write_json(state/'courses.json', ['Course'])
                d.write_json(state/'summary.json', dict(downloads_complete=True, pending_courses=['Course']))
                d.write_json(state/'course-history.json', {'Course': dict(state='recovering')})
                entries = []
                for index in range(12):
                    checking = index>=8
                    item = dict(path=f'/Course/{index}.mp4', size=len(content), sha256=digest, file='https://example.invalid/video')
                    event = dict(item, course='Course', attempt=3, status='missing', corrupted_path='corrupted/old'+item['path'], checking=checking)
                    key = hashlib.sha256(json.dumps([source, item['path'], item['size'], item['sha256'], 'range']).encode()).hexdigest()
                    job = dict(kind='range', state='applied' if checking else 'pending', source=source,
                               item=item, course='Course', attempt=3, event=event, result={'event': event})
                    d.write_json(state/'tasks'/(key+'.json'), job); entries.append(item)
                d.write_json(state/'01-manifest.json', entries)
                queue = state/'queue.json';d.write_json(queue,dict(archives=[dict(destination=str(root),catalog=str(state/'catalog.json'))]));queues.append(queue)
            def recover(root, event, item, *args, **kwargs):
                with mutex:
                    started.append((root.name, event['checking']))
                    if len(started)==24:gate.set()
                self.assertTrue(gate.wait(5), 'Extra rechecks blocked normal slots')
                local=d.target(root,item,'Course');local.parent.mkdir(exist_ok=True);local.write_bytes(content)
                proof=d.record(root/'.download',item,'Course',3,'valid','verified')
                event.update(proof,range_recovered_complete=True,recovered_stat=q.Background.stat(local))
                stopping.set()
            with patch('recovery.finish_recovery',side_effect=recover):
                q.run(queues,files=16,secondary_slots=8,stopping=stopping,recheck_interval=86400,recheck_files=8)
            from collections import Counter
            self.assertEqual(Counter(started),{('personal',False):8,('courses',False):8,('personal',True):4,('courses',True):4})
            for queue in queues:
                for path in (queue.parent/'tasks').glob('*.json'):
                    row=json.loads(path.read_text());self.assertIn('yandex_checked_at',row)

    def test_budget_bounds_concurrency_and_request_start_rate(self):
        budget = q.RangeBudget(2, 50)
        mutex = Lock()
        active = peak = 0
        starts = []
        def work(_):
            nonlocal active, peak
            with budget.request(Event()):
                with mutex:
                    active += 1
                    peak = max(peak, active)
                    starts.append(time.monotonic())
                time.sleep(.045)
                with mutex:
                    active -= 1
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(work, range(8)))
        self.assertEqual(peak, 2)
        self.assertTrue(all(b-a >= .018 for a,b in zip(starts, starts[1:])))

    def test_cache_reads_only_changed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.json'
            d.write_json(path, {'value': 1})
            cache = q.JsonCache()
            with patch.object(Path, 'read_text', wraps=None, return_value=path.read_text()) as read:
                self.assertEqual(cache.read(path), {'value': 1})
                self.assertEqual(cache.read(path), {'value': 1})
                self.assertEqual(read.call_count, 1)
            d.write_json(path, {'value': 2})
            self.assertEqual(cache.read(path), {'value': 2})

    def test_task_cache_retains_only_compact_summaries_and_reloads_changed_tasks(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder) / 'tasks'
            directory.mkdir()
            for index in range(40):
                d.write_json(directory / f'{index}.json', dict(
                    kind='range', state='applied' if index else 'pending',
                    course='Course', source='test', attempt=1,
                    item=dict(path=f'/Course/{index}.mp4', size=4, sha256='a'*64),
                    event=dict(path=f'/Course/{index}.mp4', range_attempts=['x'*4096]*10),
                    result=dict(event=dict(range_recovered_complete=False,
                                           range_attempts=['y'*4096]*10))))
            cache = q.JsonCache()
            tasks = cache.tasks(directory)
            self.assertEqual(len(tasks), 40)
            self.assertTrue(all('event' not in job for _, job in tasks))
            self.assertTrue(all('range_attempts' not in str(job) for _, job in tasks))
            self.assertLess(len(str(tasks)), 30000)
            self.assertFalse(any(path.parent == directory for path in cache.entries))
            with patch.object(Path, 'read_text', wraps=None, side_effect=AssertionError('Unchanged tasks reread')):
                self.assertEqual(len(cache.tasks(directory)), 40)
            path = directory / '0.json'
            changed = json.loads(path.read_text())
            changed['state'] = 'done'
            d.write_json(path, changed)
            self.assertEqual(next(job for name, job in cache.tasks(directory) if name == path)['state'], 'done')

    def test_task_checkpoint_restores_summaries_without_reenumerating_or_rereading_json(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)/'.download/tasks';directory.mkdir(parents=True)
            job=dict(kind='range',state='applied',course='Course',source='test',
                     attempt=1,item=dict(path='/Course/movie.mp4',size=7,sha256='a'*64),
                     event=dict(range_attempts=['expensive history']*100))
            for i in range(40):
                d.write_json(directory/f'{i:02}.json',dict(job,item=dict(job['item'],path=f'/Course/{i}.mp4')))
            first=q.JsonCache(checkpoint=True)
            self.assertEqual(len(first.tasks(directory)),40)
            checkpoint=directory.parent/'tasks.index.json'
            self.assertTrue(checkpoint.exists())
            self.assertNotIn('expensive history',checkpoint.read_text())
            warm=q.JsonCache(checkpoint=True)
            with patch.object(Path,'glob',side_effect=AssertionError('Tasks were re-enumerated')), \\
                 patch.object(Path,'read_text',wraps=None,side_effect=lambda *args,**kw:
                     (_ for _ in ()).throw(AssertionError('Task JSON reread'))):
                tasks=warm.tasks(directory)
                self.assertEqual(len(tasks),40)
                self.assertTrue(warm.tasks_complete(directory))
            self.assertEqual(warm.task_reads,0)

    def test_task_checkpoint_invalidates_when_tasks_are_created_changed_or_removed(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)/'.download/tasks';directory.mkdir(parents=True)
            job=dict(kind='range',state='applied',course='Course',source='test',
                     attempt=1,item=dict(path='/Course/movie.mp4',size=7,sha256='a'*64))
            for i in range(8):d.write_json(directory/f'{i}.json',job)
            q.JsonCache(checkpoint=True).tasks(directory)
            d.write_json(directory/'0.json',dict(job,yandex_checked_at='2026-10-20T00:00:00+00:00'))
            d.write_json(directory/'new.json',dict(job,state='pending'))
            (directory/'1.json').unlink()
            cache=q.JsonCache(checkpoint=True)
            result=dict(cache.tasks(directory))
            self.assertEqual(len(result),8)
            self.assertEqual(result[directory/'0.json']['yandex_checked_at'],'2026-10-20T00:00:00+00:00')
            self.assertEqual(result[directory/'new.json']['state'],'pending')
            self.assertNotIn(directory/'1.json',result)

    def test_corrupt_task_checkpoint_falls_back_to_task_json(self):
        with tempfile.TemporaryDirectory() as folder:
            directory=Path(folder)/'.download/tasks';directory.mkdir(parents=True)
            job=dict(kind='range',state='applied',course='Course',source='test',
                     attempt=1,item=dict(path='/Course/a.mp4',size=7,sha256='a'*64))
            d.write_json(directory/'a.json',job)
            q.JsonCache(checkpoint=True).tasks(directory)
            checkpoint=directory.parent/'tasks.index.json'
            saved=json.loads(checkpoint.read_text())
            saved['payload']['tasks'][0][2]['state']='pending'
            checkpoint.write_text(json.dumps(saved))
            cache=q.JsonCache(checkpoint=True)
            self.assertEqual(dict(cache.tasks(directory))[directory/'a.json']['state'],'applied')
            self.assertEqual(cache.task_reads,1)

    def test_task_discovery_is_bounded_and_includes_changes_during_scan(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            job = dict(kind='range', state='pending', course='Course', source='test', attempt=1,
                       item=dict(path='/Course/video.mp4', size=4, sha256='a'*64))
            for index in range(5):
                d.write_json(directory / f'{index}.json', job)
            cache = q.JsonCache()
            original = Path.read_text
            reads = []
            def read(path, *args, **kwargs):
                reads.append(path)
                return original(path, *args, **kwargs)
            with patch.object(Path, 'read_text', new=read):
                self.assertEqual(len(cache.tasks(directory, limit=2)), 2)
                self.assertEqual(len(reads), 2)
                self.assertFalse(cache.tasks_complete(directory))
                d.write_json(directory / '0.json', dict(job, state='done'))
                d.write_json(directory / 'new.json', job)
                (directory / '1.json').unlink()
                for _ in range(5):
                    before = len(reads)
                    tasks = cache.tasks(directory, limit=2)
                    self.assertLessEqual(len(reads)-before, 2)
                    if cache.tasks_complete(directory):
                        break
            self.assertTrue(cache.tasks_complete(directory))
            self.assertEqual({path.name for path, _ in tasks}, {'0.json', '2.json', '3.json', '4.json', 'new.json'})
            self.assertEqual(dict(tasks)[directory / '0.json']['state'], 'done')

    def test_task_discovery_reaches_tail_during_continuous_prefix_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            job = dict(kind='range', state='pending', course='Course', source='test', attempt=1,
                       item=dict(path='/Course/video.mp4', size=4, sha256='a'*64))
            for index in range(40):
                d.write_json(directory / f'{index:02}.json', job)
            cache = q.JsonCache()
            original = Path.read_text
            reads = []
            def read(path, *args, **kwargs):
                reads.append(path)
                return original(path, *args, **kwargs)
            with patch.object(Path, 'read_text', new=read):
                for attempt in range(6):
                    before = len(reads)
                    cache.tasks(directory, limit=32)
                    self.assertLessEqual(len(reads)-before, 32)
                    for index in range(32):
                        d.write_json(directory / f'{index:02}.json', dict(job, attempt=attempt+2))
                self.assertEqual(len(set(reads)), 40, 'Repeated prefix writes starved unseen tasks')
                for _ in range(4):
                    tasks = cache.tasks(directory, limit=32)
                    if cache.tasks_complete(directory):
                        break
            self.assertTrue(cache.tasks_complete(directory))
            self.assertEqual(len(tasks), 40)
            self.assertEqual(dict(tasks)[directory / '00.json']['attempt'], 7)

    def _finished_archive_with_cached_range(self, root):
        state = root / '.download'
        (state / 'tasks').mkdir(parents=True)
        source = 'https://disk.yandex.ru/d/test'
        local = root / 'external.mp4'
        local.write_bytes(b'lecture')
        item = dict(path='/Course/video.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(),
                    file='https://example.invalid/video')
        catalog, queue = state / 'catalog.json', state / 'queue.json'
        files = {'source.json': dict(source=source), 'catalog.json': dict(source=source),
                 'settings.json': dict(retries=2, tuning=resolve()), 'courses.json': ['Course'],
                 'summary.json': dict(downloads_complete=True, pending_courses=[], complete=False, failed=1),
                 'course-history.json': {'Course': dict(state='finished', verified=0, failed=1)},
                 '01-manifest.json': [{key: value for key, value in item.items() if key != 'file'}],
                 'local-copies.json': [dict(item, source=source, status='valid', local=str(local), mtime_ns=local.stat().st_mtime_ns, ctime_ns=local.stat().st_ctime_ns)],
                 'queue.json': dict(report_repo=str(root / 'reports'), mark='Test',
                                    archives=[dict(state='finished', destination=str(root), catalog=str(catalog))])}
        for name, value in files.items():
            d.write_json(state / name, value)
        d.write_json(state / 'tasks/00.json', dict(kind='range', source=source, item=item, event={},
                                                course='Course', attempt=3, state='pending'))
        for index in range(40):
            d.write_json(state / 'tasks' / f'zz-{index:02}.json', dict(kind='sha'))
        return state, queue

    def test_deferred_finished_course_report_survives_stop_and_restart(self):
        with tempfile.TemporaryDirectory() as folder:
            state, queue = self._finished_archive_with_cached_range(Path(folder))
            stopping = Event()
            with patch.object(stopping, 'wait', side_effect=lambda _: stopping.set()), patch('report.update_report') as report:
                q.run([queue], stopping=stopping)
                self.assertEqual(json.loads((state / 'tasks/00.json').read_text())['state'], 'applied')
                self.assertEqual(json.loads((state / 'summary.json').read_text())['failed'], 1)
                report.assert_not_called()
                stopping.clear()
                passes = 0
                def wait(_):
                    nonlocal passes
                    passes += 1
                    if passes >= 4:
                        stopping.set()
                with patch.object(stopping, 'wait', side_effect=wait):
                    q.run([queue], stopping=stopping)
                report.assert_called_once()
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            self.assertEqual(json.loads((state / 'course-history.json').read_text())['Course']['verified'], 1)
            self.assertFalse((state / 'range-dirty-courses.json').exists())

    def test_disabling_partially_discovered_archive_releases_idle_lock(self):
        with tempfile.TemporaryDirectory() as folder:
            state, queue = self._finished_archive_with_cached_range(Path(folder))
            stopping = Event()
            passes = 0
            def wait(_):
                nonlocal passes
                passes += 1
                with (state / 'download.lock').open('a') as lock:
                    if passes == 1:
                        with self.assertRaises(BlockingIOError):
                            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        config = json.loads(queue.read_text())
                        config['archives'][0]['enabled'] = False
                        d.write_json(queue, config)
                    else:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        stopping.set()
            with patch.object(stopping, 'wait', side_effect=wait), patch('report.update_report') as report:
                q.run([queue], stopping=stopping)
            report.assert_not_called()
            self.assertEqual(json.loads((state / 'range-dirty-courses.json').read_text()), ['Course'])

    def test_finish_courses_defers_until_all_tasks_are_discovered(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            directory = root / '.download/tasks'
            directory.mkdir(parents=True)
            for index in range(40):
                d.write_json(directory / f'{index:02d}.json', dict(
                    kind='range', state='applied', course='Course', source='test', attempt=1,
                    item=dict(path=f'/Course/{index}.mp4', size=4, sha256='a'*64)))
            cache = q.JsonCache()
            cache.tasks(directory, limit=1)
            bg = SimpleNamespace(root=root, state=directory.parent, source='test',
                                 directory=directory, task_cache=cache)
            self.assertFalse(q.finish_courses(bg, {}))
            self.assertFalse((bg.state / 'course-history.json').exists())
            self.assertFalse((bg.state / 'summary.json').exists())

    def test_event_checkpoint_restores_compact_index_and_reads_only_new_tail(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'events.jsonl'
            rows = [dict(path=f'/Course/{i}.mp4', status='invalid', size=i,
                         sha256='a'*64, range_attempts=['x'*4096]*10) for i in range(30)]
            d.append_events(path,rows)
            first = d.EventIndex(path,checkpoint=True)
            self.assertEqual(len(first.refresh()),30)
            saved_offset=first.offset
            checkpoint=path.with_name(path.name+'.index.json')
            self.assertTrue(checkpoint.exists())
            d.append_events(path,[dict(rows[0],status='valid')])
            with patch.object(d,'scan_events',wraps=d.scan_events) as scanner:
                restored = d.EventIndex(path,checkpoint=True)
                events=restored.refresh()
            self.assertEqual(scanner.call_args.kwargs['start'],saved_offset)
            self.assertEqual(events[rows[0]['path']]['status'],'valid')
            self.assertNotIn('range_attempts',str(events))
            self.assertNotIn('range_attempts',checkpoint.read_text())
            self.assertEqual(len(events),30)

    def test_event_checkpoint_corruption_falls_back_to_full_history(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            row=dict(path='/one',status='valid')
            d.append_events(path,[row])
            d.EventIndex(path,checkpoint=True).refresh()
            checkpoint=path.with_name(path.name+'.index.json')
            corrupted=json.loads(checkpoint.read_text())
            corrupted['payload']['events']['/one']['status']='invalid'
            checkpoint.write_text(json.dumps(corrupted))
            with patch.object(d,'scan_events',wraps=d.scan_events) as scanner:
                restored=d.EventIndex(path,checkpoint=True).refresh()
            self.assertEqual(scanner.call_args.kwargs['start'],0)
            self.assertEqual(restored['/one']['status'],'valid')

    def test_event_index_replays_when_device_changes_but_inode_matches(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            d.append_events(path,[dict(path='/one',status='valid')])
            index=d.EventIndex(path)
            index.refresh()
            self.assertEqual(index.scanned_events,1)
            original_stat=Path.stat
            def changed_device(p, *args, **kwargs):
                stat=original_stat(p,*args,**kwargs)
                if p == path:
                    return SimpleNamespace(st_ino=stat.st_ino,st_dev=stat.st_dev+1,
                                           st_size=stat.st_size,st_mtime_ns=stat.st_mtime_ns)
                return stat
            with patch.object(Path,'stat',changed_device):
                index.refresh()
            self.assertEqual(index.scanned_events,2,
                             'Device switch must not reuse proofs solely by inode')

    def test_event_checkpoint_invalidates_replaced_and_rewritten_journal(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            original=dict(path='/one',status='valid')
            d.append_events(path,[original])
            d.EventIndex(path,checkpoint=True).refresh()
            # Same-size in-place rewrite must not inherit previous verified results.
            path.write_text(json.dumps(dict(original,status='other'))+chr(10))
            self.assertEqual(d.EventIndex(path,checkpoint=True).refresh()['/one']['status'],'other')
            replacement=path.with_name('replacement.jsonl')
            d.append_events(replacement,[dict(original,status='lost')])
            replacement.replace(path)
            self.assertEqual(d.EventIndex(path,checkpoint=True).refresh()['/one']['status'],'lost')
            path.write_bytes(b'')
            d.append_events(path,[dict(original,status='new')])
            self.assertEqual(d.EventIndex(path,checkpoint=True).refresh()['/one']['status'],'new')

    def test_event_checkpoint_repairs_crash_torn_final_jsonl_row(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            row=dict(path='/one',status='valid')
            path.write_text(json.dumps(row)+chr(10)+'{"path":')
            initial=d.EventIndex(path,checkpoint=True)
            self.assertEqual(initial.refresh()['/one']['status'],'valid')
            self.assertEqual(path.read_text(),json.dumps(row)+chr(10))
            d.append_events(path,[dict(row,status='other')])
            self.assertEqual(d.EventIndex(path,checkpoint=True).refresh()['/one']['status'],'other')

    def test_event_checkpoint_keeps_appends_during_snapshot_publication(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            first=dict(path='/one',status='invalid')
            last=dict(path='/one',status='valid')
            d.append_events(path,[first])
            snapshot=path.with_name(path.name+'.index.json')
            original_write=d.write_json
            appended=[]
            def append_during_save(destination, data):
                if destination==snapshot and not appended:
                    d.append_events(path,[last])
                    appended.append(True)
                return original_write(destination,data)
            with patch.object(d,'write_json',side_effect=append_during_save):
                old=d.EventIndex(path,checkpoint=True).refresh()
            self.assertEqual(old['/one']['status'],'invalid')
            self.assertTrue(appended)
            restored=d.EventIndex(path,checkpoint=True)
            self.assertEqual(restored.refresh()['/one']['status'],'valid')
            self.assertEqual(restored.scanned_events,1,
                             'Only the concurrent append should be replayed')

    def test_event_checkpoint_failed_publication_preserves_history(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'events.jsonl'
            row=dict(path='/one',status='valid')
            d.append_events(path,[row])
            original=path.read_bytes()
            with patch.object(d,'write_json',side_effect=OSError('checkpoint write interrupted')):
                with self.assertRaises(OSError):
                    d.EventIndex(path,checkpoint=True).refresh()
            self.assertEqual(path.read_bytes(),original)
            self.assertEqual(d.EventIndex(path,checkpoint=True).refresh()['/one']['status'],'valid')

    def test_event_index_streams_latest_proofs_without_holding_range_history(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'events.jsonl'
            row = dict(path='/Course/video.mp4', status='invalid', source='test',
                       size=4, sha256='a'*64, range_attempts=['x'*4096]*50)
            d.append_events(path, [row])
            index = q.EventIndex(path)
            with patch.object(d, 'read_events', side_effect=AssertionError('Bulk JSONL read')), \
                    patch.object(Path, 'read_bytes', side_effect=AssertionError('Whole journal loaded')):
                self.assertEqual(index.refresh()['/Course/video.mp4']['status'], 'invalid')
            self.assertNotIn('range_attempts', str(index.events))
            before = index.offset
            d.append_events(path, [dict(row, status='valid', mtime_ns=10)])
            self.assertEqual(index.refresh()['/Course/video.mp4']['status'], 'valid')
            self.assertGreater(index.offset, before)
            self.assertEqual(index.refresh()['/Course/video.mp4']['mtime_ns'], 10)

    def test_finishing_course_uses_worker_event_index_not_full_jsonl_history(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            state = root / '.download'
            (state / 'tasks').mkdir(parents=True)
            local = root / 'video.mp4'
            local.write_bytes(b'data')
            item = dict(path='/Course/video.mp4', size=4, sha256=hashlib.sha256(b'data').hexdigest())
            row = dict(item, source='test', status='valid', local=str(local),
                       mtime_ns=local.stat().st_mtime_ns)
            d.append_events(state / 'events.jsonl', [row])
            d.write_json(state / 'courses.json', ['Course'])
            d.write_json(state / 'course-history.json', {'Course': dict(state='recovering')})
            d.write_json(state / '01-manifest.json', [item])
            d.write_json(state / 'settings.json', {})
            bg = SimpleNamespace(root=root, state=state, source='test',
                                 directory=state/'tasks', event_index=q.EventIndex(state/'events.jsonl'),
                                 task_cache=q.JsonCache())
            with patch.object(d, 'read_events', side_effect=AssertionError('Whole history reread')):
                q.finish_courses(bg, {})
            status = json.loads((state / 'course-history.json').read_text())['Course']
            self.assertEqual((status['verified'], status['failed']), (1, 0))

    def test_semester_queue_advances_without_waiting_for_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            queue = root / 'queue.json'
            jobs = [dict(title=str(i), catalog='catalog.json', destination=str(root / str(i))) for i in range(2)]
            d.write_json(queue, dict(archives=jobs))
            calls = []
            def transfer(command, **kwargs):
                self.assertIn('--defer-range', command)
                destination = Path(command[command.index('--destination') + 1])
                state = destination / '.download'
                state.mkdir(parents=True)
                d.write_json(state / 'summary.json', dict(downloads_complete=True, pending_courses=['Course']))
                calls.append(destination.name)
                if len(calls) == 2:
                    self.assertEqual(json.loads(queue.read_text())['archives'][0]['state'], 'recovering')
                    for job in jobs:
                        d.write_json(Path(job['destination']) / '.download/summary.json', dict(downloads_complete=True, pending_courses=[]))
                return 1
            with patch('sys.argv', ['semester_queue.py', str(queue), '--defer-range']), patch.object(semester_queue, 'transfer_process', side_effect=transfer), patch.object(semester_queue.time, 'sleep'):
                semester_queue.main()
            self.assertEqual(calls, ['0', '1'])
            self.assertTrue(all(job['state'] == 'finished' for job in json.loads(queue.read_text())['archives']))
            with patch('sys.argv', ['semester_queue.py', str(queue), '--defer-range']), patch.object(semester_queue, 'transfer_process', side_effect=AssertionError('Completed archive downloaded again')):
                semester_queue.main()

    def test_cached_copy_retirement_requires_unchanged_full_sha_proof(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            state.mkdir()
            local = root / 'renamed.mp4'
            local.write_bytes(b'lecture')
            item = dict(path='/Course/video.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(), file='https://example.invalid/video')
            source = 'https://disk.yandex.ru/d/test'
            job = dict(item=item, course='Course', attempt=3, source=source, state='pending')
            path = state / 'task.json'
            job['event'] = dict(item, corrupted_path='corrupted/old/Course/video.mp4', course='Course', status='missing')
            d.write_json(path, job)
            (root / 'corrupted/old').mkdir(parents=True)
            d.write_json(root / 'corrupted/failed.json', [job['event']])
            d.write_json(root / 'corrupted/old/failed.json', [job['event']])
            (root / 'corrupted/new').mkdir()
            latest = dict(job['event'], corrupted_path='corrupted/new/Course/video.mp4', range_attempts=[{'pass': 1}])
            d.write_json(root / 'corrupted/new/failed.json', [latest])
            d.write_json(state / 'recovery-state.json', {item['path']: dict(state='running', event=latest)})
            bg = SimpleNamespace(root=root, state=state, source=source, checkpoint=Mock(),local_candidates={})
            proof = dict(item, source=source, status='valid', local=str(local), mtime_ns=local.stat().st_mtime_ns,ctime_ns=local.stat().st_ctime_ns)
            with patch.object(d, 'sha256_file', side_effect=AssertionError('Redundant SHA')):
                for change in ({'mtime_ns': 0}, {'status': 'partial'}, {'source': 'other'}, {'sha256': '0'*64}, {'mtime_ns': None}):
                    self.assertFalse(q.retire_cached_copy(bg, path, job, [dict(proof, **change)]))
                Path(str(local) + '.aria2').touch()
                self.assertFalse(q.retire_cached_copy(bg, path, job, [proof]))
                Path(str(local) + '.aria2').unlink()
                self.assertTrue(q.retire_cached_copy(bg, path, job, [proof]))
            self.assertEqual(json.loads(path.read_text())['state'], 'applied')
            self.assertTrue(json.loads(path.read_text())['superseded_by_local_copy'])
            self.assertEqual(bg.checkpoint.call_args.args[3], 'finished')
            self.assertEqual(json.loads((root / 'corrupted/failed.json').read_text()), [])
            self.assertEqual(json.loads((root / 'corrupted/new/failed.json').read_text()), [])
            self.assertEqual(FileState(state, 'recovery-state').get(item['path'])['event']['corrupted_path'], latest['corrupted_path'])
            self.assertEqual(FileState(state, 'recovery-state').get(item['path'])['event']['range_attempts'], latest['range_attempts'])
            self.assertEqual(FileState(state, 'recovery-state').get(item['path'])['state'], 'finished')
            self.assertEqual(q.storage.saved_local(root, d.read_events(state / 'events.jsonl')[-1]['local']), local)

    def test_finished_archive_retires_obsolete_range_without_network_or_sha(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            (state / 'tasks').mkdir(parents=True)
            source = 'https://disk.yandex.ru/d/test'
            local = root / 'external-renamed.mp4'
            local.write_bytes(b'lecture')
            item = dict(path='/Course/video.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(), file='https://example.invalid/video')
            d.write_json(state / 'source.json', dict(source=source))
            catalog = state / 'catalog.json'
            d.write_json(catalog, dict(source=source))
            d.write_json(state / 'settings.json', dict(retries=2, tuning=resolve()))
            d.write_json(state / 'courses.json', ['Course'])
            d.write_json(state / 'summary.json', dict(pending_courses=[], complete=True))
            d.write_json(state / 'course-history.json', {'Course': dict(state='finished', verified=1, failed=0)})
            d.write_json(state / '01-manifest.json', [{k:v for k,v in item.items() if k != 'file'}])
            d.write_json(state / 'local-copies.json', [dict(item, source=source, status='valid', local=str(local), mtime_ns=local.stat().st_mtime_ns,ctime_ns=local.stat().st_ctime_ns)])
            task = state / 'tasks' / 'old.json'
            d.write_json(task, dict(kind='range', source=source, item=item, event={}, course='Course', attempt=3, state='pending'))
            stale = state / 'tasks' / 'stale.json'
            d.write_json(stale, dict(kind='range', source=source, item=dict(item, sha256='0'*64), event={}, course='Course', attempt=3, state='pending'))
            queue = state / 'queue.json'
            d.write_json(queue, dict(archives=[dict(state='finished', destination=str(root), catalog=str(catalog))]))
            stopping = Event()
            finish = q.finish_courses
            def finalize(*args):
                finish(*args)
                stopping.set()
            with patch('recovery.finish_recovery', side_effect=AssertionError('Redundant Range')), patch.object(d, 'sha256_file', side_effect=AssertionError('Redundant SHA')), patch.object(q, 'finish_courses', side_effect=finalize):
                q.run([queue], stopping=stopping)
            self.assertEqual(json.loads(task.read_text())['state'], 'applied')
            self.assertTrue(json.loads(stale.read_text())['superseded_by_manifest'])
            self.assertEqual(d.read_events(state / 'events.jsonl')[-1]['sha256'], item['sha256'])
            self.assertEqual(FileState(state, 'transfer-state').get(item['path'])['phase'], 'finished')
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            self.assertFalse(d.target(root, item, 'Course').exists())
            # A crash after task application must still finalize the pending summary.
            d.write_json(state / 'summary.json', dict(downloads_complete=True, pending_courses=['Course']))
            d.write_json(state / 'course-history.json', {'Course': dict(state='recovering')})
            stopping.clear()
            with patch('recovery.finish_recovery', side_effect=AssertionError('Applied task restarted')), patch.object(q, 'finish_courses', side_effect=finalize):
                q.run([queue], stopping=stopping)
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            with (state / 'download.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_legacy_recovery_repairs_journal_and_publishes_new_course_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            (state / 'tasks').mkdir(parents=True)
            source = 'https://disk.yandex.ru/d/test'
            item = dict(path='/Course/video.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(), file='https://example.invalid/video')
            event = dict(item, course='Course', attempt=3, status='missing', time='2026-01-01', corrupted_path='corrupted/old/Course/video.mp4')
            d.write_json(state / 'source.json', dict(source=source))
            catalog = state / 'catalog.json'
            d.write_json(catalog, dict(source=source))
            d.write_json(state / 'settings.json', dict(retries=2, tuning=resolve()))
            d.write_json(state / 'courses.json', ['Course'])
            d.write_json(state / 'summary.json', dict(downloads_complete=True, pending_courses=[], failed=1))
            d.write_json(state / 'course-history.json', {'Course': dict(state='finished', verified=0, failed=1)})
            d.write_json(state / '01-manifest.json', [{k:v for k,v in item.items() if k != 'file'}])
            (state / 'events.jsonl').write_text(json.dumps(event) + '\n{"path":')
            d.write_json(state / 'tasks/stale-sha.json', dict(kind='sha', source=source, item=item, course='Course', attempt=1, state='running'))
            key = hashlib.sha256(json.dumps([source, item['path'], item['size'], item['sha256'], 'range']).encode()).hexdigest()
            task = state / 'tasks' / (key + '.json')
            d.write_json(task, dict(kind='range', source=source, item=item, event=event, course='Course', attempt=3, state='pending'))
            queue = state / 'queue.json'
            d.write_json(queue, dict(report_repo=str(root / 'reports'), mark='Walterit', git_push=True, archives=[dict(state='finished', destination=str(root), catalog=str(catalog))]))
            stopping = Event()
            def recover(root, event, item, *args, **kwargs):
                local = d.target(root, item, 'Course')
                local.parent.mkdir()
                local.write_bytes(b'lecture')
                d.record(state, item, 'Course', 3, 'valid', 'SHA-256 verified')
                event['range_recovered_complete'] = True
                event['recovered_stat'] = q.Background.stat(local)
            finish = q.finish_courses
            def finalize(*args):
                try:
                    finish(*args)
                finally:
                    stopping.set()
            with patch('recovery.finish_recovery', side_effect=recover), patch('report.update_report') as report, patch.object(q, 'finish_courses', side_effect=finalize):
                q.run([queue], stopping=stopping)
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            report.assert_called_once()
            self.assertEqual(d.read_events(state / 'events.jsonl')[-1]['status'], 'valid')

    def test_four_slots_reserved_for_courses_with_personal_work_pending(self):
        from collections import Counter
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            queues = []
            for name in ('personal', 'courses'):
                root = base / name
                state = root / '.download'
                (state / 'tasks').mkdir(parents=True)
                source = 'https://disk.yandex.ru/d/' + name
                items = [dict(path=f'/Course/{i}.mp4', size=7, sha256=hashlib.sha256(b'lecture').hexdigest(), file='https://example.invalid/video') for i in range(8)]
                for filename, value in {
                    'source.json': dict(source=source), 'catalog.json': dict(source=source),
                    'settings.json': dict(retries=2, tuning=resolve()), 'courses.json': ['Course'],
                    'summary.json': dict(downloads_complete=True, pending_courses=['Course']),
                    'course-history.json': {'Course': dict(state='recovering')},
                    '01-manifest.json': [{k:v for k,v in item.items() if k != 'file'} for item in items]
                }.items():
                    d.write_json(state / filename, value)
                for item in items:
                    key = hashlib.sha256(json.dumps([source, item['path'], item['size'], item['sha256'], 'range']).encode()).hexdigest()
                    event = dict(item, course='Course', status='missing', corrupted_path='corrupted/old' + item['path'])
                    d.write_json(state / 'tasks' / (key + '.json'), dict(kind='range', source=source, item=item, event=event, course='Course', attempt=3, state='pending'))
                queue = state / 'queue.json'
                d.write_json(queue, dict(archives=[dict(destination=str(root), catalog=str(state / 'catalog.json'))]))
                queues.append(queue)
            stopping = Event()
            gate = Event()
            mutex = Lock()
            started = []
            def recover(root, event, item, *args, **kwargs):
                with mutex:
                    started.append(root.name)
                    if len(started) == 8:
                        stopping.set()
                        gate.set()
                self.assertTrue(gate.wait(3), 'Eight files did not start together')
            submitted = []
            original_submit = q.Background.submit
            original_read = q.JsonCache.read
            def capture_submit(background, *args, **kwargs):
                submitted.append(background.root.name)
                return original_submit(background, *args, **kwargs)
            def capture_read(cache, path, default=None):
                if path == queues[1].parent / 'source.json':
                    self.assertEqual(submitted[:4], ['personal'] * 4,
                                     'Ready personal tasks waited for the next archive scan')
                return original_read(cache, path, default)
            with patch('recovery.finish_recovery', side_effect=recover), \
                    patch.object(q.Background, 'submit', capture_submit), \
                    patch.object(q.JsonCache, 'read', capture_read):
                q.run(queues, files=8, secondary_slots=4, stopping=stopping)
            self.assertEqual(Counter(started), {'personal': 4, 'courses': 4})

    def test_course_reserve_reaches_later_archives_before_personal_borrows_slots(self):
        from collections import Counter
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            queues = []
            for group, counts in (('personal', [16]), ('courses', [2, 8])):
                archives = []
                for index, count in enumerate(counts):
                    root = base / f'{group}-{index}'
                    state = root / '.download'
                    (state / 'tasks').mkdir(parents=True)
                    source = 'https://disk.yandex.ru/d/' + root.name
                    items = [dict(path=f'/Course/{i}.mp4', size=7,
                                  sha256=hashlib.sha256(b'lecture').hexdigest(),
                                  file='https://example.invalid/video') for i in range(count)]
                    for filename, value in {
                        'source.json': dict(source=source), 'catalog.json': dict(source=source),
                        'settings.json': dict(retries=2, tuning=resolve()), 'courses.json': ['Course'],
                        'summary.json': dict(downloads_complete=True, pending_courses=['Course']),
                        'course-history.json': {'Course': dict(state='recovering')},
                        '01-manifest.json': [{k:v for k,v in item.items() if k != 'file'} for item in items]
                    }.items():
                        d.write_json(state / filename, value)
                    for item in items:
                        key = hashlib.sha256(json.dumps([source, item['path'], item['size'], item['sha256'], 'range']).encode()).hexdigest()
                        event = dict(item, course='Course', status='missing', corrupted_path='corrupted/old' + item['path'])
                        d.write_json(state / 'tasks' / (key + '.json'), dict(kind='range', source=source,
                                     item=item, event=event, course='Course', attempt=3, state='pending'))
                    archives.append(dict(destination=str(root), catalog=str(state / 'catalog.json')))
                queue = base / (group + '.json')
                d.write_json(queue, dict(archives=archives))
                queues.append(queue)
            stopping = Event()
            gate = Event()
            mutex = Lock()
            started = []
            def recover(root, event, item, *args, **kwargs):
                with mutex:
                    started.append(root.name)
                    if len(started) == 16:
                        stopping.set()
                        gate.set()
                self.assertTrue(gate.wait(3), 'Sixteen files did not start together')
            with patch('recovery.finish_recovery', side_effect=recover):
                q.run(queues, files=16, secondary_slots=8, stopping=stopping)
            self.assertEqual(Counter(started), {'personal-0': 8, 'courses-0': 2, 'courses-1': 6})

    def test_worker_recovers_four_files_before_full_task_discovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / '.download'
            (state / 'tasks').mkdir(parents=True)
            source = 'https://disk.yandex.ru/d/test'
            content = b'lecture'
            digest = hashlib.sha256(content).hexdigest()
            d.write_json(state / 'source.json', dict(source=source))
            catalog = state / 'catalog.json'
            d.write_json(catalog, dict(source=source))
            d.write_json(state / 'settings.json', dict(retries=2, tuning=resolve()))
            d.write_json(state / 'courses.json', ['Course'])
            d.write_json(state / 'summary.json', dict(downloads_complete=True, pending_courses=['Course']))
            d.write_json(state / 'course-history.json', {'Course': dict(state='recovering')})
            items = []
            for i in range(4):
                item = dict(path=f'/Course/{i}.mp4', size=len(content), sha256=digest, file='https://example.invalid/video')
                event = dict(item, course='Course', attempt=3, time='2026-01-01', status='missing', corrupted_path=f'corrupted/old/Course/{i}.mp4')
                key = hashlib.sha256(json.dumps([source, item['path'], item['size'], item['sha256'], 'range']).encode()).hexdigest()
                d.write_json(state / 'tasks' / (key + '.json'), dict(kind='range', source=source, item=item, event=event, course='Course', attempt=3, state='pending'))
                items.append(item)
            d.write_json(state / '01-manifest.json', [{k:v for k,v in item.items() if k != 'file'} for item in items])
            queue = state / 'queue.json'
            d.write_json(queue, dict(archives=[dict(destination=str(root), catalog=str(catalog))]))
            for index in range(40):
                d.write_json(state / 'tasks' / f'zz-{index:02d}.json', dict(kind='sha'))
            stopping = Event()
            gate = Event()
            mutex = Lock()
            count = 0
            read_tasks = set()
            original_read = Path.read_text
            def read(path, *args, **kwargs):
                if path.parent == state / 'tasks':
                    read_tasks.add(path)
                return original_read(path, *args, **kwargs)
            def recover(root, event, item, *args, **kwargs):
                nonlocal count
                self.assertEqual(kwargs['tuning']['range_workers'], 2)
                self.assertIsNotNone(kwargs['request_budget'])
                self.assertLessEqual(len(read_tasks), 32, 'Worker waited for the full task scan')
                with mutex:
                    count += 1
                    if count == 4:
                        gate.set()
                self.assertTrue(gate.wait(3), 'Files were serialized')
                local = d.target(root, item, 'Course')
                local.parent.mkdir(exist_ok=True)
                local.write_bytes(content)
                d.record(state, item, 'Course', 3, 'valid', 'SHA-256 verified')
                event['range_recovered_complete'] = True
                event['recovered_stat'] = q.Background.stat(local)
            finish = q.finish_courses
            def finalize(*args):
                finish(*args)
                if not json.loads((state / 'summary.json').read_text())['pending_courses']:
                    stopping.set()
            with patch('recovery.finish_recovery', side_effect=recover), patch.object(q, 'finish_courses', side_effect=finalize), \
                    patch.object(q, 'retire_cached_copy', side_effect=AssertionError('Completed task was offered again')), \
                    patch.object(Path, 'read_text', new=read):
                q.run([queue], stopping=stopping)
            self.assertEqual(count, 4)
            self.assertEqual(len(read_tasks), 44, 'Unvisited tasks were skipped')
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            self.assertTrue(all(json.loads(path.read_text())['state'] == 'applied' for path in (state / 'tasks').glob('*.json') if len(path.stem) == 64))
            with (state / 'download.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
