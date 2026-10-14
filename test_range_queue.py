"""Shared Range budgets, archive handoff and durable results."""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
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


class SharedRangeChecks(unittest.TestCase):
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
            bg = SimpleNamespace(root=root, state=state, source=source, checkpoint=Mock())
            proof = dict(item, source=source, status='valid', local=str(local), mtime_ns=local.stat().st_mtime_ns)
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
            self.assertEqual(json.loads((state / 'recovery-state.json').read_text())[item['path']]['event']['corrupted_path'], latest['corrupted_path'])
            self.assertEqual(json.loads((state / 'recovery-state.json').read_text())[item['path']]['event']['range_attempts'], latest['range_attempts'])
            self.assertEqual(json.loads((state / 'recovery-state.json').read_text())[item['path']]['state'], 'finished')
            self.assertEqual(d.read_events(state / 'events.jsonl')[-1]['local'], str(local))

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
            d.write_json(state / 'local-copies.json', [dict(item, source=source, status='valid', local=str(local), mtime_ns=local.stat().st_mtime_ns)])
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
            self.assertEqual(json.loads((state / 'transfer-state.json').read_text())[item['path']]['phase'], 'finished')
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
            with patch('recovery.finish_recovery', side_effect=recover):
                q.run(queues, files=8, secondary_slots=4, stopping=stopping)
            self.assertEqual(Counter(started), {'personal': 4, 'courses': 4})

    def test_worker_recovers_four_files_without_ordinary_downloads(self):
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
            stopping = Event()
            gate = Event()
            mutex = Lock()
            count = 0
            def recover(root, event, item, *args, **kwargs):
                nonlocal count
                self.assertEqual(kwargs['tuning']['range_workers'], 2)
                self.assertIsNotNone(kwargs['request_budget'])
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
            with patch('recovery.finish_recovery', side_effect=recover), patch.object(q, 'finish_courses', side_effect=finalize):
                q.run([queue], stopping=stopping)
            self.assertEqual(count, 4)
            self.assertTrue(json.loads((state / 'summary.json').read_text())['complete'])
            self.assertTrue(all(json.loads(path.read_text())['state'] == 'applied' for path in (state / 'tasks').glob('*.json') if len(path.stem) == 64))
            with (state / 'download.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
