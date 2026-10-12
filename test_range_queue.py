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
