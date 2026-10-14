#!/usr/bin/env python3
"""Recover durable Range tasks across archives without blocking ordinary downloads."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import math
from threading import Event, Lock, Semaphore
import time

import download as d
import storage
from report import queue_report, retry_reports
from pipeline import Background, failure_manifests, now
import recovery as r
from settings import resolve
from metrics import Metrics


class RangeBudget:
    def __init__(self, requests=12, rps=12):
        if requests < 1 or not math.isfinite(rps) or rps <= 0:
            raise ValueError('Range request limits must be positive')
        self.slots = Semaphore(requests)
        self.rate = Lock()
        self.interval = 1 / rps
        self.next_request = 0.0

    @contextmanager
    def request(self, stopping):
        while not self.slots.acquire(timeout=.25):
            if stopping.is_set():
                raise InterruptedError('Range interrupted')
        try:
            with self.rate:
                if stopping.wait(max(0, self.next_request - time.monotonic())):
                    raise InterruptedError('Range interrupted')
                self.next_request = time.monotonic() + self.interval
            yield
        finally:
            self.slots.release()


class JsonCache:
    def __init__(self):
        self.entries = {}
        self.directories = {}

    def read(self, path, default=None):
        if not path.exists():
            self.entries.pop(path, None)
            return default
        stamp = Background.stat(path)
        saved = self.entries.get(path)
        if saved is None or saved[0] != stamp:
            saved = stamp, json.loads(path.read_text())
            self.entries[path] = saved
        return saved[1]

    def tasks(self, directory):
        if not directory.exists():
            self.directories.pop(directory, None)
            return []
        stamp = Background.stat(directory)
        saved = self.directories.get(directory)
        if saved is not None and saved[0] == stamp:
            return saved[2]
        previous = saved[1] if saved else {}
        current = {}
        for path in sorted(directory.glob('*.json')):
            file_stamp = Background.stat(path)
            if path in previous and previous[path][0] == file_stamp:
                summary = previous[path][1]
            else:
                job = json.loads(path.read_text())
                summary = None
                if job['kind'] == 'range':
                    item = job['item']
                    summary = dict(kind='range', state=job['state'], course=job['course'],
                                   source=job['source'], attempt=job['attempt'],
                                   item={key: item[key] for key in ('path', 'size', 'sha256')},
                                   superseded_by_manifest=job.get('superseded_by_manifest', False),
                                   result={'event': {'range_recovered_complete':
                                       job.get('result', {}).get('event', {}).get('range_recovered_complete', False)}})
            current[path] = file_stamp, summary
        tasks = [(path, row) for path, (_, row) in current.items() if row is not None]
        self.directories[directory] = stamp, current, tasks
        return tasks


def retire_cached_copy(background, path, job, proofs):
    item = job['item']
    destination = d.target(background.root, item, job['course'])
    for proof in proofs:
        if (proof.get('status') != 'valid' or proof.get('path') != item['path']
                or proof.get('source', background.source).rstrip('/') != background.source.rstrip('/')
                or (proof.get('size'), proof.get('sha256')) != (item['size'], item['sha256'])
                or proof.get('mtime_ns') is None):
            continue
        local = Path(proof['local']) if proof.get('local') else destination
        try:
            stat = local.stat()
        except FileNotFoundError:
            continue
        if ((stat.st_size, stat.st_mtime_ns) != (item['size'], proof['mtime_ns'])
                or not local.is_file() or Path(str(local) + '.aria2').exists()):
            continue
        recovery_path = background.state / 'recovery-state.json'
        saved = json.loads(recovery_path.read_text()).get(item['path'], {}).get('event', {}) if recovery_path.exists() else {}
        previous = saved if (saved.get('size'), saved.get('sha256')) == (item['size'], item['sha256']) else job.get('event', {})
        archived = previous.get('corrupted_path')
        if archived:
            r.body_path(background.root, dict(corrupted_path=archived))
        event = d.record(background.state, item, job['course'], job['attempt'], 'valid',
                         'Cached SHA-256 verified local copy; obsolete Range task retired', local=local,
                         verified_stat=[stat.st_ino, stat.st_size, stat.st_mtime_ns])
        if event['status'] != 'valid':
            continue
        event = dict(previous, **event, range_recovered_complete=True)
        if archived:
            event['corrupted_path'] = archived
            failure_manifests(background.root, event)
        r.recovery_state(background.root, event, 'finished')
        background.checkpoint(item, job['course'], job['attempt'], 'finished', event)
        job.update(state='applied', result={'event': event}, time=now(), superseded_by_local_copy=True)
        d.write_json(path, job)
        return True
    return False


def finish_courses(background, config, changed_courses=()):
    root, state = background.root, background.state
    courses = json.loads((state / 'courses.json').read_text())
    history_path = state / 'course-history.json'
    history = json.loads(history_path.read_text())
    events = {row['path']: row for row in d.read_events(state / 'events.jsonl')}
    registry = state / 'local-copies.json'
    external = {row['path']: row for row in (json.loads(registry.read_text()) if registry.exists() else [])
                if row.get('source', '').rstrip('/') == background.source.rstrip('/') and row.get('status') == 'valid'}
    jobs = [json.loads(path.read_text()) for path in background.directory.glob('*.json')]
    for index, course in enumerate(courses, 1):
        pending = any(job['kind'] == 'range' and job['course'] == course and job['state'] != 'applied' for job in jobs)
        if history.get(course, {}).get('state') == 'finished' and not pending and course not in changed_courses:
            continue
        entries = json.loads((state / f'{index:02d}-manifest.json').read_text())
        valid = 0
        verified_paths=set()
        for item in entries:
            for proof in (events.get(item['path'], {}), external.get(item['path'], {})):
                # Manifests deliberately omit expiring URLs; only the destination is needed here.
                local = Path(proof['local']) if proof.get('local') else d.target(root, dict(item, file='https://disk.yandex.ru'), course)
                if (proof.get('status') == 'valid' and (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256'])
                        and local.exists() and local.stat().st_size == item['size']
                        and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                        and not Path(str(local) + '.aria2').exists()):
                    valid += 1
                    verified_paths.add(item['path'])
                    break
        from nextcloud import remote_saved
        settings=json.loads((state/'settings.json').read_text())
        remote=sum(remote_saved(state,item,None if getattr(background,'fallback',None) else settings.get('nextcloud_existing')) for item in entries
                   if item['path'] not in verified_paths)
        status = dict(course=course, source=background.source, state='recovering' if pending else 'finished',
                      complete=not pending and valid + remote == len(entries), total=len(entries), verified=valid, remote_available=remote,
                      failed=0 if pending else len(entries) - valid - remote, pending=len(entries) - valid - remote if pending else 0, time=now())
        if not pending and config.get('report_repo'):
            queue_report(root, Path(config['report_repo']), config['mark'], config.get('git_push', False),
                         f"Update {config['mark']} copies: {root.name} / {course}")
        history[course] = status
        d.write_json(state / f'{index:02d}-status.json', status)
        if not pending:
            print(('COURSE AVAILABLE: ' if status['complete'] and remote else 'COURSE VERIFIED COMPLETE: ' if status['complete'] else 'COURSE FINISHED WITH ERRORS: ') + str(root) + ' / ' + course, flush=True)
    d.write_json(history_path, history)
    pending = [course for course in courses if history[course]['state'] != 'finished']
    failed = sum(history[course]['failed'] for course in courses)
    d.write_json(state / 'summary.json', dict(verified=sum(history[course]['verified'] for course in courses),remote_available=sum(history[course].get('remote_available',0) for course in courses),
                 failed=failed, pending_courses=pending, downloads_complete=True, complete=not failed and not pending))


def fallback_due(fallback,job):
    return (fallback is not None and job['state']=='applied' and job['kind']=='range'
            and not job.get('superseded_by_manifest')
            and not job.get('result',{}).get('event',{}).get('range_recovered_complete')
            and fallback.ready(job['item']))


def run(queues, files=4, per_file=2, requests=12, rps=12, stopping=None, secondary_slots=1, budget_directory=None, metrics_interval=60, nextcloud_fallback=None):
    if files < 1 or per_file < 1:
        raise ValueError('Range file limits must be positive')
    if not 1 <= secondary_slots <= max(1, files - 1):
        raise ValueError('Secondary slots must leave a slot for the first queue')
    stopping = stopping if stopping is not None else Event()
    if budget_directory is None:
        budget = RangeBudget(requests, rps)
    else:
        from request_budget import ProcessRangeBudget
        budget = ProcessRangeBudget(budget_directory, requests, rps)
    cache = JsonCache()
    roots = {}
    priorities = {}
    fallbacks = {}
    with Metrics(queues[0].with_suffix('.range.metrics.jsonl'),'range-queue',interval=metrics_interval,
                 settings=dict(files=files,per_file=per_file,requests=requests,rps=rps,secondary_slots=secondary_slots)) as metrics, \
            ThreadPoolExecutor(max_workers=files, thread_name_prefix='range-file') as pool:
        try:
            while not stopping.is_set():
                for queue in queues:
                    storage.check(queue)
                # Poll applies every result under the archive's exclusive download lock.
                for root, (background, lock, config) in list(roots.items()):
                    before = len(background.futures)
                    changed_courses = {job['course'] for job in background.jobs.values()}
                    background.poll()
                    if len(background.futures) != before:
                        finish_courses(background, config, changed_courses)
                eligible = []
                for priority, queue in enumerate(queues):
                    config = cache.read(queue)
                    for archive in config['archives']:
                        if not archive.get('enabled', True):
                            continue
                        root = Path(archive['destination']).resolve()
                        retry_reports(root, stopping)
                        state = root / '.download'
                        summary = cache.read(state / 'summary.json', {})
                        if not summary.get('downloads_complete') and archive.get('state') != 'finished':
                            continue
                        tasks = cache.tasks(state / 'tasks')
                        settings=cache.read(state/'settings.json',{})
                        link=nextcloud_fallback or settings.get('nextcloud_fallback')
                        fallback=None
                        if link and state.joinpath('source.json').exists() and not cache.read(state/'catalog.json',{}).get('root'):
                            from nextcloud_fallback import Fallback
                            if root not in fallbacks or fallbacks[root].link!=link:
                                fallbacks[root]=Fallback(root,link,stopping)
                            fallback=fallbacks[root]
                        if not summary.get('pending_courses') and not any(job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or fallback_due(fallback,job)) for _, job in tasks):
                            continue
                        if root not in roots:
                            lock = (state / 'download.lock').open('a')
                            try:
                                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            except BlockingIOError:
                                lock.close()
                                continue
                            try:
                                identity = cache.read(state / 'source.json')
                                catalog = cache.read(Path(archive['catalog']))
                                if identity['source'] != catalog['source']:
                                    raise ValueError('Range archive source does not match catalog')
                                settings = cache.read(state / 'settings.json')
                                tuning = resolve({**settings['tuning'], 'range_workers': per_file, 'range_rps': rps})
                                background = Background(root, identity['source'], settings['retries'], tuning,
                                                        range_pool=pool, budget=budget, range_files=files, nextcloud_fallback=link if fallback else None,retry_check=settings.get('retry_check',False))
                            except BaseException:
                                lock.close()
                                raise
                            roots[root] = background, lock, config
                            priorities[root] = priority
                            background.cached_events = {row['path']: row for row in d.read_events(state / 'events.jsonl', repair=True)}
                            background.manifest_items = {(course, item['path']): (item['size'], item['sha256'])
                                for index, course in enumerate(cache.read(state / 'courses.json'), 1)
                                for item in cache.read(state / f'{index:02d}-manifest.json')}
                        background = roots[root][0]
                        retired = set()
                        external = cache.read(state / 'local-copies.json', [])
                        external_by_path = {}
                        for copy in external:
                            if copy.get('source', '').rstrip('/') == background.source.rstrip('/'):
                                external_by_path.setdefault(copy['path'], []).append(copy)
                        for path, job in tasks:
                            if path in background.futures:
                                continue
                            if job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or fallback_due(fallback,job)):
                                if job['source'] != background.source:
                                    raise ValueError('Range task source mismatch')
                                d.target(root, job['item'], job['course'])
                                item = job['item']
                                from nextcloud import remote_saved
                                settings=cache.read(state/'settings.json')
                                if not fallback and remote_saved(state,item,settings.get('nextcloud_existing')):
                                    background.checkpoint(item,job['course'],job['attempt'],'remote')
                                    saved = json.loads(path.read_text())
                                    saved.update(state='applied',time=now(),superseded_by_nextcloud=True)
                                    d.write_json(path,saved)
                                    retired.add(job['course'])
                                    continue
                                if background.manifest_items.get((job['course'], item['path'])) != (item['size'], item['sha256']):
                                    saved = json.loads(path.read_text())
                                    saved.update(state='applied', time=now(), superseded_by_manifest=True)
                                    d.write_json(path, saved)
                                    continue
                                proofs = [background.cached_events.get(item['path'], {}), *external_by_path.get(item['path'], [])]
                                if any(proof.get('status') == 'valid' for proof in proofs):
                                    if retire_cached_copy(background, path, json.loads(path.read_text()), proofs):
                                        retired.add(job['course'])
                                        continue
                                eligible.append((priority, background, path))
                        if retired:
                            finish_courses(background, config, retired)
                active = sum(len(background.futures) for background, _, _ in roots.values())
                while eligible and active < files:
                    # Reserve slots for courses while retaining priority for personal files.
                    chosen = 0
                    if len(queues) > 1 and any(row[0] > 0 for row in eligible):
                        first_active = sum(len(bg.futures) for bg, _, config in roots.values()
                                           if priorities[bg.root] == 0)
                        if first_active >= max(1, files - secondary_slots):
                            chosen = next(i for i, row in enumerate(eligible) if row[0] > 0)
                    _, background, path = eligible.pop(chosen)
                    job = json.loads(path.read_text())
                    print('RANGE START: ' + str(background.root) + job['item']['path'], flush=True)
                    background.submit('range', job['item'], job['course'], job['attempt'], job['event'],force=job['state']=='applied',fallback_only=job['state']=='applied')
                    active += 1
                for root, (background, lock, config) in list(roots.items()):
                    if not background.futures:
                        if any(row[1] is background for row in eligible):
                            continue
                        if cache.read(background.state / 'summary.json', {}).get('pending_courses'):
                            finish_courses(background, config)
                        background.close()
                        lock.close()
                        del roots[root]
                metrics.gauge('range_active_files',sum(len(bg.futures) for bg,_,_ in roots.values()))
                metrics.gauge('range_eligible_files',len(eligible))
                metrics.gauge('archives_owned',len(roots))
                stopping.wait(2)
        finally:
            for background, _, _ in roots.values():
                background.stopping.set()
            for background, lock, _ in roots.values():
                background.close()
            pool.shutdown(wait=True, cancel_futures=True)
            for background, lock, _ in roots.values():
                background.metrics.close()
                lock.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--queue', action='append', required=True, type=Path, help='Repeat in priority order')
    parser.add_argument('--files', type=int, default=4)
    parser.add_argument('--secondary-slots', type=int, default=1, help='File slots reserved for later queues while the first queue has work')
    parser.add_argument('--requests-per-file', type=int, default=2)
    parser.add_argument('--requests', type=int, default=12)
    parser.add_argument('--rps', type=float, default=12)
    parser.add_argument('--metrics-interval',type=int,default=60,help='Service metrics interval, seconds')
    parser.add_argument('--budget', type=Path, help='Share the HTTP budget with overlapping ordinary downloaders')
    parser.add_argument('--nextcloud-fallback',help='Also backfill completed failed course tasks from this public share')
    args = parser.parse_args()
    stopping = Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: stopping.set())
    # Prevent two workers from multiplying the global limits.
    with args.queue[0].with_suffix('.range.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args.queue, args.files, args.requests_per_file, args.requests, args.rps, stopping, args.secondary_slots, args.budget, args.metrics_interval, args.nextcloud_fallback)


if __name__ == '__main__':
    try:
        main()
    except OSError as error:
        if not storage.disk_error(error):
            raise
        print('DISK OFFLINE: resume state retained', flush=True)
        raise SystemExit(storage.TEMPFAIL)
