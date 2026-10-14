#!/usr/bin/env python3
"""Recover durable Range tasks across archives without blocking ordinary downloads."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import math
import hashlib
from datetime import datetime
from threading import Event, Lock, Semaphore
import time

import download as d
import storage
from report import queue_report, retry_reports
from pipeline import Background, failure_manifests, now
from file_state import FileState
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
                                   item={key: item[key] for key in ('path', 'size', 'sha256', 'file') if key in item},
                                   superseded_by_manifest=job.get('superseded_by_manifest', False),
                                   superseded_by_nextcloud=job.get('superseded_by_nextcloud', False),
                                   superseded_by_local_copy=job.get('superseded_by_local_copy', False),
                                   yandex_recheck=job.get('yandex_recheck',False),
                                   yandex_checked_at=job.get('yandex_checked_at'),
                                   result={'event': {'range_recovered_complete':
                                       job.get('result', {}).get('event', {}).get('range_recovered_complete', False),
                                       'status':job.get('result',{}).get('event',{}).get('status')}})
            current[path] = file_stamp, summary
        tasks = [(path, row) for path, (_, row) in current.items() if row is not None]
        self.directories[directory] = stamp, current, tasks
        return tasks



class EventIndex:
    """Hold current compact proofs, not the complete append-only JSONL history."""

    FIELDS = ('path', 'source', 'status', 'local', 'size', 'sha256', 'mtime_ns')

    def __init__(self, path):
        self.path = path
        self.events = {}
        self.inode = None
        self.offset = 0

    def refresh(self):
        # The writer and reader share STATE_LOCK; a crash-torn final row is repaired.
        with d.STATE_LOCK:
            try:
                stat = self.path.stat()
            except FileNotFoundError:
                self.events.clear()
                self.inode = None
                self.offset = 0
                return self.events
            if self.inode != stat.st_ino or stat.st_size < self.offset:
                self.events.clear()
                self.offset = 0
            self.inode = stat.st_ino
            for offset, row in d.scan_events(self.path, repair=True, start=self.offset):
                self.offset = offset
                if row is not None:
                    self.events[row['path']] = {key: row[key] for key in self.FIELDS if key in row}
        return self.events

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
        saved = FileState(background.state, 'recovery-state').get(item['path'], {}).get('event', {})
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
    events = (background.event_index.refresh() if hasattr(background, 'event_index')
              else {row['path']: row for row in d.read_events(state / 'events.jsonl')})
    registry = state / 'local-copies.json'
    external = {row['path']: row for row in (json.loads(registry.read_text()) if registry.exists() else [])
                if row.get('source', '').rstrip('/') == background.source.rstrip('/') and row.get('status') == 'valid'}
    jobs = ([job for _, job in background.task_cache.tasks(background.directory)]
            if hasattr(background, 'task_cache')
            else [json.loads(path.read_text()) for path in background.directory.glob('*.json')])
    from nextcloud import saved_remote_files
    settings = json.loads((state/'settings.json').read_text())
    link = None if getattr(background, 'fallback', None) else settings.get('nextcloud_existing')
    remote_path = state/'nextcloud-existing.json'
    saved = json.loads(remote_path.read_text()) if link and remote_path.exists() else {}
    remote_files = saved_remote_files(state, link, source=background.source, saved=saved)
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
        remote=sum((remote_files.get(item['path'], {}).get('size'),
                    remote_files.get(item['path'], {}).get('sha256')) == (item['size'], item['sha256'])
                   for item in entries if item['path'] not in verified_paths)
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


def yandex_checked_time(job):
    try:return datetime.fromisoformat(job.get('yandex_checked_at')).timestamp()
    except (TypeError,ValueError):return 0


def recheck_due(job, interval, timestamp=None):
    result=job.get('result',{}).get('event',{})
    if (not interval or job['kind']!='range' or job['state']!='applied'
            or any(job.get('superseded_by_'+reason) for reason in
                   ('manifest', 'nextcloud', 'local_copy'))
            or result.get('range_recovered_complete') or result.get('status')=='valid'):
        return False
    last=yandex_checked_time(job)
    if not last:return True
    return (time.time() if timestamp is None else timestamp)-last>=interval


def seed_legacy_tasks(background):
    """Convert legacy failed manifests to replayable tasks under the archive lock."""
    root,state=background.root,background.state
    marker=state/'legacy-recheck-seeded.json'
    if marker.exists() and json.loads(marker.read_text()).get('complete'):return
    aggregate=root/'corrupted/failed.json'
    paths=[aggregate] if aggregate.exists() else list((root/'corrupted').glob('*/failed.json'))
    failures={row['path']:row for path in paths for row in json.loads(path.read_text())}
    saved=FileState(state, 'recovery-state')
    count=0
    for index,course in enumerate(json.loads((state/'courses.json').read_text()),1):
        for item in json.loads((state/f'{index:02d}-manifest.json').read_text()):
            event=failures.get(item['path'])
            if not event or (event.get('size'),event.get('sha256'))!=(item['size'],item['sha256']):continue
            previous=saved.get(item['path'],{}).get('event',{})
            if r.prefer_saved_event(root,event,previous):event=previous
            if event.get('status')=='valid' or event.get('range_recovered_complete'):continue
            r.body_path(root,event)
            item=dict(item,file=background.source)
            d.target(root,item,course)
            key=hashlib.sha256(json.dumps([background.source,item['path'],item['size'],item['sha256'],'range']).encode()).hexdigest()
            path=background.directory/(key+'.json')
            if path.exists():continue
            d.write_json(path,dict(kind='range',state='applied',source=background.source,item=item,course=course,
                                  attempt=event.get('attempt',background.retries+1),event=event,result={'event':event},time=now()))
            count+=1
    d.write_json(marker,dict(complete=True,time=now(),files_seeded=count))
    if count:print('YANDEX RECHECK LEGACY QUEUED: '+str(root)+'; files='+str(count),flush=True)


def run(queues, files=4, per_file=2, requests=12, rps=12, stopping=None, secondary_slots=1, budget_directory=None, metrics_interval=60, nextcloud_fallback=None, recheck_interval=0, recheck_files=8, recheck_probes=16, recheck_full_interval=604800):
    if files < 1 or per_file < 1:
        raise ValueError('Range file limits must be positive')
    if not 1 <= secondary_slots <= max(1, files - 1):
        raise ValueError('Secondary slots must leave a slot for the first queue')
    if not math.isfinite(recheck_interval) or recheck_interval<0 or recheck_files<1:
        raise ValueError('Recheck interval must be nonnegative and recheck files positive')
    if recheck_probes < 0 or not math.isfinite(recheck_full_interval) or recheck_full_interval < 0:
        raise ValueError('Recheck probes and full interval must be nonnegative')
    if recheck_interval and len(queues)>1 and recheck_files<2:
        raise ValueError('Rechecks across queues require at least two file slots')
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
    recheck_roots = {}
    recheck_limits = ([recheck_files] if len(queues)==1 else [(recheck_files+1)//2,recheck_files//2])
    with Metrics(queues[0].with_suffix('.range.metrics.jsonl'),'range-queue',interval=metrics_interval,
                 settings=dict(files=files,per_file=per_file,requests=requests,rps=rps,secondary_slots=secondary_slots,recheck_interval=recheck_interval,recheck_files=recheck_files,recheck_probes=recheck_probes,recheck_full_interval=recheck_full_interval)) as metrics, \
            ThreadPoolExecutor(max_workers=files+(recheck_files if recheck_interval else 0), thread_name_prefix='range-file') as pool:
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
                rechecks = []
                recheck_times = {}
                for priority, queue in enumerate(queues):
                    config = cache.read(queue)
                    archives=list(config['archives'])
                    predecessor=config.get('wait_for',{}).get('destination')
                    if recheck_interval and predecessor and all(row['destination']!=predecessor for row in archives):
                        old_root=Path(predecessor)
                        if (old_root/'.download/catalog.json').exists():
                            archives.insert(0,dict(destination=predecessor,catalog=str(old_root/'.download/catalog.json'),state='finished',legacy_recheck=True))
                    for archive in archives:
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
                        legacy=recheck_interval and archive.get('legacy_recheck') and not cache.read(state/'legacy-recheck-seeded.json',{}).get('complete')
                        if not legacy and not summary.get('pending_courses') and not any(job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or recheck_due(job,recheck_interval) or fallback_due(fallback,job)) for _, job in tasks):
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
                                                        range_pool=pool, budget=budget, range_files=files, nextcloud_fallback=link if fallback else None,retry_check=settings.get('retry_check',False),
                                                        recheck_probes=recheck_probes,recheck_full_interval=recheck_full_interval)
                            except BaseException:
                                lock.close()
                                raise
                            roots[root] = background, lock, config
                            priorities[root] = priority
                            background.event_index = EventIndex(state / 'events.jsonl')
                            background.cached_events = background.event_index.refresh()
                            background.task_cache = cache
                            if legacy:
                                seed_legacy_tasks(background)
                                tasks=cache.tasks(state/'tasks')
                            background.manifest_items = {(course, item['path']): (item['size'], item['sha256'])
                                for index, course in enumerate(cache.read(state / 'courses.json'), 1)
                                for item in cache.read(state / f'{index:02d}-manifest.json')}
                        background = roots[root][0]
                        background.cached_events = background.event_index.refresh()
                        from nextcloud import saved_remote_files
                        remote_link = None if fallback else settings.get('nextcloud_existing')
                        remote_files = saved_remote_files(state, remote_link, source=background.source,
                            saved=cache.read(state/'nextcloud-existing.json', {}) if remote_link else {})
                        retired = set()
                        external = cache.read(state / 'local-copies.json', [])
                        external_by_path = {}
                        for copy in external:
                            if copy.get('source', '').rstrip('/') == background.source.rstrip('/'):
                                external_by_path.setdefault(copy['path'], []).append(copy)
                        for path, job in tasks:
                            if path in background.futures:
                                continue
                            checking=bool(recheck_interval and (job.get('yandex_recheck') and job['state']!='applied' or recheck_due(job,recheck_interval)))
                            if job['kind'] == 'range' and (job['state'] in ('pending', 'running', 'done') or checking or fallback_due(fallback,job)):
                                if job['source'] != background.source:
                                    raise ValueError('Range task source mismatch')
                                d.target(root, job['item'], job['course'])
                                item = job['item']
                                remote = remote_files.get(item['path'], {})
                                if (remote.get('size'), remote.get('sha256')) == (item['size'], item['sha256']):
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
                                (rechecks if checking else eligible).append((priority, background, path))
                                if checking:recheck_times[path]=(job['state']=='applied',yandex_checked_time(job))
                        if retired:
                            finish_courses(background, config, retired)
                def is_recheck(background,path):
                    return bool(recheck_interval and background.jobs[path].get('yandex_recheck'))
                active = sum(not is_recheck(bg,path) for bg,_,_ in roots.values() for path in bg.futures)
                while eligible and active < files:
                    # Reserve slots for courses while retaining priority for personal files.
                    chosen = 0
                    if len(queues) > 1 and any(row[0] > 0 for row in eligible):
                        first_active = sum(not is_recheck(bg,path) for bg,_,_ in roots.values()
                                           if priorities[bg.root] == 0 for path in bg.futures)
                        if first_active >= max(1, files - secondary_slots):
                            chosen = next(i for i, row in enumerate(eligible) if row[0] > 0)
                    _, background, path = eligible.pop(chosen)
                    job = json.loads(path.read_text())
                    print('RANGE START: ' + str(background.root) + job['item']['path'], flush=True)
                    background.submit('range', job['item'], job['course'], job['attempt'], job['event'],force=job['state']=='applied',fallback_only=job['state']=='applied')
                    active += 1
                checking_active=Counter(min(priorities[bg.root],1) for bg,_,_ in roots.values()
                                        for path in bg.futures if is_recheck(bg,path))
                rechecks.sort(key=lambda row:recheck_times[row[2]])
                for group,limit in enumerate(recheck_limits):
                    while checking_active[group]<limit:
                        options=[row for row in rechecks if min(row[0],1)==group]
                        if not options:break
                        candidates=list(dict.fromkeys(row[1].root for row in options))
                        previous=recheck_roots.get(group)
                        root=candidates[(candidates.index(previous)+1)%len(candidates)] if previous in candidates else candidates[0]
                        row=next(row for row in options if row[1].root==root);rechecks.remove(row)
                        _,background,path=row;job=json.loads(path.read_text())
                        event=job.get('result',{}).get('event') or job['event']
                        print('YANDEX RECHECK START: '+str(background.root)+job['item']['path'],flush=True)
                        background.submit('range',job['item'],job['course'],job['attempt'],event,
                                          force=job['state']=='applied',yandex_recheck=True)
                        recheck_roots[group]=root;checking_active[group]+=1
                for root, (background, lock, config) in list(roots.items()):
                    if not background.futures:
                        if any(row[1] is background for row in eligible+rechecks):
                            continue
                        if cache.read(background.state / 'summary.json', {}).get('pending_courses'):
                            finish_courses(background, config)
                        background.close()
                        lock.close()
                        del roots[root]
                metrics.gauge('range_active_files',sum(len(bg.futures) for bg,_,_ in roots.values()))
                metrics.gauge('range_eligible_files',len(eligible))
                metrics.gauge('range_recheck_active_files',sum(checking_active.values()))
                metrics.gauge('range_recheck_eligible_files',len(rechecks))
                metrics.gauge('range_regular_active_files',active)
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
    parser.add_argument('--recheck-interval',type=float,default=0,help='Seconds between completed Yandex rechecks; 0 disables')
    parser.add_argument('--recheck-files',type=int,default=8,help='Extra recheck file slots, split between first and later queues')
    parser.add_argument('--recheck-probes',type=int,default=16,help='Maximum cheap availability requests before full recovery; 0 always runs full recovery')
    parser.add_argument('--recheck-full-interval',type=float,default=604800,help='Seconds between full rechecks even after negative probes; 0 always runs full recovery')
    args = parser.parse_args()
    stopping = Event()
    for number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(number, lambda *_: stopping.set())
    # Prevent two workers from multiplying the global limits.
    with args.queue[0].with_suffix('.range.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run(args.queue, args.files, args.requests_per_file, args.requests, args.rps, stopping, args.secondary_slots, args.budget, args.metrics_interval, args.nextcloud_fallback,args.recheck_interval,args.recheck_files,args.recheck_probes,args.recheck_full_interval)


if __name__ == '__main__':
    try:
        main()
    except OSError as error:
        if not storage.disk_error(error):
            raise
        print('DISK OFFLINE: resume state retained', flush=True)
        raise SystemExit(storage.TEMPFAIL)
