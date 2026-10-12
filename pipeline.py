"""Ordered course transfers with durable SHA and Range background tasks."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import fcntl
import hashlib
import json
from local_paths import local_path
from pathlib import Path
import shutil
import subprocess
from threading import Event
import time

import download as d
import recovery as r
from settings import resolve


def now():
    return datetime.now().astimezone().isoformat()


def failure_manifests(root, event):
    with d.STATE_LOCK:
        paths = [root / 'corrupted/failed.json', (root / event['corrupted_path']).parents[len(Path(event['path']).parts) - 2] / 'failed.json']
        for path in dict.fromkeys(paths):
            path.parent.mkdir(parents=True, exist_ok=True)
            rows = json.loads(path.read_text()) if path.exists() else []
            rows = [row for row in rows if not (row['path'] == event['path'] and row['sha256'] == event['sha256'])]
            if not event.get('range_recovered_complete'):
                rows.append(event)
            d.write_json(path, rows)


class Background:
    def __init__(self, root, source, retries, tuning):
        self.root, self.source, self.retries, self.tuning = root, source, retries, tuning
        self.state = root / '.download'
        self.directory = self.state / 'tasks'
        self.directory.mkdir(exist_ok=True)
        self.pools = {kind: ThreadPoolExecutor(max_workers=1, thread_name_prefix=kind) for kind in ('sha', 'range')}
        self.futures = {}
        self.stopping = Event()
        self.download_active = Event()
        self.wake = Event()
        self.retry = []
        self.ledger_path = self.state / 'transfer-state.json'
        self.ledger = json.loads(self.ledger_path.read_text()) if self.ledger_path.exists() else {}

    def checkpoint(self, item, course, attempt, phase, event=None):
        row = dict(source=self.source, size=item['size'], sha256=item['sha256'],
                   course=course, attempt=attempt, phase=phase, time=now())
        if event is not None:
            row['event'] = event
        self.ledger[item['path']] = row
        d.write_json(self.ledger_path, self.ledger)

    def continuation(self, item):
        row = self.ledger.get(item['path'], {})
        return row if (row.get('source'), row.get('size'), row.get('sha256')) == (self.source, item['size'], item['sha256']) else {}

    def submit(self, kind, item, course, attempt, event=None, force=False):
        key = hashlib.sha256(json.dumps([self.source, item['path'], item['size'], item['sha256'], kind]).encode()).hexdigest()
        path = self.directory / (key + '.json')
        if path in self.futures:
            return
        job = dict(source=self.source, kind=kind, item=item, course=course, attempt=attempt,
                   state='pending', time=now())
        if event is not None:
            job['event'] = event
        if path.exists() and not force:
            old = json.loads(path.read_text())
            if old['state'] == 'done':
                # Reapply a result saved just before interruption, but never trust changed bytes.
                local = d.target(self.root, item, course)
                result = old.get('result', {})
                if kind == 'range' or (local.exists() and result.get('stat') == self.stat(local)
                                       and not Path(str(local) + '.aria2').exists()):
                    job = old
            elif kind == 'range':
                ledger = self.state / 'recovery-state.json'
                saved = json.loads(ledger.read_text()).get(item['path'], {}) if ledger.exists() else {}
                proof = saved.get('event', {})
                if (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256']):
                    job['event'] = proof
                    if saved.get('state') == 'finished':
                        job.update(state='done', result=dict(event=proof))
        d.write_json(path, job)
        self.futures[path] = self.pools[kind].submit(self.work, path, job)
        self.futures[path].add_done_callback(lambda future: self.wake.set())

    @staticmethod
    def stat(local):
        stat = local.stat()
        return [stat.st_ino, stat.st_size, stat.st_mtime_ns]

    def work(self, path, job):
        if job['state'] == 'done':
            return job
        if self.stopping.is_set():
            raise InterruptedError('Background task interrupted')
        job['state'] = 'running'
        d.write_json(path, job)
        if job['kind'] == 'sha':
            local = d.target(self.root, job['item'], job['course'])
            before = self.stat(local) if local.exists() else None
            status, detail = d.inspect(self.root, job['item'], job['course'], stopping=self.stopping)
            after = self.stat(local) if local.exists() else None
            if status == 'valid' and before != after:
                status, detail = 'invalid', 'File changed during SHA verification'
            result = dict(status=status, detail=detail, stat=before)
        else:
            event = job['event']
            try:
                r.finish_recovery(self.root, event, job['item'], self.retries, tuning=self.tuning,
                                  stopping=self.stopping, download_active=self.download_active, background=True)
            except InterruptedError:
                raise
            except (ValueError, FileExistsError) as error:
                event['recovery_error'] = str(error)
                print('RECOVERY ERROR: ' + event['path'] + '; ' + type(error).__name__, flush=True)
                r.recovery_state(self.root, event, 'finished')
            result = dict(event=event)
        job.update(state='done', result=result, time=now())
        d.write_json(path, job)
        return job

    def archive(self, item, course, attempt, event):
        local = d.target(self.root, item, course)
        archive = self.root / 'corrupted' / datetime.now().strftime('%Y%m%d-%H%M%S-%f')
        broken = archive / local.relative_to(self.root)
        broken.parent.mkdir(parents=True, exist_ok=True)
        event['corrupted_path'] = str(broken.relative_to(self.root))
        self.checkpoint(item, course, attempt, self.continuation(item).get('phase', 'range'), event)
        # Persist the destination before moving either the body or the resume control.
        failure_manifests(self.root, event)
        self.repair_archive(item, course, event)
        return event

    def repair_archive(self, item, course, event):
        local = d.target(self.root, item, course)
        broken = r.body_path(self.root, event)
        broken.parent.mkdir(parents=True, exist_ok=True)
        for suffix in ('', '.aria2', '.ranges.json'):
            existing = Path(str(local) + suffix)
            if existing.exists():
                destination = Path(str(broken) + suffix)
                if destination.exists():
                    raise FileExistsError('Both archive and original exist: ' + str(existing))
                existing.rename(destination)

    def failed(self, item, course, attempt, event):
        if attempt <= self.retries:
            self.checkpoint(item, course, attempt, 'retry', event)
            # aria without checksums would otherwise skip a complete body with a bad SHA.
            if event['status'] == 'invalid':
                self.archive(item, course, attempt, event)
            self.retry.append((item, course, attempt))
        else:
            self.checkpoint(item, course, attempt, 'range', event)
            self.submit('range', item, course, attempt, self.archive(item, course, attempt, event), force=True)

    def poll(self):
        for path, future in list(self.futures.items()):
            if not future.done():
                continue
            job = future.result()
            item, course, attempt = job['item'], job['course'], job['attempt']
            result = job['result']
            if job['kind'] == 'sha':
                local = d.target(self.root, item, course)
                if result['status'] == 'valid' and (not local.exists() or result['stat'] != self.stat(local)
                                                   or Path(str(local) + '.aria2').exists()):
                    result.update(status='invalid', detail='File changed after SHA verification')
                event = d.record(self.state, item, course, attempt, result['status'], result['detail'])
                if event['status'] != 'valid':
                    self.failed(item, course, attempt, event)
                else:
                    self.checkpoint(item, course, attempt, 'finished', event)
            else:
                event = result['event']
                if not event.get('range_recovered_complete'):
                    with (self.state / 'events.jsonl').open('a') as log:
                        log.write(json.dumps(event, ensure_ascii=False) + '\n')
                failure_manifests(self.root, event)
                self.checkpoint(item, course, attempt, 'finished', event)
            job['state'] = 'applied'
            d.write_json(path, job)
            del self.futures[path]

    def transfer(self, pending, course, previous_attempt=0):
        if previous_attempt:
            time.sleep(self.tuning['retry_delay'])
            fresh = {item['path']: item for item in d.list_course(self.source, course, self.retries, self.tuning['listing_retry_delay'])}
            for item in pending:
                current = fresh.get(item['path'])
                if current is None or (current['size'], current['sha256']) != (item['size'], item['sha256']):
                    raise ValueError('Source changed during download retry: ' + item['path'])
                item['file'] = current['file']
        needed = sum(max(0, item['size'] - (r.progress(d.target(self.root, item, course), item['size'],
                     sha256=item['sha256']).get('downloaded_bytes') or 0)) for item in pending)
        if shutil.disk_usage(self.root).free < needed + self.tuning['free_space_mib'] * 1024 ** 2:
            raise RuntimeError('Insufficient free space for the course')
        lines = []
        for item in pending:
            local = d.target(self.root, item, course)
            local.parent.mkdir(parents=True, exist_ok=True)
            lines.append(f"{item['file']}\n dir={local.parent}\n out={local.name}\n")
            self.checkpoint(item, course, previous_attempt + 1, 'downloading')
        (self.state / 'queue.txt').write_text(''.join(lines))
        t = self.tuning
        command = ['aria2c', '--input-file=' + str(self.state / 'queue.txt'), '--continue=true',
                   '--always-resume=true', '--check-integrity=false', '--auto-file-renaming=false', '--allow-overwrite=true',
                   f"--max-concurrent-downloads={t['download_concurrency']}",
                   '--optimize-concurrent-downloads=' + (f"{t['auto_concurrency_a']:g}:{t['auto_concurrency_b']:g}" if t['auto_concurrency'] else 'false'),
                   f"--max-connection-per-server={t['connections']}", f"--split={t['split']}",
                   f"--min-split-size={t['min_split_size_mib']}M", '--file-allocation=none', f"--disk-cache={t['disk_cache_mib']}M",
                   f"--max-overall-download-limit={t['bandwidth_limit']}", f"--auto-save-interval={t['auto_save_interval']}",
                   '--max-tries=1', f"--connect-timeout={t['connect_timeout']}", f"--timeout={t['timeout']}",
                   f"--summary-interval={t['summary_interval']}", '--enable-color=false', '--console-log-level=warn',
                   '--log=' + str(self.state / 'aria2.log'), '--log-level=notice']
        self.download_active.set()
        try:
            code = d.subprocess.call(command, env=d.ENV)
        finally:
            self.download_active.clear()
        if code == 9:
            raise RuntimeError('aria2 reports insufficient disk space')
        if code in (130, -2) or self.stopping.is_set():
            raise InterruptedError('Download interrupted')
        attempt = previous_attempt + 1
        for item in pending:
            self.checkpoint(item, course, attempt, 'verifying')
            self.submit('sha', item, course, attempt, force=True)
        self.poll()

    def retry_ready(self):
        self.poll()
        if self.retry:
            course, attempt = self.retry[0][1:]
            pending = [item for item, name, number in self.retry if (name, number) == (course, attempt)]
            self.retry = [row for row in self.retry if row[1:] != (course, attempt)]
            self.transfer(pending, course, attempt)

    def close(self):
        self.stopping.set()
        for pool in self.pools.values():
            pool.shutdown(wait=True, cancel_futures=True)


def run(data, courses, root, retries, report_repo, mark, git_push, retry_check, tuning):
    tuning = resolve(tuning)
    if not all(shutil.which(name) for name in ('aria2c', 'cp')):
        raise RuntimeError('Install aria2c and coreutils first')
    root.mkdir(parents=True, exist_ok=True)
    state = root / '.download'
    state.mkdir(mode=0o700, exist_ok=True)
    with (state / 'download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source = data['source']
        identity = state / 'source.json'
        if identity.exists() and json.loads(identity.read_text())['source'] != source:
            raise ValueError('Use a separate destination for a different public folder/year')
        d.write_json(identity, dict(source=source, title=data['title']))
        d.write_json(state / 'settings.json', dict(retries=retries, deep=False, retry_check=retry_check, tuning=tuning))
        events = {row['path']: row for row in d.read_events(state / 'events.jsonl', repair=True)}
        history_path = state / 'course-history.json'
        history = json.loads(history_path.read_text()) if history_path.exists() else {}
        for path in state.glob('*-status.json'):
            old = json.loads(path.read_text())
            if old.get('state') == 'running' and not old.get('time'):
                old['time'] = datetime.fromtimestamp(path.stat().st_mtime).astimezone().isoformat()
            if old.get('source') == source and old.get('time', '') >= history.get(old['course'], {}).get('time', ''):
                history[old['course']] = old
        registry = state / 'local-copies.json'
        for row in json.loads(registry.read_text()) if registry.exists() else []:
            local = Path(row['local'])
            if (row['source'].rstrip('/') == source.rstrip('/') and row['status'] == 'valid' and local.exists()
                    and (local.stat().st_size, local.stat().st_mtime_ns) == (row['size'], row['mtime_ns'])
                    and not Path(str(local) + '.aria2').exists()):
                events[row['path']] = row
        candidates = {}
        for manifest in (root / 'corrupted').glob('*/failed.json'):
            for row in json.loads(manifest.read_text()):
                if row['course'] not in courses:
                    continue
                row = dict(row, corrupted_path=row.get('corrupted_path') or str(local_path(manifest.parent, row['path']).relative_to(root)))
                amount = r.progress(r.body_path(root, row), row['size'], sha256=row['sha256']).get('downloaded_bytes') or 0
                rank = (not row.get('sha256_mismatch', False), amount, row['time'])
                if row['path'] not in candidates or rank > candidates[row['path']][0]:
                    candidates[row['path']] = rank, row
        d.write_json(state / 'catalog.json', data)
        d.write_json(state / 'courses.json', courses)
        lines = [data['title'] + ' — порядок скачивания', 'Источник: ' + source, '']
        for index, course in enumerate(courses, 1):
            lines += [f'{index:02d}. {course}', source.rstrip('/') + '/' + d.urllib.parse.quote(course, safe=''), '']
        (root / 'Порядок скачивания.txt').write_text('\n'.join(lines))
        background = Background(root, source, retries, tuning)
        active = {}

        def finalize():
            background.poll()
            proofs = {row['path']: row for row in d.read_events(state / 'events.jsonl')}
            for course, (index, entries) in list(active.items()):
                jobs = [json.loads(path.read_text()) for path in background.futures if json.loads(path.read_text())['course'] == course]
                waiting = bool(jobs or any(name == course for _, name, _ in background.retry))
                valid = 0
                for item in entries:
                    for proof in (proofs.get(item['path'], {}), events.get(item['path'], {})):
                        local = Path(proof['local']) if proof.get('local') else d.target(root, item, course)
                        if (proof.get('status') == 'valid' and (proof.get('size'), proof.get('sha256')) == (item['size'], item['sha256'])
                                and local.exists() and local.stat().st_size == item['size']
                                and ('mtime_ns' not in proof or local.stat().st_mtime_ns == proof['mtime_ns'])
                                and not Path(str(local) + '.aria2').exists()):
                            valid += 1
                            break
                status = dict(course=course, source=source, state=('recovering' if any(job['kind'] == 'range' for job in jobs) else 'verifying') if waiting else 'finished',
                              complete=not waiting and valid == len(entries), total=len(entries), verified=valid,
                              failed=0 if waiting else len(entries) - valid, pending=len(entries) - valid if waiting else 0, time=now())
                if {key: value for key, value in history.get(course, {}).items() if key != 'time'} != {key: value for key, value in status.items() if key != 'time'}:
                    history[course] = status
                    d.write_json(state / f'{index:02d}-status.json', status)
                    d.write_json(history_path, history)
                if not waiting:
                    del active[course]
                    print(('COURSE VERIFIED COMPLETE: ' if status['complete'] else 'COURSE FINISHED WITH ERRORS: ') + course, flush=True)
                    if report_repo:
                        try:
                            from report import update_report
                            update_report(root, report_repo, mark, git_push, f"Update {mark} copies: {data['title']} / {course}")
                        except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
                            print(f'REPORT ERROR: {error}', flush=True)
            summary = dict(verified=sum(history.get(course, {}).get('verified', 0) for course in courses),
                         failed=sum(history.get(course, {}).get('failed', 0) for course in courses),
                         pending_courses=list(active), complete=False)
            summary_path = state / 'summary.json'
            if not summary_path.exists() or json.loads(summary_path.read_text()) != summary:
                d.write_json(summary_path, summary)

        try:
            for index, course in enumerate(courses, 1):
                background.retry_ready()
                finalize()
                previous = history.get(course, {})
                if previous.get('state') == 'finished' and previous.get('source') == source and not (retry_check and previous.get('failed')):
                    d.write_json(state / f'{index:02d}-status.json', previous)
                    print('COURSE SKIPPED: ' + course, flush=True)
                    continue
                print(f'COURSE {index}/{len(courses)}: {course}', flush=True)
                history[course] = dict(previous, course=course, source=source, state='running', complete=False, time=now())
                d.write_json(history_path, history)
                d.write_json(state / f'{index:02d}-status.json', history[course])
                entries = d.list_course(source, course, retries, tuning['listing_retry_delay'])
                for item in entries:
                    d.target(root, item, course)
                old_entries = {}
                for manifest in sorted(state.glob('*-manifest.json'), key=lambda path: path.stat().st_mtime_ns):
                    old_entries.update({row['path']: row for row in json.loads(manifest.read_text())})
                d.bind_resume(root, entries, old_entries, events, False)
                d.write_json(state / f'{index:02d}-manifest.json', [{key: item[key] for key in ('path', 'size', 'sha256')} for item in entries])
                active[course] = index, entries
                pending = []
                for item in entries:
                    cached = events.get(item['path'], {})
                    local = Path(cached['local']) if cached.get('local') and cached.get('status') == 'valid' else d.target(root, item, course)
                    if (cached.get('status') == 'valid' and (cached.get('size'), cached.get('sha256')) == (item['size'], item['sha256'])
                            and local.exists() and local.stat().st_size == item['size']
                            and ('mtime_ns' not in cached or local.stat().st_mtime_ns == cached['mtime_ns'])
                            and not Path(str(local) + '.aria2').exists()):
                        continue
                    archived = candidates.get(item['path'], (None, None))[1]
                    continuation = background.continuation(item)
                    if continuation.get('phase') in ('retry', 'downloading'):
                        attempt = continuation['attempt']
                        event = continuation.get('event')
                        if continuation['phase'] == 'retry' and event and event['status'] == 'invalid':
                            if event.get('corrupted_path'):
                                background.repair_archive(item, course, event)
                            else:
                                background.archive(item, course, attempt, dict(event))
                        background.retry.append((item, course, attempt if continuation['phase'] == 'retry' else attempt - 1))
                    elif continuation.get('phase') == 'verifying':
                        background.submit('sha', item, course, continuation['attempt'])
                    elif continuation.get('phase') == 'range':
                        event = continuation['event']
                        if not event.get('corrupted_path'):
                            event = background.archive(item, course, continuation['attempt'], event)
                        else:
                            background.repair_archive(item, course, event)
                        background.submit('range', item, course, continuation['attempt'], event, force=retry_check)
                    elif (archived and (retry_check or previous.get('state') in ('running', 'verifying', 'recovering'))
                          and (archived['size'], archived['sha256']) == (item['size'], item['sha256'])):
                        ledger_path = state / 'recovery-state.json'
                        ledger = json.loads(ledger_path.read_text()).get(item['path'], {}) if ledger_path.exists() else {}
                        old = ledger.get('event', {})
                        same = (old.get('size'), old.get('sha256')) == (item['size'], item['sha256'])
                        if same:
                            archived = old
                        if not retry_check and (same and ledger.get('state') == 'finished' or not ledger and archived.get('range_attempts')):
                            if not ledger:
                                r.recovery_state(root, archived, 'finished')
                            with (state / 'events.jsonl').open('a') as log:
                                log.write(json.dumps(archived, ensure_ascii=False) + '\n')
                        else:
                            background.repair_archive(item, course, archived)
                            background.submit('range', item, course, archived['attempt'], archived, force=retry_check)
                    elif local.exists() and local.stat().st_size == item['size'] and not Path(str(local) + '.aria2').exists():
                        background.submit('sha', item, course, 0)
                    else:
                        pending.append(item)
                if pending:
                    background.transfer(pending, course)
                finalize()
            while background.futures or background.retry:
                background.retry_ready()
                finalize()
                if background.futures and not background.retry:
                    background.wake.wait(1)
                    background.wake.clear()
            finalize()
        finally:
            background.close()
        failed = sum(history[course]['failed'] for course in courses)
        verified = sum(history[course]['verified'] for course in courses)
        d.write_json(state / 'summary.json', dict(verified=verified, failed=failed, pending_courses=[], complete=not failed))
        print(f'ALL SELECTED COURSES PROCESSED: verified={verified}, failed={failed}', flush=True)
        return 1 if failed else 0
