"""Minute telemetry from task results and resume metadata, without reading bodies."""
from collections import Counter, deque
from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
from threading import Event, RLock, Thread
import time
import uuid

import network

try:
    REVISION = subprocess.check_output(['git','-C',str(Path(__file__).parent),'rev-parse','--short','HEAD'],
                                       stderr=subprocess.DEVNULL,text=True).strip()
except (OSError, subprocess.SubprocessError):
    REVISION = 'unknown'


class Metrics:
    def __init__(self, path, role, interval=60, settings=None, start=True):
        if interval <= 0:
            raise ValueError('Metrics interval must be positive')
        self.path, self.role, self.interval = Path(path), role, interval
        self.settings = settings or {}
        self.run_id = uuid.uuid4().hex
        self.lock, self.snapshot_lock, self.stop = RLock(), RLock(), Event()
        self.counters, self.previous = Counter(), Counter()
        self.errors, self.gauges, self.tasks = {}, {}, {}
        self.first_bytes = deque(maxlen=1024)
        self.sample = lambda: None
        self.previous_time = time.monotonic()
        self.disk_name = self.disk_device()
        self.gauges['disk_device'] = self.disk_name
        self.host_previous = self.host()
        self.warned = self.closed = False
        self.thread = None
        if start:
            self.thread = Thread(target=self.loop, name='metrics', daemon=True)
            self.thread.start()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def add(self, **values):
        with self.lock:
            self.counters.update(values)

    def gauge(self, name, value):
        with self.lock:
            self.gauges[name] = value

    def task(self, key, kind, state, queued_at):
        with self.lock:
            if state == 'applied':
                self.tasks.pop(str(key), None)
            else:
                self.tasks[str(key)] = kind, state, queued_at

    def range_request(self, row):
        with self.lock:
            self.add(range_requests=1, range_received_bytes=row['saved_bytes'],
                     range_request_seconds=row['request_seconds'], range_io_seconds=row['save_seconds'],
                     range_wait_seconds=max(0,row['seconds']-row['request_seconds']))
            self.add(range_http_attempts=row.get('request_attempts', 1))
            for reason, count in row.get('retry_reasons', {}).items():
                self.add(**{'range_retries_' + reason: count})
            self.errors.setdefault('range_causes', Counter()).update(row.get('request_errors', {}))
            if row['error']:
                self.errors.setdefault('range',Counter()).update([row['error']])
            if row['first_byte_seconds'] is not None:
                self.first_bytes.append(row['first_byte_seconds'])

    def range_pass(self, outcomes, seconds, count_requests=True):
        with self.lock:
            self.add(range_passes=1, range_zero_byte_passes=int(not any(r['saved_bytes'] for r in outcomes)),
                     range_pass_seconds=seconds)
            if count_requests:
                for row in outcomes:
                    self.range_request(row)

    def disk_device(self):
        try:
            dev = self.path.parent.stat().st_dev
            device = (Path('/sys/dev/block')/f'{os.major(dev)}:{os.minor(dev)}').resolve()
            if not device.exists():
                # Btrfs st_dev is virtual; mountinfo names its actual backing device.
                destination = str(self.path.parent.resolve())
                closest = (0,None)
                for row in Path('/proc/self/mountinfo').read_text().splitlines():
                    fields, filesystem = row.split(' - ',1)
                    mount = re.sub(r'\\([0-7]{3})',lambda m:chr(int(m[1],8)),fields.split()[4])
                    if destination == mount or destination.startswith(mount.rstrip('/')+'/'):
                        if len(mount) > closest[0]:
                            closest = len(mount),filesystem.split()[1]
                source = closest[1]
                if not source or not source.startswith('/dev/'):
                    return None
                device = (Path('/sys/class/block')/Path(source).resolve().name).resolve()
            return device.parent.name if (device/'partition').exists() else device.name
        except (OSError, ValueError, IndexError):
            return None

    def host(self):
        result = {}
        for interface in Path('/sys/class/net').glob('*'):
            if (interface/'device').exists():
                try:
                    result['rx_bytes:'+interface.name] = int((interface/'statistics/rx_bytes').read_text())
                    result['tx_bytes:'+interface.name] = int((interface/'statistics/tx_bytes').read_text())
                except OSError:
                    pass
        try:
            for row in Path('/proc/diskstats').read_text().splitlines():
                fields = row.split()
                if fields[2] == self.disk_name:
                    result.update(disk_read_bytes=int(fields[5])*512, disk_write_bytes=int(fields[9])*512,
                                  disk_busy_ms=int(fields[12]))
                    break
            cpu = os.times()
            result['process_cpu_seconds'] = cpu.user + cpu.system
        except (OSError, ValueError):
            pass
        return result

    def snapshot(self, reason='periodic'):
        try:
            with self.snapshot_lock:
                self.sample()
                now = time.monotonic(); elapsed = max(.001,now-self.previous_time)
                host = self.host()
                rates = {key:(value-self.host_previous[key])/elapsed for key,value in host.items()
                         if key in self.host_previous and value >= self.host_previous[key]}
                with self.lock:
                    queue = Counter(); oldest = {}
                    for kind, state, queued in self.tasks.values():
                        queue[kind+':'+state] += 1
                        if state == 'pending':
                            try:
                                age = max(0,time.time()-datetime.fromisoformat(queued).timestamp())
                                oldest[kind] = max(oldest.get(kind,0),age)
                            except (ValueError, TypeError):
                                pass
                    first = sorted(self.first_bytes)
                    row = dict(time=datetime.now().astimezone().isoformat(), role=self.role, reason=reason, pid=os.getpid(),
                               run_id=self.run_id, revision=REVISION, settings=self.settings,
                               seconds=elapsed, counters=dict(self.counters),
                               interval_counters={k:v-self.previous.get(k,0) for k,v in self.counters.items()},
                               errors=deepcopy(self.errors), gauges=deepcopy(self.gauges), tasks=queue, oldest_pending_seconds=oldest,
                               first_byte_p95_last_1024=first[min(len(first)-1,int(len(first)*.95))] if first else None,
                               host_rates=rates)
                self.path.parent.mkdir(parents=True,exist_ok=True)
                with self.path.open('a',encoding='utf-8') as stream:
                    stream.write(json.dumps(row,ensure_ascii=False)+'\n')
                latest = self.path.with_name(self.path.stem+'-latest.json')
                temporary = latest.with_name(latest.name+'.tmp')
                temporary.write_text(json.dumps(row,ensure_ascii=False,indent=2),encoding='utf-8')
                temporary.replace(latest)
                self.previous = row['counters']; self.previous_time = now; self.host_previous = host
        except (OSError, ValueError, KeyError, TypeError) as error:
            if not self.warned:
                print('METRICS WARNING: '+type(error).__name__,flush=True)
                self.warned = True

    def loop(self):
        while not self.stop.wait(self.interval):
            self.snapshot()

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
        self.snapshot('shutdown')


class AriaMetrics:
    def __init__(self, path, metrics):
        self.path, self.metrics = Path(path), metrics
        try:
            stat = self.path.stat()
            self.offset, self.inode = stat.st_size, stat.st_ino
        except OSError:
            self.offset, self.inode = 0, None
        self.buffer = b''
        self.files = {}
        self.lock = RLock()

    def begin(self, files):
        with self.lock:
            self.sample()
            self.files = {path:dict(size=size, seen=received, complete=False) for path,(size,received) in files.items()}

    def received(self, entry, amount):
        if entry['seen'] is None:
            self.metrics.add(aria_initial_unmeasured_bytes=amount)
            entry['seen'] = amount
        elif amount > entry['seen']:
            network.GUARD.observe(str(id(entry)))
            self.metrics.add(aria_received_bytes=amount-entry['seen'])
            entry['seen'] = amount

    def sample(self):
        with self.lock:
            self._sample()

    def _sample(self):
        from progress import received_bytes
        try:
            stat = self.path.stat()
            if stat.st_ino != self.inode or stat.st_size < self.offset:
                self.offset = 0; self.buffer = b''
            self.inode = stat.st_ino
            with self.path.open('rb') as stream:
                stream.seek(self.offset); data=stream.read(); self.offset=stream.tell()
            lines = (self.buffer+data).split(b'\n'); self.buffer=lines.pop()
            for raw in lines:
                line = raw.decode('utf-8',errors='replace')
                if 'Download complete: ' in line:
                    path = line.partition('Download complete: ')[2].strip()
                    entry = self.files.get(path)
                    if entry and not entry['complete']:
                        self.received(entry,entry['size']); entry['complete']=True
                        self.metrics.add(aria_completed_files=1,aria_completed_bytes=entry['size'])
                if 'Download aborted.' in line:
                    self.metrics.add(aria_aborted_files=1)
                codes = re.findall(r'errorCode=(\d+)',line)
                if codes:
                    with self.metrics.lock:
                        self.metrics.errors.setdefault('aria_error_messages',Counter()).update(codes)
            self.metrics.gauge('aria_log_backlog_bytes',max(0,stat.st_size-self.offset))
        except OSError:
            pass
        active = 0
        for path,entry in self.files.items():
            if entry['complete']:
                continue
            control = Path(path+'.aria2')
            if control.exists():
                active += 1
                try:
                    self.received(entry,received_bytes(control,entry['size']))
                except (OSError, ValueError):
                    pass
        self.metrics.gauge('aria_files_with_resume_control',active)
