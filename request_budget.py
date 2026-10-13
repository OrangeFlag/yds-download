"""A crash-safe HTTP concurrency and start-rate budget shared by processes."""
from contextlib import contextmanager
import fcntl
import json
import math
from pathlib import Path
import time


class ProcessRangeBudget:
    def __init__(self, directory, requests=12, rps=12):
        if requests < 1 or not math.isfinite(rps) or rps <= 0:
            raise ValueError('Range request limits must be positive')
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.requests, self.interval = requests, 1 / rps
        with (self.directory/'config.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            config = self.directory/'config.json'
            expected = dict(requests=requests, rps=rps)
            if config.exists():
                if json.loads(config.read_text()) != expected:
                    raise ValueError('Shared Range budget limits differ between processes')
            else:
                from download import write_json
                write_json(config, expected)

    @contextmanager
    def request(self, stopping):
        slot = None
        while slot is None:
            if stopping.is_set():
                raise InterruptedError('Range interrupted')
            for index in range(self.requests):
                candidate = (self.directory/f'{index}.slot').open('a')
                try:
                    fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    candidate.close()
                else:
                    slot = candidate
                    break
            if slot is None:
                stopping.wait(.05)
        try:
            # Never replace this inode: all processes must lock the same rate state.
            with (self.directory/'rate.lock').open('a+') as rate:
                while True:
                    try:
                        fcntl.flock(rate, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if stopping.wait(.05):
                            raise InterruptedError('Range interrupted')
                rate.seek(0)
                previous = rate.read().strip()
                delay = min(self.interval, max(0, float(previous or 0) - time.monotonic()))
                if stopping.wait(delay):
                    raise InterruptedError('Range interrupted')
                rate.seek(0); rate.truncate()
                rate.write(str(time.monotonic() + self.interval)); rate.flush()
            yield
        finally:
            slot.close()
