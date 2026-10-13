"""Wait through a local connection outage without spending file retries."""
import errno
import http.client
import json
import socket
import time
from threading import Event, Lock
import urllib.error


class NetworkGuard:
    def __init__(self):
        self.lock = Lock()
        self.generation = 0
        self.probe_errors = []
        self.probe_success = 'unknown'

    def online(self):
        self.probe_errors = []
        self.probe_success = 'unknown'
        for host in ('cloud-api.yandex.net', 'api.github.com'):
            started = time.monotonic()
            try:
                with socket.create_connection((host, 443), timeout=2, all_errors=True):
                    self.probe_success = f'{host}:443; probe={time.monotonic() - started:.3f}s'
                    return True
            except (OSError, ExceptionGroup) as error:
                failures = error.exceptions if isinstance(error, ExceptionGroup) else [error]
                details = []
                for failure in failures:
                    number = getattr(failure, 'errno', None)
                    name = errno.errorcode.get(number, type(failure).__name__)
                    code = f'{name}({number})' if number is not None else name
                    details.append(f"{code}: {getattr(failure, 'strerror', None) or str(failure)}")
                self.probe_errors.append(f"{host}:443 [{'; '.join(details)}]; probe={time.monotonic() - started:.3f}s")
        return False

    def wait_if_offline(self, stopping=None, *, force=True):
        stopping = stopping if stopping is not None else Event()
        generation = self.generation
        while not self.lock.acquire(timeout=.25):
            if stopping.is_set():
                raise InterruptedError('Network wait interrupted')
        try:
            if stopping.is_set():
                raise InterruptedError('Network wait interrupted')
            if self.online():
                return generation != self.generation
            self.generation += 1
            started = time.monotonic()
            print('NETWORK PROBE FAILED: ' + '; '.join(self.probe_errors) + '; file retries retained', flush=True)
            delay, rechecks = 0, 0
            while True:
                if stopping.wait(delay):
                    raise InterruptedError('Network wait interrupted')
                rechecks += 1
                if self.online():
                    print(f'NETWORK PROBE OK: {self.probe_success}; waited={time.monotonic() - started:.3f}s; rechecks={rechecks}; retrying interrupted requests', flush=True)
                    return True
                delay = min(5, delay * 2 if delay else .1)
        finally:
            self.lock.release()


GUARD = NetworkGuard()


def wait_if_offline(stopping=None, *, force=True):
    return GUARD.wait_if_offline(stopping, force=force)


def retry_while_offline(operation, stopping=None):
    while True:
        try:
            return operation()
        except (OSError, http.client.HTTPException, json.JSONDecodeError, UnicodeDecodeError) as error:
            connection = (not isinstance(error, urllib.error.HTTPError)
                          and (isinstance(error, (urllib.error.URLError, TimeoutError, ConnectionError, socket.gaierror,
                                                  http.client.HTTPException, json.JSONDecodeError, UnicodeDecodeError))
                               or getattr(error, 'errno', None) in (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN)))
            if not connection or not wait_if_offline(stopping):
                raise
