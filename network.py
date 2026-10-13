"""Wait through a local connection outage without spending file retries."""
import errno
import http.client
import json
import socket
from threading import Event, Lock
import urllib.error


class NetworkGuard:
    def __init__(self):
        self.lock = Lock()
        self.generation = 0

    @staticmethod
    def online():
        for host in ('cloud-api.yandex.net', 'api.github.com'):
            try:
                with socket.create_connection((host, 443), timeout=2):
                    return True
            except OSError:
                pass
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
            print('NETWORK OFFLINE: waiting; file retries retained', flush=True)
            while True:
                if stopping.wait(5):
                    raise InterruptedError('Network wait interrupted')
                if self.online():
                    print('NETWORK ONLINE: resuming interrupted requests', flush=True)
                    return True
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
