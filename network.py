"""Retry real requests using their causes and recent request outcomes."""
import errno
from email.utils import parsedate_to_datetime
from functools import partial
import http.client
import json
import re
import socket
import subprocess
import time
from threading import Event, Lock
import urllib.error
import urllib.request


def retry_after(headers):
    value = (headers or {}).get('Retry-After', '')
    try:
        if value.strip().isdigit():
            return int(value)
        return max(0, parsedate_to_datetime(value).timestamp() - time.time())
    except (ValueError, TypeError, AttributeError, OverflowError):
        return 0


def failure(error, phase='unknown'):
    phase = getattr(error, 'network_phase', phase)
    if isinstance(error, (http.client.IncompleteRead, json.JSONDecodeError, UnicodeDecodeError)):
        phase = 'body'
    if isinstance(error, urllib.error.HTTPError):
        return dict(category='http', phase='headers', error_type='HTTPError', errno=None,
                    http_status=error.code, retry_after=retry_after(error.headers))
    if isinstance(error, urllib.error.URLError) and isinstance(error.reason, BaseException):
        error = error.reason
    number = getattr(error, 'errno', None)
    category = 'other'
    if number in (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN) or (
            isinstance(error, socket.gaierror) and number == socket.EAI_AGAIN):
        category = 'routing'
    elif isinstance(error, (TimeoutError, ConnectionError, http.client.HTTPException,
                            json.JSONDecodeError, UnicodeDecodeError, urllib.error.URLError)):
        category = 'ambiguous'
    elif isinstance(error, (str, subprocess.CalledProcessError, subprocess.TimeoutExpired)):
        text = error if isinstance(error, str) else (error.stderr or error.output or '')
        if isinstance(text, bytes):
            text = text.decode('utf8', errors='replace')
        if re.search(r'\beof\b|incomplete response', text, re.I):
            phase = 'body'
        status = re.search(r'(?:status=|HTTP(?: Error)?[ :]+)([45][0-9]{2})\b', text)
        if status:
            return dict(category='http', phase='headers', error_type='HTTPError', errno=None,
                        http_status=int(status[1]), retry_after=0)
        for phrase, code in (('No route to host', errno.EHOSTUNREACH),
                             ('Network is unreachable', errno.ENETUNREACH),
                             ('Network is down', errno.ENETDOWN)):
            if phrase.lower() in text.lower():
                category, number = 'routing', code
                break
        else:
            if isinstance(error, subprocess.TimeoutExpired) or re.search(
                    r'timeout|timed out|\beof\b|incomplete response|connection reset|connection aborted', text, re.I):
                category = 'ambiguous'
    return dict(category=category, phase=phase, error_type=type(error).__name__, errno=number,
                http_status=None)


def connect(*args, **kwargs):
    try:
        return socket.create_connection(*args, **kwargs, all_errors=True)
    except ExceptionGroup as errors:
        # An unrouted IPv6 address must not hide an IPv4 timeout or refusal.
        for error in errors.exceptions:
            if failure(error)['category'] != 'routing':
                raise error
        raise errors.exceptions[0]


def connection(factory, *args, **kwargs):
    result = factory(*args, **kwargs)
    result._create_connection = connect
    return result


class HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, request):
        return self.do_open(partial(connection, http.client.HTTPConnection), request)


class HTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, request):
        return self.do_open(partial(connection, http.client.HTTPSConnection), request, context=self._context)


class NetworkGuard:
    def __init__(self):
        self.lock = Lock()
        self.failures = {}
        self.last_success = float('-inf')

    def observe(self, target, error=None):
        with self.lock:
            now = time.monotonic()
            self.failures = {key: stamp for key, stamp in self.failures.items() if now-stamp <= 30}
            if error is None or error['category'] == 'http':
                self.last_success = now
                self.failures.pop(target, None)
            elif error['category'] == 'routing' or (error['category'] == 'ambiguous' and error['phase'] != 'body'):
                self.failures[target] = now
                if len(self.failures) > 256:
                    del self.failures[min(self.failures, key=self.failures.get)]
            return (len(self.failures) >= 3
                    and self.last_success < min(self.failures.values()))


GUARD = NetworkGuard()


class RequestRetry:
    def __init__(self, target, stopping=None, confirmation_retries=2):
        self.target = target
        self.stopping = stopping if stopping is not None else Event()
        self.retries = self.confirmations = 0
        self.confirmation_retries = confirmation_retries
        self.delay = self.last_log = 0

    def retry(self, error):
        if self.stopping.is_set():
            raise InterruptedError('Request retry interrupted')
        correlated = GUARD.observe(self.target, error)
        category = error['category']
        confirmed = category == 'routing' or category == 'ambiguous' and error['phase'] != 'body' and correlated
        limited = category == 'ambiguous' or category == 'http' and error['http_status'] in (429, 503)
        if not confirmed:
            if not limited or self.confirmations >= self.confirmation_retries:
                return False
            self.confirmations += 1
        delay = max(self.delay, error.get('retry_after', 0))
        self.retries += 1
        now = time.monotonic()
        if self.retries == 1 or now-self.last_log >= 30:
            number = error['errno']
            code = f'{errno.errorcode.get(number, number)}({number})' if number is not None else 'none'
            reason = 'correlated transport failures' if confirmed and category != 'routing' else category
            print(f'REQUEST RETRY: reason={reason}; phase={error["phase"]}; errno={code}; '
                  f'http={error["http_status"]}; retries={self.retries}; pause={delay:g}s; file retries retained', flush=True)
            self.last_log = now
        if self.stopping.wait(delay):
            raise InterruptedError('Request retry interrupted')
        self.delay = min(5, self.delay*2 if self.delay else .1)
        return True


def retry_request(operation, stopping=None, *, target='request', confirmation_retries=2):
    retry = RequestRetry(target, stopping, confirmation_retries)
    while True:
        if retry.stopping.is_set():
            raise InterruptedError('Request interrupted')
        try:
            result = operation()
            GUARD.observe(target)
            return result
        except (OSError, http.client.HTTPException, json.JSONDecodeError, UnicodeDecodeError) as error:
            # HTTP retries belong to the caller's existing server retry budget.
            if isinstance(error, urllib.error.HTTPError):
                GUARD.observe(target, failure(error))
                raise
            if not retry.retry(failure(error)):
                raise
