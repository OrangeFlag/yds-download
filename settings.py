"""Shared CLI and semester-queue tuning; sizes have explicit binary units."""
import math
import argparse
from pathlib import Path

# name: default, minimum, maximum (None = unbounded), description
OPTIONS = {
    'download_concurrency': (2, 1, None, 'Maximum simultaneous files'),
    'auto_concurrency': (False, None, None, 'Let aria2 adapt file concurrency within the maximum'),
    'auto_concurrency_a': (1.0, 1, None, 'aria2 concurrency coefficient A'),
    'auto_concurrency_b': (2.0, 0, None, 'aria2 concurrency coefficient B'),
    'connections': (4, 1, 16, 'Connections per server per file'),
    'split': (4, 1, None, 'Download splits per file'),
    'min_split_size_mib': (4, 1, 1024, 'Minimum aria2 split size, MiB'),
    'disk_cache_mib': (256, 0, None, 'aria2 disk cache, MiB; 0 disables'),
    'bandwidth_limit': (0, 0, None, 'Total aria2 limit, bytes/second; 0 unlimited'),
    'auto_save_interval': (30, 1, None, 'Resume metadata save interval, seconds'),
    'connect_timeout': (20, 1, None, 'aria2 connection timeout, seconds'),
    'rpc_timeout': (30.0, 0.1, None, 'Local aria2 RPC timeout, seconds'),
    'rpc_status_retries': (2, 0, None, 'Extra read-only aria2 status retries; never repeats addUri'),
    'timeout': (60, 1, None, 'aria2 timeout, seconds'),
    'metrics_interval': (60, 1, None, 'Service metrics interval, seconds'),
    'summary_interval': (30, 1, None, 'aria2 progress interval, seconds'),
    'retry_delay': (10, 0, None, 'Delay between download retries, seconds'),
    'listing_retry_delay': (60, 0, None, 'Delay between course-listing retries, seconds'),
    'free_space_mib': (1024, 0, None, 'Free space to retain in addition to required bytes, MiB'),
    'range_workers': (2, 1, None, 'Simultaneous Range requests'),
    'range_rps': (12.0, 0.01, None, 'Range requests/second per recovery'),
    'range_timeout': (30, 1, None, 'Range request timeout, seconds'),
    'range_retry_delay': (10, 0, None, 'Delay between Range passes, seconds'),
    'range_block_size_kib': (16384, 1, None, 'Initial Range block, KiB'),
    'range_min_block_size_kib': (1024, 1, None, 'Switch to linear probing at this block size, KiB'),
    'range_linear_block_size_kib': (64, 1, None, 'Linear probe block size, KiB'),
    'range_failure_limit': (32, 0, None, 'Switch to linear after more failed ranges than this'),
    'range_linear_probes': (256, 1, None, 'Maximum scatter/expansion/linear requests per recovery'),
    'range_max_passes': (0, 0, None, 'Maximum Range passes; 0 = min(4, 2*retries)+2'),
}


def resolve(values=None):
    values = {} if values is None else values
    if not isinstance(values, dict) or values.keys() - OPTIONS.keys():
        raise ValueError('Unknown or invalid tuning parameters')
    result = {}
    for name, (default, minimum, maximum, _) in OPTIONS.items():
        value = values.get(name, default)
        kind = type(default)
        if kind is bool:
            if not isinstance(value, bool):
                raise ValueError(f'Invalid {name}: expected bool')
            result[name] = value
            continue
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or (kind is int and not isinstance(value, int))
                or value < minimum or (maximum is not None and value > maximum)):
            raise ValueError(f'Invalid {name}: expected {kind.__name__} >= {minimum}' +
                             (f' and <= {maximum}' if maximum is not None else ''))
        result[name] = value
    if not (result['range_linear_block_size_kib'] <= result['range_min_block_size_kib']
            <= result['range_block_size_kib']):
        raise ValueError('Range sizes must satisfy linear <= minimum <= initial')
    return result


def add_arguments(parser):
    group = parser.add_argument_group('Download and Range tuning')
    for name, (default, _, _, help_text) in OPTIONS.items():
        flag = '--' + name.replace('_', '-')
        if isinstance(default, bool):
            group.add_argument(flag, action=argparse.BooleanOptionalAction, default=default,
                               help=f'{help_text} (default: {default})')
        else:
            group.add_argument(flag, type=type(default), default=default,
                               help=f'{help_text} (default: {default:g})')


def arguments(values):
    result = []
    for name, value in resolve(values).items():
        flag = name.replace('_', '-')
        result.append(('--' if value else '--no-') + flag if isinstance(value, bool)
                      else '--' + flag + '=' + str(value))
    return result


def add_source_arguments(parser):
    group = parser.add_argument_group('Verified cloud fallback and adaptive downloads')
    group.add_argument('--opencloud-fallback', help='HTTPS OpenCloud server origin for failed course originals')
    group.add_argument('--opencloud-username', help='OpenCloud account username')
    group.add_argument('--opencloud-password-file', type=Path, help='OpenCloud password file; only its path is persisted')
    group.add_argument('--download-budget', type=Path, help='Shared measured download budget directory')
    group.add_argument('--download-min-files', type=int, default=2, help='Minimum total simultaneous files across all download sources (default: 2)')
    group.add_argument('--download-max-files', type=int, default=8, help='Maximum total simultaneous files across all download sources (default: 8)')


def validate_source_settings(opencloud=None, budget=None):
    from urllib.parse import urlsplit
    if opencloud is not None:
        if (not isinstance(opencloud, dict) or set(opencloud) != {'server', 'username', 'password_file'}
                or any(not isinstance(opencloud[key], (str, Path)) or not str(opencloud[key]).strip()
                       or any(ord(char) < 32 for char in str(opencloud[key])) for key in opencloud)):
            raise ValueError('OpenCloud requires server, username and password file')
        server = str(opencloud['server'])
        parsed = urlsplit(server)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment
                or any(char.isspace() for char in server)):
            raise ValueError('Expected an HTTPS OpenCloud server origin')
        parsed.port  # Validate the port before any transfer.
        if ':' in str(opencloud['username']):
            raise ValueError('OpenCloud username cannot contain a colon')
        opencloud = dict(server=server.rstrip('/'), username=str(opencloud['username']),
                         password_file=str(Path(opencloud['password_file']).expanduser().resolve()))
    if budget is not None:
        if (not isinstance(budget, dict) or set(budget) != {'directory', 'minimum', 'maximum'}
                or not isinstance(budget['directory'], (str, Path)) or not str(budget['directory']).strip()
                or any(ord(char) < 32 for char in str(budget['directory']))
                or any(type(budget[key]) is not int for key in ('minimum', 'maximum'))
                or not 1 <= budget['minimum'] <= budget['maximum']):
            raise ValueError('Download file limits must satisfy 1 <= minimum <= maximum')
        budget = dict(budget, directory=str(Path(budget['directory']).expanduser().resolve()))
    return opencloud, budget


def source_settings(args):
    values = (args.opencloud_fallback, args.opencloud_username, args.opencloud_password_file)
    if any(value is not None for value in values) and not all(values):
        raise ValueError('--opencloud-fallback requires --opencloud-username and --opencloud-password-file')
    if not 1 <= args.download_min_files <= args.download_max_files:
        raise ValueError('Download file limits must satisfy 1 <= minimum <= maximum')
    cloud = dict(zip(('server', 'username', 'password_file'), values)) if all(values) else None
    budget = (dict(directory=args.download_budget, minimum=args.download_min_files, maximum=args.download_max_files)
              if args.download_budget is not None else None)
    return validate_source_settings(cloud, budget)


def source_arguments(opencloud=None, budget=None):
    opencloud, budget = validate_source_settings(opencloud, budget)
    result = []
    if opencloud:
        result += ['--opencloud-fallback', opencloud['server'], '--opencloud-username', opencloud['username'],
                   '--opencloud-password-file', opencloud['password_file']]
    if budget:
        result += ['--download-budget', budget['directory'], '--download-min-files', str(budget['minimum']),
                   '--download-max-files', str(budget['maximum'])]
    return result
