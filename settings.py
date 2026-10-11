"""Shared CLI and semester-queue tuning; sizes have explicit binary units."""
import math

# name: default, minimum, maximum (None = unbounded), description
OPTIONS = {
    'download_concurrency': (2, 1, None, 'Simultaneous files'),
    'connections': (4, 1, 16, 'Connections per server per file'),
    'split': (4, 1, None, 'Download splits per file'),
    'min_split_size_mib': (4, 1, 1024, 'Minimum aria2 split size, MiB'),
    'disk_cache_mib': (256, 0, None, 'aria2 disk cache, MiB; 0 disables'),
    'bandwidth_limit': (0, 0, None, 'Total aria2 limit, bytes/second; 0 unlimited'),
    'auto_save_interval': (30, 1, None, 'Resume metadata save interval, seconds'),
    'connect_timeout': (20, 1, None, 'aria2 connection timeout, seconds'),
    'timeout': (60, 1, None, 'aria2 timeout, seconds'),
    'summary_interval': (30, 1, None, 'aria2 progress interval, seconds'),
    'retry_delay': (10, 0, None, 'Delay between download retries, seconds'),
    'listing_retry_delay': (60, 0, None, 'Delay between course-listing retries, seconds'),
    'free_space_mib': (1024, 0, None, 'Free space to retain in addition to required bytes, MiB'),
    'range_workers': (4, 1, None, 'Simultaneous Range requests'),
    'range_rps': (12.0, 0.01, None, 'Range requests/second per recovery'),
    'range_timeout': (30, 1, None, 'Range request timeout, seconds'),
    'range_retry_delay': (10, 0, None, 'Delay between Range passes, seconds'),
    'range_block_size_kib': (16384, 1, None, 'Initial Range block, KiB'),
    'range_min_block_size_kib': (1024, 1, None, 'Switch to linear probing at this block size, KiB'),
    'range_linear_block_size_kib': (64, 1, None, 'Linear probe block size, KiB'),
    'range_failure_limit': (32, 0, None, 'Switch to linear after more failed ranges than this'),
    'range_linear_probes': (256, 1, None, 'Maximum linear requests per recovery'),
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
        group.add_argument('--' + name.replace('_', '-'), type=type(default), default=default,
                           help=f'{help_text} (default: {default:g})')


def arguments(values):
    return ['--' + name.replace('_', '-') + '=' + str(value)
            for name, value in resolve(values).items()]
