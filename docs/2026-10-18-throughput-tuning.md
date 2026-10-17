# Throughput tuning, 18 October 2026

## Applied changes

- Publish confirmed Nextcloud/OpenCloud fallback download bytes during streaming,
  using the existing once-per-second shared-budget update. Long files no longer
  hide their traffic from the download concurrency controller until completion.
- Retry transient OpenCloud catalog reads with the existing request retry helper,
  preserving completed folders during the scan.
- Restore pending/running SHA tasks before the course traversal after a restart.
  Discard superseded SHA results and exceptions before they can affect a newer
  source version, local body, aria2 control, or transfer checkpoint.
- Set the shared upload range to 2–3 files for both cloud configurations. Keep
  Nextcloud uploading paused. Set daily Recheck to 4 slots; regular Range remains
  16 files with 12 concurrent requests and 5 requests/s. Speed targets remain
  90 Mbit/s in each direction, not hard traffic limits.

## Measurements on the running system

Each transfer window lasted 240 seconds after uploads became active. Interface
rates include Range, retries, and other host traffic. OpenCloud bytes count only
confirmed payload. Range bytes come from archive metrics with unchanged run IDs.

| Upload setting | Actual upload slots, mean | Incoming Mbit/s | Outgoing Mbit/s | HDD busy | Range bytes saved |
|---|---:|---:|---:|---:|---:|
| Fixed 8 | 8.00 | 16.56 | 64.72 | 94.7% | 0.426 GB |
| Automatic 2–3 | 2.67 | 31.08 | 41.28 | 94.6% | 0.786 GB |

The second window favored incoming traffic at the expense of outgoing traffic.
There were no completed SHA checks in either window, so faster SHA processing
has **not** been demonstrated. The waiting SHA count rose from 6 to 17 in the
second window as more files arrived. Availability at Yandex changes with time;
these sequential windows do not establish a repeatable causal speedup.

A final 240-second observation with Recheck=4 measured 31.78 Mbit/s incoming,
52.87 Mbit/s outgoing, 96.3% HDD busy, and 0.759 GB of Range data saved. Mean
upload concurrency was 2.89. One SHA check completed (2.15 GB); this check started
before the window, and new arrivals increased the waiting count from 17 to 23.
This does not demonstrate a SHA throughput improvement.

The subsequent Recheck=4 observation also includes changed regular-workload and
discovery state after restarting Range, so it cannot isolate the Recheck limit.
The lower limit is a priority choice: fewer daily repeat checks compete with
ordinary recovery and verification. It is not a proven throughput improvement.

Cold starts were excluded from the table. OpenCloud took several minutes to
build its candidate list; Range reread roughly 2 GB of events and state. Restart
cost matters, but this change does not add a new persistent event index.

Raw observations (metadata only, not committed):
`~/.local/state/yds-download/tuning-20261018.jsonl`.
Private configuration before tuning:
`~/.local/state/yds-download/tuning-20261018-services-original.json`.

## Verification

584 unittest checks passed. Deterministic regressions reproduced live-byte delay,
SHA restart starvation, and obsolete SHA results/exceptions affecting replacement
files before the fixes. Independent deep review found and closed the stale-result
and stale-exception races; final review reported no remaining P1/P2. Deployment
configuration was parsed with `services.load` and its concurrency settings were
compared with the live private configuration.
