# Bounded tolerance for slow local aria2 status

Production source:12 foreground restarts exposed `aria2 control channel lost (TimeoutError)` with a hardcoded3-second local timeout. External diagnostics cannot read the real RPC secret because aria2 masks process argv; do not use those invalid-auth samples to infer request latency. Instrument existing authenticated calls instead.

Approved: fix remaining operational problems with independent deep review/debug loop before deployment. OpenCloud/Nextcloud uploads and Range normal queue are working; preserve their progress, shared budgets, source matching, SHA and local-outage retries. Yandex backend failures remain external; do not change those timeouts/retry classes without evidence.

- [x] Red regression: status timeout aborts the whole batch; stop during timeout loses cancellation.
- [x] Raise configurable local RPC timeout to30seconds; two bounded read-only retries with sameGID/slot. Never replay addUri. Dead/permanently rejected engines fail safely; native controls preserve progress for supervision.
- [x] Cap startup calls by remaining15-second readiness deadline. Check cancellation/diskfull before promoting control failures.
- [x] Add RPC call/time/error/retry counters to existing batched Metrics, no network probes or perrequest state writes.
- [x] Focused/full checks, independent review; fix P1/P2, original validation then fresh review as needed.
- [ ] Commit/push main and restart affected downloader services, verify real calls/progress/restarts and memory.

Original diagnosis reviewer validated both P1/P2 fixes and passed all 17 adaptive tests. Fresh independent implementation review found no P1/P2. Focused normal/deep/pipeline suite: 70 tests passed. Full release suite: 505 tests passed (`python3 -m unittest discover -q`, exit 0).
