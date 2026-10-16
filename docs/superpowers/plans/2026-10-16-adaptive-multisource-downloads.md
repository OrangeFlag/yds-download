# Adaptive multisource downloads implementation plan

> **For agentic workers:** use Superpowers execution with targeted test-first changes and independent review before deployment.

**Goal:** Tune actual concurrent downloads by measured byte-rate improvement, sharing slots across Yandex and verified Nextcloud/OpenCloud fallback.

**Architecture:** Preserve archive locks, Yandex-first retries, file ledgers, SHA and Range. Run aria2 with authenticated loopback RPC only in adaptive mode; submit one Yandex file per shared-budget slot, preserving its .aria2 controls. Fallback sources use the same global download budget, separate from upload and Range HTTP budgets. Reuse the existing bounded speed experiment and process-crash-safe file slots.

**Tech stack:** Python stdlib, existing aria2 RPC, existing flock/JSON state.

**Constraints:** No personal files in course cloud fallback; candidates must correspond to Yandex manifests. Remote checksums identify candidates; local SHA-256 decides validity. Preserve wrong-SHA alternatives with source suffix. Never mix unverified blocks between different cloud bodies; retain independent ETag/Range checkpoints. No payload on system disk, no TCP availability probes, no cloud writes for testing.

## Tasks

- [x] Extend existing resumable fallback with OpenCloud metadata adapter, reuse scan cache across archives, avoid duplicate verified/retained copies. Tests: metadata match, ambiguity, personal exclusion, SHA mismatch, resume and already-local skip.
- [x] Add shared-budget aria2 RPC runner and zero-speed exploratory probe; retain safe stop, local-outage retry semantics and old fixed mode. Tests: real local HTTP/aria2 transfer, resume-byte accounting, failed HTTP, failed RPC without duplicate submissions, concurrency/drain and stop.
- [x] Wire six source/budget flags through download, semester queue and Range worker; persist configuration without credentials. Tests: partial configuration rejection, flag forwarding, fallback scheduling.
- [x] Run full unittest suite and independent deep review. Fix P1/P2 with regression tests, validate in original reviewer and rerun independent review when required.
- [ ] Commit/push main and restart affected services with saved settings only after clean review. Verify actual transfers and bounded shared state.

## Review focus

- Ambiguous RPC submission cannot start duplicate writes.
- A resumed prefix must not inflate measured speed.
- Missing Yandex payload versus local network/disk outage must retain different retry semantics.
- Source matching cannot claim unrelated semester/video as the original.
- Process death, changing workload and reduction in limits must not strand slots or delete partial bytes.

## Release verification

486 tests passed (`python3 -m unittest discover -q`). Independent final review #5 found no remaining P1/P2 after regression fixes and validation by the original reviewers. Runtime rollout follows this commit.
