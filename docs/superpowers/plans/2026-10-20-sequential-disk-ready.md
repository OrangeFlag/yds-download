# Sequential disk readiness implementation plan

**Goal:** Continue downloads on a new HDD while the old HDD is disconnected, preserving the old archive and avoiding repeated complete files.
**Architecture:** Reuse the signed host volume catalog for content lookup. Keep offline availability separate from local SHA verification; provision a fresh queue and single service configuration without copying old partial-file proofs.
**Constraints:** One archive disk connected at a time. No formatting, deletion, payload copies onto system SSD, automatic download resume, or reuse of offline partial bytes. New target must be explicitly identified by mounted path and UUID. Preserve infrastructure/programming selection and personal-file priority.

1. Add content lookup and snapshot coverage for registered/imported verified copies. Use SHA-256 plus size; exclude the current UUID; cache metadata; never read offline mounts or rehash videos.
2. Add metadata-only handoff export and safe target provisioning, including personal queue before courses and fresh state. Preserve old raw Range bodies on their original volume.
3. Wire offline checks into normal download selection, Range retirement/rechecks, course summaries and table marks. Never report offline copies as present on the new HDD or erase old ownership marks.
4. Add regressions for offline skipping, catalog invalidation, unchanged restart, Range recovery and report preservation. Run complete CI checks.
5. Deep independent review; fix P1/P2 and repeat with a fresh reviewer until clear.
6. Stop all services before final handoff; save reflink state backup on old HDD, signed catalog on host+HDD, queue/config bundle on host and disk, and concise operator instructions. No target installation until its UUID/path are known.
