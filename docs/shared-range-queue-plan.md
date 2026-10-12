# Shared Range queue

Goal: retain the maximum available data while ordinary downloads continue through
archives. Keep SHA-256 verification and durable per-request journals unchanged.

## Changes

1. `pipeline.py`, `download.py`: optional deferred Range jobs. Finish SHA and
   ordinary retries, persist Range jobs, leave courses recovering, release the
   archive download lock. Standalone downloads keep their existing inline mode.
2. `semester_queue.py`: skip archives whose ordinary transfers finished, continue
   to the next archive, and reflect Range completion from archive summaries.
   Deep verification and retry-check retain their independent meanings.
3. `range_queue.py`: one worker service for both queues, four files globally,
   two HTTP Range requests per file, eight requests globally, 12 requests/s.
   Prefer personal files while reserving capacity for course recovery.
   Claim archive locks before recovery; current foreground archives remain owned
   by the downloader. Reuse Background's task application and failure handling.
4. `recovery.py`: shared request budget instead of the foreground one-request
   mutex when the external worker is used. Existing inline behavior remains.
5. Cache event/task reads by file metadata; use in-memory job descriptors during
   normal pipeline polling. Preserve fsync before advancing trusted ranges.
6. Serialize report publication through a repository Git lock so separate
   foreground and recovery processes cannot commit over each other.

## Checks

- A deferred job survives restart without another ordinary download or completed
  recovery; SHA/retry finish before an archive advances.
- Shared request count and request rate stay within limits; four files can make
  progress independently and stop preserves received bytes.
- Range worker respects foreground archive locks, validates source identity,
  applies results once, and updates course summaries and reports.
- Full repository unittest discovery passes; review P1/P2 findings and fix them.
- Merge/push, restart the two foreground units and the shared Range unit, record
  a real interval of completed files, bytes saved and active task counts.

Logs: the existing queue logs plus a shared Range log in the private download
state. No personal state, tokens, or signed download URLs are committed.
