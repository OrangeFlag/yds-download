# Remote main review and cloud proof fixes

Approved scope: validate remote main changes against live base 2c7a62f, repair OpenCloud paths and fallback matching, reduce proof-cache memory/read amplification, permanently set Range MemoryHigh=2.5GiB and MemoryMax=3GiB. Do not change Yandex timeouts; independent curl reproduced remote storage failures and historical metrics showed the same failures before adaptive downloads.

Remote main 6bc5c2e supplies raw DAV traversal, whitespace-preserving Nextcloud matching, shared EventIndex in download.py, compact pipeline/deep/upload consumers and a persistent systemd memory drop-in. Original isolated fixes are retained on fix/cloud-paths-proof-cache (8756f8b); duplicated changes were not reapplied.

- [x] Remote main baseline: 492 unittest tests passed.
- [x] Close the remaining CloudFallback full-history cache: reuse d.EventIndex with separate (path,local) identities and complete SHA/stat proof fields.
- [x] Independent review #1: fix P1 where upload-side tail repair could truncate a concurrent writer; nonrepair readers wait for newline-terminated rows. Add regressions that fail before the fix.
- [x] Independent review #1: fix P2 where OpenCloud fallback parent matching trimmed trailing course whitespace despite correct traversal. Add positive/negative matching regression.
- [x] Set permanent live Range limits and save resume properties; back up launch configuration on the external disk.
- [x] Validate fixes in original reviewer, full suite, fresh independent deep review/debug loop for P1/P2.
- [ ] Commit/push main, install repository memory drop-in, restart affected services with saved launch settings, verify raw catalog traversal/resume/memory.

Release gate: 497 tests passed (`python3 -m unittest discover -q`); original reviewer validated concurrent appends in four real multiprocessing runs; fresh reviewer #2 found no P1/P2 and passed157 focused tests.
