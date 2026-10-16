# Remote main review and cloud proof fixes

Approved scope: validate remote main changes against live base 2c7a62f, repair OpenCloud paths and fallback matching, reduce proof-cache memory/read amplification, permanently set Range MemoryHigh=2.5GiB and MemoryMax=3GiB. Do not change Yandex timeouts; independent curl reproduced remote storage failures and historical metrics showed the same failures before adaptive downloads.

Remote main 6bc5c2e supplies raw DAV traversal, whitespace-preserving Nextcloud matching, shared EventIndex in download.py, compact pipeline/deep/upload consumers and a persistent systemd memory drop-in. Original isolated fixes are retained on fix/cloud-paths-proof-cache (8756f8b); duplicated changes were not reapplied.

- [x] Remote main baseline: 492 unittest tests passed.
- [x] Close the remaining CloudFallback full-history cache: reuse d.EventIndex with separate (path,local) identities and complete SHA/stat proof fields.
- [x] Independent review #1: fix P1 where upload-side tail repair could truncate a concurrent writer; nonrepair readers wait for newline-terminated rows. Add regressions that fail before the fix.
- [x] Independent review #1: fix P2 where OpenCloud fallback parent matching trimmed trailing course whitespace despite correct traversal. Add positive/negative matching regression.
- [x] Set permanent live Range limits and save resume properties; back up launch configuration on the external disk.
- [x] Validate fixes in original reviewer, full suite, fresh independent deep review/debug loop for P1/P2.
- [x] Commit/push main, install repository memory drop-in, restart affected services with saved launch settings, verify raw catalog traversal/resume/memory.

Release gate: 497 tests passed (`python3 -m unittest discover -q`); original reviewer validated concurrent appends in four real multiprocessing runs; fresh reviewer #2 found no P1/P2 and passed157 focused tests.

Runtime gate after 17ac7d0: all four services started; OpenCloud scan produced a893-video plan without404 and resumed its old20MB upload; Nextcloud resumed persisted chunk positions. Range memory initially low while loading state; permanent limits verified.

A foreground packet subsequently exited with a masked `File transfer budget interrupted`; storage supervision restarted it with progress retained. Review#2 reproduced P2: the initiating RPC failure was hidden by a sibling budget waiter during shutdown. Save the first real worker error before setting stopping and prefer it during result collection, preserving diskfull9 and user interrupt handling. New delayed-shutdown regression observed red then passed; original reviewer validated the fix. Production's initiating RPC cause remains unknown. Full tests and fresh review#3 run before enabling this additional fix.

Additional release gate: 498 tests passed, original reviewer validated RPC error preservation, fresh independent review#3 found no remaining P1/P2 and passed all10 adaptive download tests. Deploy the additional diagnostic change to the foreground queue; cloud upload processes and the warming Range worker keep the reviewed17ac7d0 logic.
