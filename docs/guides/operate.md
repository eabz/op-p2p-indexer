# Operate and recover

Start with one chain and one role. Use v0.1.12 configuration and record the config path, service owner, ports, binary version and data directories for each process. Do not share a writable data directory between processes.

## Health and capacity

Watch peer connectivity, received heights, gaps, receipt progress, safe/finalized progress when enabled, and consumer lag. Check disk free space, memory, CPU, file descriptors and object-store errors alongside process logs. A running systemd unit or an advancing unsafe head alone is not sufficient health evidence.

There is no universal production sizing promise. Measure a representative range and live workload on your hardware. Grow concurrency only while ingestion stays current and tail latencies remain acceptable. [Benchmarks](../bench.md) describe observed workloads; they are not service-level commitments.

## Access

Keep the API on loopback for your first setup. Optional bearer keys provide access control; the binaries have no built-in TLS. Put remote APIs behind a TLS-capable proxy or private tunnel, restrict network access and protect config files with mode `0600`. Separate client credentials from object-store writer credentials. Never publish these in issue reports.

## Upgrade and backup

1. Record the current release and pause consumers if they cannot tolerate restart.
2. Stop the process gracefully. Copy its config and complete data directory while stopped, including node state and the unsafe journal. For a fleet, preserve the object-store manifest and its referenced immutable chunks as well.
3. Install the intended release explicitly, review its configuration changes, and validate with `--check-config` before startup.
4. Restart one node, check head/receipt continuity, and run a small read. Reconcile consumer checkpoints before resuming larger jobs.

Do not assume that replacing binaries makes data formats backward compatible. Test restore and upgrades on a copy before relying on rollback. A deployment is not backed up until you have demonstrated a restore.

## Import failures

A PUT timeout can leave later sealed records behind a missing chunk. A subsequent `verify` may report a gap. Read the exact error and the release's [import reference](../import.md); upload/retry behavior can differ from development versions.

Stop the importer before changing its state. Preserve a copy of its state records. If the error explicitly instructs removal of sealed records from a given block, first identify their filenames and ranges, then **move the affected records to a recovery directory** so the repair is reversible. Retain raw inputs. Re-run verification and check manifest continuity; never delete remote chunks or manifests as a generic timeout fix.

A missing header field requires a fill/refetch before verification. `download` can scan hundreds of thousands of existing raw files without immediately fetching anything. A growing scan count with zero fetched blocks does not yet prove that the failing record has been repaired. Check the fill source/configuration and completion summary before retrying `verify`.

## Recovery acceptance

Before depending on the service, demonstrate restart from local state, object-store failure/retry, interrupted import, exporter boundary continuity and consumer resume under server loss. Track evidence and unresolved cases in [readiness](readiness.md). Never silently skip a gap to make a process appear healthy.
