# Pipeline spec (`crates/pipeline`)

Status: **agreed 2026-10-03, being built** on `feat/pipeline`.

The pipeline is the connection between `p2p` and `storage`. It takes the unsafe blocks the
network emits, writes them to the unsafe store, and, when L1 commits them, moves them to the
committed store and the local archive. It owns the retry policy that storage deliberately
does not have.

It does not depend on `p2p`: the binary hands it a channel. It is generic over the three store
traits, so it does not know about Redis, ClickHouse or fjall.

## 1. Inputs and outputs

| Direction | What | Type | From / to |
|---|---|---|---|
| in | unsafe blocks | `mpsc::Receiver<UnsafeBlock>` | `p2p`, through the binary |
| in | L1 heads (safe, finalized) | `watch::Receiver<L1Heads>` | the future `l1` crate; nothing sends until it exists |
| in | receipts for a block | not in this PR | the future `el` crate |
| out | safe block number | `watch::Sender<BlockNumber>` | `p2p` (its gap detection ignores heights at or below it) |
| out | the three stores | `UnsafeStore`, `CommittedStore`, `ArchiveStore` | `storage` |

## 2. Two tasks

Ingestion and promotion are separate tasks, so a slow ClickHouse never delays a gossip block.

```text
p2p ─▶ [ingest]  decode ─▶ recover senders ─▶ UnsafeStore::insert
l1  ─▶ [promote] UnsafeStore::ancestry ─▶ CommittedStore::insert ─▶ ArchiveStore::append_batch/trim
                 ─▶ CommittedStore::set_l1_heads ─▶ UnsafeStore::prune ─▶ safe number to p2p
```

Both stop on the cancellation token after finishing the write in progress. Every store write
is idempotent, so a write cut short is repeated on the next start.

## 3. Ingest

1. **Decode.** `p2p` already builds the full block from the SSZ payload to check
   its hash, and used to discard it and send the raw SSZ. `UnsafeBlock` now carries that
   `OpBlock` instead of the raw payload, so the block is decoded once and the pipeline needs no
   SSZ or engine-API dependencies. A block that passes every gossip rule but holds a
   transaction this build cannot decode (a type newer than it) is ignored by `p2p`, not
   rejected: it is our limitation, so the peer is not penalised, but the block is neither
   stored nor forwarded.
2. **Recover senders.** One secp256k1 recovery per signed transaction; deposits carry their
   sender. It is CPU work, so it runs on a blocking thread, one block at a time. A transaction
   whose sender cannot be recovered makes the block invalid: it is dropped with a warning and a
   counter (the sequencer signed it, so this should not happen).
3. **Insert** into the unsafe store as `DecodedBlock { receipts: None, source: Gossip }`. The
   returned events (`NewHead`, `Reorg`, `Filled`) are logged and counted; nothing else consumes
   them in this PR.
4. **Order.** Blocks are inserted in arrival order, one at a time. Fork choice in the store
   handles out-of-order and competing blocks.

## 4. Promotion

Runs whenever the L1 heads change. `C` is the safe head recorded in the committed store
(`CommittedStore::l1_heads`), `S` the new safe head.

1. **L1 reorg** (`S` is below `C`, or at `C`'s height with another hash):
   `CommittedStore::rollback_to(S)`; `ArchiveStore::truncate_above(S.number)` if the archive
   ends at or below `C` (so at most `C - S` blocks go); then continue.
2. `UnsafeStore::set_l1_heads(heads)`, so the unsafe store stops accepting blocks at or below
   `S` and fork choice respects it.
3. `UnsafeStore::ancestry(S, C.number)`: the blocks above `C` up to `S`, oldest first. The
   first block's parent must be `C`.
4. `CommittedStore::insert(blocks)`, then `ArchiveStore::append_batch` of the blocks if they
   extend the archive's tip, and `ArchiveStore::trim(retention)` (skipped when the archive is
   disabled, keeps everything, or already holds more than the window).
5. `CommittedStore::set_l1_heads(heads)`: the marker that the range is committed. Written
   after the data, so a crash before it repeats the range.
6. `UnsafeStore::prune(S)`, then publish `S.number` to `p2p`.

A change of the finalized head alone only records the heads (steps 2 and 5).

**Receipts.** Blocks are promoted whether or not they have receipts. Until the
`el` crate exists nothing has receipts, and waiting would mean nothing is ever committed. The
committed store's insert is idempotent with a version, so `el` can insert the same block again
with its receipts later.

**When the range cannot be read**, the pipeline promotes now and backfills later: it promotes
the part of the range next to `S` that it can read and leaves the rest as a hole, because
everything from `S` down to the break is on `S`'s chain and therefore safe. It cannot fill the
hole itself; backfill belongs to the `el` crate.

- `MissingAncestor` (a block was never received or has expired): the blocks above the missing
  one are read again and promoted; the hole is from `C + 1` to the missing block. If `S`
  itself is missing, nothing is promoted.
- `AncestryTooLong` (more than 1024 blocks): the newest blocks one call returns are promoted;
  the hole is the rest, which may still be in the unsafe store.
- The first block does not build on `C`: the range is promoted. Nothing is missing, but the
  committed block at `C`'s height belongs to another chain (the reorg limit below).

One promotion makes at most four `ancestry` calls (`MAX_RANGE_READS`); if none succeeds, the
whole range is the hole. Each hole is logged with its range and reason and counted, with only
the blocks actually left out; a promotion repeated after a crash counts its hole again. `S` is
recorded as the committed safe head and promotion continues from there. The blocks promoted
after a hole do not extend the archive and are not archived until range sync fills it (below).
Stalling until backfill exists was rejected: it would stop pruning and committing entirely.

**First safe head** (the committed store has recorded none): the committed store begins at
`S`. Block `S` alone is promoted if the unsafe store has it; `S` is recorded either way and no
hole is counted. Older history belongs to backfill. Heads with no safe head (only finalized
known) are recorded and nothing else happens.

**The archive is never emptied by promotion.** It holds one contiguous range, and the
importer and range sync write to it too, below and above `C`. The rule for who writes where:
promotion appends at the tip, and only blocks that extend it; range sync and the importer own
everything else, including the gap below a promoted range that did not connect.

- A promoted range that does not extend the archive's tip (`NotContiguous`: the archive is
  behind, or holds another chain at that height) is not archived. It is logged at most once
  in ten minutes with the archive's tip and the block, and counted
  (`op_indexer_pipeline_archive_skipped_blocks_total`). Nothing is removed to make room.
  Blocks the archive already holds are skipped by `append_batch`, so a range that range sync
  or an import stored first is fine.
- `trim` runs only while the archive holds no more than the retention window plus the blocks
  just appended, so it removes at most as many blocks as were appended. An archive that is
  already larger (an import: set the retention to `all`) is not trimmed, with a warning.
- Blocks are removed only in step 1, an L1 reorg of the safe head, and only if the archive
  ends at or below `C`: at most `C - S` blocks, the depth of the reorg. If it reaches above
  `C`, other writers put blocks there and it is left alone, with a warning.
- Startup removes nothing (section 5).

**Known limits, to be closed with the `l1` crate:**

- *A safe head that jumps more than 1024 blocks at once* (an L1 source catching up after
  downtime): only the newest 1024 are promoted, even when the unsafe store has every block.
  Promoting the rest in chunks, oldest first, needs a lookup of a block's ancestor at a height
  in the unsafe store.
- *An L1 reorg where the block at `S.number` itself changed*: `rollback_to(S)` deletes the rows
  above `S.number`, but the row at `S.number` is the old chain's block and stays. The pipeline
  cannot replace it (the unsafe store ignores blocks at or below the safe head, and the
  committed store has no read call to compare hashes). Backfill repairs it, like a hole of one
  block.

## 5. Startup

1. Read `C` from the committed store. If there is none, start both tasks.
2. Write the heads to the unsafe store (Redis may have been wiped by a layout change), prune it
   up to `C` (the last run may have stopped between the marker and the prune), and publish
   `C`'s number to `p2p`.
3. Start both tasks.

Nothing is deleted from the committed store or the archive at startup. Rows or archived blocks
above `C` may come from a promotion that stopped before its marker, or from an import or a
range sync that reached further; the two cannot be told apart, and deleting the second kind
would discard work that takes days. What a stopped promotion leaves is harmless: the repeat
inserts the same rows again (the insert is idempotent) and finds its blocks in the archive,
which skips them. Only if the safe chain is another one after the restart (a crash between
steps 4 and 5, then an L1 reorg) do rows of the stopped attempt stay and its blocks stay at
the archive's tip, where later ranges then do not connect: backfill repairs the rows, the
operator the archive.


The network is not held back: it starts next to this reconciliation, and the block channel
buffers what arrives until ingest starts.

## 6. Errors and retries

Storage never retries; the pipeline decides by `StorageError::severity()`.

| Severity | Ingest | Promotion |
|---|---|---|
| Transient | Retry the same block with exponential backoff (200 ms doubling to 30 s, with jitter), forever; cancellation ends a retry at once. The channel fills behind it; `p2p` already drops and counts blocks when the channel is full. | Retry the same step with backoff, forever. |
| Expected | Does not occur on insert. | `MissingAncestor`, `AncestryTooLong`, `NotContiguous`: handled as in section 4. |
| Fatal | `InvalidBlock` / `UnsupportedTransaction`: drop the block, warn, count. Anything else: stop the binary with the error. | Stop the binary with the error. |

A retried insert that had in fact been applied returns `stored = false`; that is fine, the
events went to the stream.

## 7. Binary wiring

- `prepare_storage` returns the stores instead of dropping them.
- The binary creates the two watch channels (L1 heads, safe number), builds the pipeline and
  runs it next to the network. The network runs on a child of the pipeline's cancellation
  token, so it can be stopped first. If either stops on its own, the other is cancelled and
  the process exits.
- Shutdown order: cancel, wait for the network, let the pipeline drain what is in the channel,
  wait for it.
- No new configuration except the backoff bounds, which are constants.

## 8. Metrics

Through the `metrics` facade, like storage: blocks ingested, blocks dropped (by reason), reorgs
and their depth, fills, retries (by store), blocks promoted, promotion holes (blocks missing),
ingest lag (now minus block timestamp), channel depth.

## 9. What can be verified in this PR

- **Ingest**: live, on mainnet gossip. Blocks, transactions and senders appear in Redis; reorg
  and fill events show in the log and the event stream.
- **Promotion**: no L1 source exists, so it cannot run live. It is verified with a throwaway
  driver outside the repo that feeds safe heads trailing the unsafe head, including a step
  back (L1 reorg), a missing block, and a kill between each pair of steps.

## 10. Not in this PR

Receipts and backfill (`el`), the source of the L1 heads and the unsafe store's reconciliation
when the safe head contradicts it (`l1`), reading data back (`query`), a metrics exporter.

## 11. Modules and API

| File | Holds |
|---|---|
| `lib.rs` | `Pipeline<U, C, A>`: `new(...)` and `run(self, cancel) -> Result<(), PipelineError>`. Runs startup (section 5), then both tasks; returns when both have stopped, or with the first fatal error after cancelling the other. |
| `ingest.rs` | The ingest task (section 3). |
| `recover.rs` | Sender recovery on a blocking thread. |
| `promote.rs` | Startup reconciliation and the promotion task (sections 4 and 5). |
| `retry.rs` | One helper used by both tasks: runs a store call, retries it with capped exponential backoff and jitter while its error is `Transient`, returns any other result, and stops waiting when cancelled. |
| `error.rs` | `PipelineError`. |
| `metrics.rs` | Names, descriptions and recording functions (section 8), like `storage::metrics`. |

`Pipeline::new` takes the unsafe store, the committed store, the archive with its retention
(`Option`, `None` when disabled), the block receiver, the L1 heads receiver and the safe-number
sender. The stores are `Clone + Send + Sync + 'static`.
