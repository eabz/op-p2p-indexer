# Pipeline spec (`crates/pipeline`)

The pipeline is the connection between the networks and `storage`. It takes the unsafe blocks
the network emits, writes them to the unsafe store, and, when L1 commits them, moves them to
the committed store and the local archive. It also attaches the receipts the execution network
fetches, stores the blocks a range sync fetches, and turns the dispute games the L1 side
verifies into the safe and finalized heads. Store calls go through storage's retry helper
(`op_indexer_storage::retry`), without a time limit.

It does not depend on `p2p`: the binary hands it a channel. It is generic over the three store
traits, so it does not know about Redis, ClickHouse or fjall.

## 1. Inputs and outputs

| Direction | What | Type | From / to |
|---|---|---|---|
| in | unsafe blocks | `mpsc::Receiver<UnsafeBlock>` | `p2p`, through the binary |
| in | L1 heads (safe, finalized) | `watch::Receiver<L1Heads>` | the binary: the commitment task's heads, held back while a range sync is still closing the archive's gap; nothing moves without the L1 side |
| in | verified dispute games | `watch::Receiver<L1Games>` | `l1`, through `Pipeline::with_l1_games` |
| in / out | receipts | `ReceiptsChannels`: requests out, `VerifiedReceipts` in | `el` |
| in | range-sync batches | `mpsc::Receiver<Vec<EncodedBlock>>` | `el`, through `Pipeline::with_range` |
| out | unsafe head | `watch::Sender<Option<BlockRef>>` | the execution network's advertised head, through `Pipeline::with_head` |
| out | safe block number | `watch::Sender<BlockNumber>` | `p2p` (its gap detection ignores heights at or below it) |
| out | the three stores | `UnsafeStore`, `CommittedStore`, `ArchiveStore` | `storage` |

## 2. Tasks

Five tasks, each its own, so a slow ClickHouse never delays a gossip block: ingest and
promotion always run; the receipts task, the range task and the commitment task run when
their input is given (section 4b).

```text
p2p ─▶ [ingest]  decode ─▶ recover senders ─▶ UnsafeStore::insert
l1  ─▶ [promote] UnsafeStore::ancestry ─▶ CommittedStore::insert ─▶ ArchiveStore::append_batch/trim
                 ─▶ CommittedStore::set_l1_heads ─▶ UnsafeStore::prune ─▶ safe number to p2p
```

All stop on the cancellation token after finishing the write in progress. Every store write
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
   returned events (`NewHead`, `Reorg`, `Filled`) are logged and counted, and a new head is
   published on the head output when there is one (`with_head`). Ingest then asks for the
   block's receipts when the execution network runs.
4. **Order.** Blocks are inserted in arrival order, one at a time. Fork choice in the store
   handles out-of-order and competing blocks.

## 4. Promotion

Runs whenever the L1 heads change. `C` is the safe head recorded in the committed store
(`CommittedStore::l1_heads`), `S` the new safe head.

1. **`S` at or below `C`.** A rollback is destructive, so it needs evidence that the block
   at `S.number` changed: `S` at `C`'s height with another hash, or `S` below `C` where the
   archive covers `S.number` and holds another block there (`ArchiveStore::number_of(S.hash)`
   is not `S.number`). Then it is an **L1 reorg**: `CommittedStore::rollback_to(S)`;
   `ArchiveStore::truncate_above(S.number)` if the archive ends at or below `C` (so at most
   `C - S` blocks go); then continue. Otherwise (`S` is on the committed chain, or cannot be
   checked: no archive, or outside it, with a warning) the head is behind and nothing is
   done.
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

**Receipts.** Blocks are promoted whether or not they have receipts: waiting would hold
promotion on the execution network. Receipts that arrive after a block was promoted are
attached in the archive (section 4b); the committed store keeps the row without them, and its
insert is idempotent with a version, so the block can be inserted again with its receipts.

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

**Known limits:**

- *A safe head that jumps more than 1024 blocks at once* (an L1 source catching up after
  downtime): only the newest 1024 are promoted, even when the unsafe store has every block.
  Promoting the rest in chunks, oldest first, needs a lookup of a block's ancestor at a height
  in the unsafe store.
- *An L1 reorg where the block at `S.number` itself changed*: `rollback_to(S)` deletes the rows
  above `S.number`, but the row at `S.number` is the old chain's block and stays. The pipeline
  cannot replace it (the unsafe store ignores blocks at or below the safe head, and the
  committed store has no read call to compare hashes). Backfill repairs it, like a hole of one
  block.

## 4b. The receipts task and the range task

Two more tasks run next to ingest and promotion, each only when something feeds it.

**Receipts** (`receipts.rs`, when the execution network is enabled). Ingest asks for the
receipts of every block it stores, on a bounded channel it never waits on (a request that does
not fit is dropped and counted). At startup the task asks again for the newest stored blocks
that still lack receipts (up to 1,024, one read per block: the unsafe store has no bulk read
of that field). Verified receipts come back on a second channel and are attached in the unsafe
store, or in the archive when the block has been promoted in the meantime; receipts for a
block neither holds are dropped and counted. A store that refuses them (wrong count or number)
is logged and counted; any other store error stops the pipeline.

**Range** (`range.rs`, when a range sync is configured). The execution network hands over
verified blocks in ascending order, in batches of consecutive blocks, as the bytes it received
(`EncodedBlock`). For each batch the task decodes the blocks and recovers their senders on
blocking threads (32 blocks per thread; a legacy transaction signed with all zeros gets the
zero address), inserts them into the committed store with source `Sync`, then appends the same
bytes to the archive with `append_batch`. It keeps no progress of its own: the archive's last
block is where the binary starts the sync again. A batch is stored whole or the pipeline
stops with the error (`PipelineError::RangeBlock` for a block this build cannot read,
`PipelineError::Storage` for a store that refuses the batch), because the archive is one
contiguous range and a sync that cannot continue must not look like it is running.

**Commitment** (`commit.rs`, when the L1 side is connected: `Pipeline::with_l1_games`). The
L1 side publishes the dispute games it has verified on L1 (`L1Games`: the 64 most recent on
the walked L1 chain, and the highest finalized L1 block on it). A game is a bonded claim
about an L2 block's output root that anyone can make, of any game type (the respected type is
not checked, `docs/l1.md` §3); verified on L1 does not mean it is about our chain. So "safe"
means "a bonded claim on L1 equals our block", not "the batch is on L1". The task judges the
games highest L2 block first, and stops once no game left can raise a head. For each it reads
our own block at the game's height (the unsafe store's canonical block at that number, else
the archive's header), computes its output root (`VerifiedGame::check`: state root, the
message passer's storage root the header carries from Isthmus on, block hash; the timestamp
too for super games) and compares. The highest match becomes the safe head, and the highest
match in a finalized L1 block the finalized head; both are published in one update.
Different: an error log with the game and both values, counted
(`op_indexer_pipeline_l1_games_total{outcome="mismatch"}`), and that game moves no head. A
block before Isthmus cannot be checked from its header: warning, counted, no advance. Each
game and block pair is logged once. Every 12 s the games are judged again, so a game about a
block we did not hold yet, or one whose block at that height has since changed, counts once
it matches. A game that matched is remembered (the whole game, with our hash) while it is recent and is
not read again: when its L1 block finalizes, minutes later, promotion has usually pruned the
block from the unsafe store. After a restart that memory is empty, so a finalized game at or
below the committed safe head needs the archive to raise the finalized head.

The heads only move up, across restarts too: the task starts from the committed store's
heads (`CommittedStore::l1_heads`, read at startup) and never publishes one below them, so a
restart does not hand promotion an older head. A recorded finalized head above the safe one
(left by a rollback, which does not touch the finalized row) is not taken over: the
finalized head starts unknown and the next finalized match sets it. The heads go to promotion through the binary,
which holds them back while a range sync is still bringing the archive up to the chain.

Ingest also publishes the unsafe head on a `watch` (`Pipeline::with_head`), which the binary
gives to the execution network as the newest block the node knows.

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

The stores do not retry; every store call goes through `op_indexer_storage::retry` without a
time limit, and the task decides what a non-transient error means by
`StorageError::severity()`.

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

## 9. What has been verified

- **Ingest**: live, on mainnet gossip. Blocks, transactions and senders appear in Redis; reorg
  and fill events show in the log and the event stream.
- **Promotion**: with a throwaway driver outside the repo that feeds safe heads trailing the
  unsafe head, including a step back (L1 reorg), a missing block, and a kill between each pair
  of steps. Not run on heads from the L1 side.
- **Receipts, range and commitment tasks**: compiled and reviewed; not run end to end.

## 10. Not done

The unsafe store's reconciliation when the safe head contradicts it (`docs/storage.md`, the
safe-head gap), reading data back (`query`), a metrics exporter.

## 11. Modules and API

| File | Holds |
|---|---|
| `lib.rs` | `Pipeline<U, C, A>`: `new(...)`, the builders `with_head`, `with_l1_games`, `with_range`, and `run(self, cancel) -> Result<(), PipelineError>`. Runs startup (section 5), then the tasks; returns when they have stopped, or with the first fatal error after cancelling the others. |
| `ingest.rs` | The ingest task (section 3). |
| `recover.rs` | Sender recovery on a blocking thread. |
| `promote.rs` | Startup reconciliation and the promotion task (sections 4 and 5). |
| `receipts.rs` | The receipts task and `ReceiptsChannels` (section 4b). |
| `range.rs` | The range task: range-sync batches into the committed store and the archive (section 4b). |
| `commit.rs` | The commitment task: verified dispute games checked against our blocks, into L1 heads (section 4b). |
| `retry.rs` | Storage's `retry` without a time limit, and `settle`, which ends a task on what it returns. |
| `error.rs` | `PipelineError`. |
| `metrics.rs` | Names, descriptions and recording functions (section 8), like `storage::metrics`. |

`Pipeline::new` takes the unsafe store, the committed store, the archive with its retention
(`Option`, `None` when disabled), the block receiver, the L1 heads receiver, the safe-number
sender and the receipts channels (`Option`, `None` without an execution network). The
builders add the head output (`with_head`), the dispute games and the L1 heads sender they
feed (`with_l1_games`), and the range-sync batches (`with_range`). The stores are `Clone + Send + Sync + 'static`.
