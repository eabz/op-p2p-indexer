# Pipeline spec (`crates/pipeline`)

The pipeline is the connection between the networks and `storage`. It takes the unsafe blocks
the network emits, writes them to the unsafe store, and, when L1 commits them, moves them to
the block archive, the committed store. It also attaches the receipts the execution network
fetches, stores the blocks a range sync fetches, and turns the dispute games the L1 side
verifies into the safe and finalized heads. Store calls go through storage's retry helper
(`op_indexer_storage::retry`), without a time limit.

It does not depend on `p2p`: the binary hands it a channel. It is generic over the two store
traits, so it does not know how either store keeps its blocks.

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
| out | the two stores | `UnsafeStore`, `ArchiveStore` | `storage` |

## 2. Tasks

Five tasks, each its own, so a slow archive never delays a gossip block: ingest and
promotion always run; the receipts task, the range task and the commitment task run when
their input is given (section 4b).

```text
p2p ─▶ [ingest]  decode ─▶ recover senders ─▶ UnsafeStore::insert
l1  ─▶ [promote] UnsafeStore::ancestry ─▶ roots checked ─▶ ArchiveStore::append_batch
                 ─▶ ArchiveStore::set_heads ─▶ UnsafeStore::prune ─▶ safe number to p2p
```

All stop on the cancellation token after finishing the write in progress. Every store write
is idempotent, so a write cut short is repeated on the next start. Promotion, the commitment
task and the range task follow inputs that close only when the node stops; one of them ending
before the node starts shutting down (`Pipeline::run`'s `shutdown` token) stops the pipeline
with `PipelineError::Ended`, naming the task, and so the node. The binary does the same for
its own two data tasks, the L1 heads forwarder and the range sync planner.

## 3. Ingest

1. **Decode.** `p2p` already builds the full block from the SSZ payload to check
   its hash, and used to discard it and send the raw SSZ. `UnsafeBlock` now carries that
   `OpBlock` instead of the raw payload, so the block is decoded once and the pipeline needs no
   SSZ or engine-API dependencies. A block the sequencer signed that this build cannot read
   is ignored by `p2p`, not rejected: the peer is not at fault. It is neither stored nor
   forwarded, and three distinct ones within ten minutes stop the node
   (`NetworkError::ProtocolChanged`): the chain activated a change this build does not know,
   and the operator must upgrade. Only what the sequencer alone can produce counts: a signed
   payload that decodes as no payload version this build knows; one on the topic the current
   time requires that breaks a fork rule or does not rebuild to its hash; a transaction of an
   unknown type. A genuine payload a peer replays on another version's topic (the signature
   covers the bytes, not the topic) decodes as its own version and is rejected as the peer's
   fault, so no peer can stop the node.
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

Runs whenever the L1 heads change. `C` is the safe head recorded in the archive
(`ArchiveStore::heads`), `S` the new safe head.

1. **`S` at or below `C`: nothing is done.** The heads only rise: the commitment task never
   publishes a safe head at or below the committed one (section 4b), so there is no rollback
   and nothing is ever removed from the archive.
2. `UnsafeStore::set_l1_heads(heads)`, so the unsafe store stops accepting blocks at or below
   `S` and fork choice respects it.
3. `UnsafeStore::ancestry(S, C.number)`: the blocks above `C` up to `S`, oldest first. The
   first block's parent must be `C`.
4. `ArchiveStore::append_batch` of the blocks, with their senders, if they extend the
   archive's tip. Each block's transactions root, and receipts root when it has receipts, is
   computed over exactly the bytes about to be written and compared with its header; the list
   ends before the first block that does not match, which is not appended (error log) and so stays the hole above the recorded safe
   head.
5. `ArchiveStore::set_heads(heads)`: the marker that the range is committed. Written after
   the data, so a crash before it repeats the range. **The recorded heads never name a block
   the archive lacks:** the safe head recorded is the newest block of `S`'s chain the archive
   holds after step 4 (`S` when the whole range went in, else the newest block appended), and
   the finalized head is recorded only if it is not above it. When nothing was appended but
   the archive already holds `S` (range sync stored it, and the unsafe store may not have it),
   `S` is recorded; otherwise the safe head stays at `C`. A part that does not extend the
   archive ends the step: the parts above it build on it.
6. `UnsafeStore::prune` up to the recorded safe head, then publish its number to `p2p`. Blocks
   above it stay in the unsafe store for a later promotion (or until they expire).

A change of the finalized head alone only records the heads (steps 2 and 5).

**Receipts.** Blocks are promoted whether or not they have receipts: waiting would hold
promotion on the execution network. Receipts that arrive after a block was promoted are
attached in the archive (section 4b).

**When the range cannot be read**, the pipeline promotes now and backfills later: it promotes
the part of the range next to `S` that it can read and leaves the rest as a hole, because
everything from `S` down to the break is on `S`'s chain and therefore safe. It cannot fill the
hole itself; backfill belongs to the `el` crate.

- `MissingAncestor` (a block was never received or has expired): the blocks above the missing
  one are read again and promoted; the hole is from `C + 1` to the missing block. If `S`
  itself is missing, nothing is promoted.
- `AncestryTooLong` (more than 1024 blocks, what one call returns): the range is walked down
  from `S` one call at a time, each part's head being the parent of the oldest block read
  before, until it reaches `C`, a missing block, or 16 parts (`MAX_PROMOTED_PARTS`, 16,384 blocks).
  The parts are then promoted oldest first, each read again so that only one part's blocks
  are held at a time. Past the cap, the rest is the hole, which may still be in the unsafe
  store. Hourly dispute games move the safe head by about 1,800 blocks on OP Mainnet and 3,600
  on Unichain, so one ancestry call alone would leave a hole every time.
- The first block does not build on `C`: the range is promoted if it extends the archive.
  Nothing is missing, but the archived block at `C`'s height belongs to another chain (the
  reorg limit below).

Each part takes at most four `ancestry` calls (`MAX_RANGE_READS`); if none succeeds, the rest
of the range is the hole. Each hole is logged with its range and reason and counted, with only
the blocks actually left out; a promotion repeated after a crash counts its hole again. The
blocks above a hole do not extend the archive, so they are not archived and the recorded safe
head stays below the hole (step 5). Range sync fills it: the binary refuses the L1 side
without range sync, and holds the L1 heads back while the archive is far behind them; the
next change of the heads then appends what the unsafe store still holds.

**First safe head** (the archive has recorded none): the committed chain begins at `S`. Block `S` alone is promoted if the unsafe store has it; `S` is recorded either way and no
hole is counted. Older history belongs to backfill. Heads with no safe head (only finalized
known) are recorded and nothing else happens.

**The archive is never emptied by promotion.** It holds one contiguous range, and the
importer and range sync write to it too, below and above `C`. The rule for who writes where:
promotion appends at the tip, and only blocks that extend it; range sync and the importer own
everything else, including the gap below a promoted range that did not connect.

- A promoted range that does not extend the archive's tip (`NotContiguous`: the archive is
  behind, or holds another chain at that height) is not archived. It is logged at most once
  in ten minutes with the archive's tip and the block. Nothing is removed to make room.
  Blocks the archive already holds are skipped by `append_batch`, so a range that range sync
  or an import stored first is fine.
- Nothing removes blocks: the archive keeps every block (no retention window), and the heads
  only rise (step 1). Startup removes nothing either (section 5).

**Known limits:**

- *A safe head that jumps more than 16,384 blocks at once* (an L1 source catching up after
  downtime): only the newest 16,384 are promoted, even when the unsafe store has every block.
  The cap is a count of blocks, so it spans about 9 hours on OP Mainnet and 4.5 on Unichain.
- *An L1 reorg below the committed safe head*: a safe head can only be a block a bonded
  dispute game on L1 matched, and the heads only rise, so it is not followed: the archive
  keeps the committed chain. The operator repairs it.

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

**The archive never stays without receipts.** Promotion does not wait for receipts, so a few
blocks reach the archive without them. The archive lists them (`pending_receipts`, section 9
of `docs/storage.md`) until `set_receipts` fills them. At startup, then every 30 s, the task
asks for the receipts of 64 of them, continuing after the last block asked for and wrapping
round, so blocks no peer serves receipts for do not hold back the others, through the same
channel as new blocks
(`ReceiptsRequest` built from the archived header and its sender count, verified by `el`
against the receipts root as usual). The answers are attached in the archive like any late
receipts. A restart resumes from the list, which lives in the archive. The list's size is
logged at startup and kept in `op_indexer_pipeline_archive_pending_receipts`.

**Range** (`range.rs`, when a range sync is configured). The execution network hands over
verified blocks in ascending order, in batches of consecutive blocks, as the bytes it received
(`EncodedBlock`). For each batch the task decodes the blocks and recovers their senders on
blocking threads (32 blocks per thread; a legacy transaction signed with all zeros gets the
zero address), then appends the same bytes, with those senders, to the archive with
`append_batch`. It keeps no progress of its own: the archive's last
block is where the binary starts the sync again. Blocks the archive already holds (promotion
appended them first) are left out; a batch that does not extend the archive is skipped with a
warning, and the planner gives its round up. A batch that extends it is stored whole or the
pipeline stops with the error (`PipelineError::RangeBlock` for a block this build cannot read,
`PipelineError::Storage` for an archive that refuses the batch), because the archive is one
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

The heads only move up, across restarts too: the task starts from the archive's heads
(`ArchiveStore::heads`, read at startup) and never publishes one below them, so a
restart does not hand promotion an older head. A recorded finalized head above the safe one
(left by an earlier build's rollback) is not taken over: the
finalized head starts unknown and the next finalized match sets it. The heads go to promotion through the binary,
which holds them back while a range sync is still bringing the archive up to the chain.

Ingest also publishes the unsafe head on a `watch` (`Pipeline::with_head`), which the binary
gives to the execution network as the newest block the node knows.

## 5. Startup

1. Read `C` from the archive. If there is none, start both tasks.
2. Write the heads to the unsafe store (its journal may have been emptied by a layout change), prune it
   up to `C` (the last run may have stopped between the marker and the prune), and publish
   `C`'s number to `p2p`.
3. Start both tasks.

Nothing is deleted from the archive at startup. Archived blocks above `C` may come from a
promotion that stopped before its marker, or from an import or a range sync that reached
further; the two cannot be told apart, and deleting the second kind would discard work that
takes days. What a stopped promotion leaves is harmless: the repeat finds its blocks in the
archive, which skips them. Only if the safe chain is another one after the restart (a crash
between steps 4 and 5, then an L1 reorg) do the blocks of the stopped attempt stay at the
archive's tip, where later ranges then do not connect: the operator repairs the archive.


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

None (user decision, 2026-10-04: the metric modules were removed, to be re-added later where
needed). What an operator must see is logged: dropped blocks, reorgs, fills, retries,
promotion holes and root mismatches, game mismatches, and the archived blocks still without
receipts (when that number changes).

## 9. What has been verified

- **Ingest**: live, on mainnet gossip, with the Redis store this replaced (D0): blocks,
  transactions and senders were stored; reorg and fill events showed in the log and the event
  stream. Not yet run on the in-memory unsafe chain.
- **Promotion**: with a throwaway driver outside the repo that feeds safe heads trailing the
  unsafe head, including a step back (L1 reorg), a missing block, and a kill between each pair
  of steps. Not run on heads from the L1 side.
- **Receipts, range and commitment tasks**: compiled and reviewed; not run end to end.

## 10. Not done

The unsafe store's reconciliation when the safe head contradicts it (`docs/storage.md`, the
safe-head gap). Reading data back is the `stream` crate's.

## 11. Modules and API

| File | Holds |
|---|---|
| `lib.rs` | `Pipeline<U, A>`: `new(...)`, the builders `with_head`, `with_l1_games`, `with_range`, and `run(self, shutdown, cancel) -> Result<(), PipelineError>`. Runs startup (section 5), then the tasks; returns when they have stopped, or with the first fatal error after cancelling the others. |
| `ingest.rs` | The ingest task (section 3). |
| `recover.rs` | Sender recovery on a blocking thread. |
| `promote.rs` | Startup reconciliation and the promotion task (sections 4 and 5). |
| `receipts.rs` | The receipts task and `ReceiptsChannels` (section 4b). |
| `range.rs` | The range task: range-sync batches into the archive (section 4b). |
| `commit.rs` | The commitment task: verified dispute games checked against our blocks, into L1 heads (section 4b). |
| `retry.rs` | Storage's `retry` without a time limit, and `settle`, which ends a task on what it returns. |
| `error.rs` | `PipelineError`. |

`Pipeline::new` takes the unsafe store, the archive, the chain's Canyon time (for the receipts
roots promotion checks), the block receiver, the L1 heads receiver, the safe-number
sender and the receipts channels (`Option`, `None` without an execution network). The
builders add the head output (`with_head`), the dispute games and the L1 heads sender they
feed (`with_l1_games`), and the range-sync batches (`with_range`). The stores are `Clone + Send + Sync + 'static`.
