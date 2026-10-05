# Import spec (`bin/op-indexer-import`)

Status: **built; the whole OP Mainnet chain (blocks 0 to 157,745,023) has been downloaded
and verified on the real service, and Unichain's verified on the user's server; Base (8453) is
supported, its download is on the user's server (2.57 TB of downloaded chunks, 2026-10-05) and
it has not been verified yet (section 10). Two steps: `download` (with the fill from the
chain's RPC of what the service leaves out, section 3.1a), and `verify`, which checks every
block, seals the range into chunks and uploads them to object storage (section 3.2), which
servers read from (`docs/serving.md`). The former `export` step and the verified copy it read
were folded into `verify` on 2026-10-05. `load` (into the node's fjall archive) was removed on
2026-10-04 (section 4). ClickHouse is no longer part of the project.**

**Goal (user, 2026-10-04): sync the whole chain from HyperSync as a separate process, usable
for any chain, into the store that servers serve history from (R2 chunks since the "Four
binaries" decision); then test a normal p2p sync against those servers.**

OP Mainnet's blocks before the Bedrock upgrade (0 to 105,235,062) cannot be re-executed by a
modern EVM, and no execution peer we reached serves them (`docs/el-viability.md`). This binary
downloads a chain's blocks from Envio HyperSync, verifies every block against a trusted anchor,
and seals the verified bytes into chunks that it uploads to Cloudflare R2. It keeps each
downloaded chunk until the sealed chunks covering it are uploaded, so a stopped `verify`
continues without HyperSync.

It is a separate binary: the indexer never links it and never talks to an external service.
The HTTP client and the compression crates belong to this package only.

**Sources.** Envio HyperSync for the blocks, and, only for what HyperSync leaves out of some
rows, the chain's JSON-RPC endpoint, read-only (section 3.1a). Neither is trusted: every block
is rebuilt and proven by its header hash and the parent links up to the anchor.

## 1. What was measured (2026-10-04, 1,009 blocks)

- HyperSync has the whole range from block 0, with every header field. Each rebuilt header
  hashed to its block hash; the Bedrock block's parent hash equals the hash of 105,235,062.
- Every transaction re-encoded to its hash and every transactions root matched. Legacy blocks
  hold exactly one transaction, always type 0.
- Every receipts root matched with the plain legacy receipt encoding (status, cumulative gas
  used, bloom, logs).
- L1-to-L2 message transactions have a zero signature and HyperSync gives their sender as the
  zero address; the old client's metadata for them is not exposed.
- About 7 KB of JSON per block with all fields; a 1,000-block query took 1.3 s (73 ms of server
  time).

## 2. The constraint that shapes the design

The API token allows unlimited requests for **30 minutes from first use**; the window can be
reset. So nothing but downloading happens during the window, as few bytes as possible
travel, as little work as possible is done per byte, and the download resumes: one window is
not enough for the whole chain.

## 3. Phases

Each phase is a subcommand, works in a state directory (section 7), and can be stopped and run
again; a completed chunk is never redone.

### 3.1 `download`

- On its first run it decides the range and records it in `plan.json` (section 7).
- The range is cut into chunks: 1,000 blocks before the chain's Bedrock block, 100 from it on,
  where one block holds as much as many legacy ones (`--chunk-blocks`, and a tenth of it).
  Chunks are fetched with `--requests` requests in flight (default 64), each on its own
  HTTP/1.1 connection.
- A response may cover less than the range asked for; the chunk is complete only when every
  block of it has arrived.
- **Done chunks.** A chunk is done when its file is in `raw/`, or when the records of sealed
  chunks (`sealed/`, section 3.2) cover it whole: `verify` deleted its file once it was sealed
  and uploaded. Only the other chunks are fetched.
- **Stored as it travels.** The response body is written to the chunk's file exactly as
  received, in the content encoding the service chose (zstd is asked for first, then gzip),
  after one byte naming that encoding. Nothing is decompressed to be compressed again. The
  only work per byte is one streaming decode, into nothing, to read the cursor
  (`next_block`) at the end of the body. Memory is about a megabyte per request in flight,
  whatever a chunk holds.
- **Fields requested**: what the header, the transactions and the receipts with their logs
  are rebuilt from. Not requested, because `verify` computes them and the header hash proves
  them: the bloom filters, the transactions root, the receipts root and the transaction
  hashes. The old client's L1 fee fields are not requested: no root covers them and nothing
  reads them.
- **Errors**: a chunk that fails for a reason that may pass (HTTP 429, 408, 5xx, a broken
  connection, an answer cut short) is fetched again from its start, up to six times, with
  capped, jittered backoff. A 400, 401 or 403 fails at once. When a chunk fails for good, or
  free disk space falls below 16 GiB, no new chunk is started, the requests in flight finish
  and are written, and the phase ends with a summary of what is missing. It never spins.
- **Open files**: a request in flight holds a connection and a chunk file. At start the
  soft limit is raised to what `--requests` needs if the hard limit allows; otherwise the step
  refuses to start and prints the `ulimit -n` command. Running out of files while writing a
  chunk is retried like a busy service.
- **Progress**, every 10 seconds: chunks done, blocks, blocks per second over the last
  minute, requests in flight, bytes per second on the wire, the processor time spent decoding
  (`decode_cpu_percent`, 100 = one core), bytes on disk, free disk space, and `bytes_left` /
  `secs_left`. The time left is estimated from bytes, not blocks: the bytes per block of each
  era (before the Bedrock block, and from it on, where blocks are ten to thirty times larger)
  times the blocks left in it, over the speed on the wire. It is absent until an era with
  blocks left has been sampled, and within the second era it still grows as blocks do.

### 3.1a What the service leaves out, from the chain's RPC

Four gaps are known: two on Unichain, two on Base; OP Mainnet's whole chain verified without
any.

- **Authorization lists.** HyperSync sends EIP-7702 transactions (type 4) without their
  `authorization_list`; every other field is there. Found at block 16,068,511: its type-4
  transaction rebuilt with an empty list hashes the header to another value. The list is not
  optional (EIP-7702 refuses an empty one), so such a row cannot be rebuilt from the download
  alone.
- **Holes: whole blocks without their rows.** At 55,142,810 to 55,142,819 the answer has the
  ten header rows, matching the chain field for field, but no transaction and no log rows,
  while the chunk's other 90 blocks have theirs and the answer was not cut short (its cursor
  is the chunk's end). The chain has 8 transactions in 55,142,810. Every block after the
  Bedrock block has at least the L1-attributes deposit, so a block from there on without
  transaction rows is a hole: the whole block is fetched.
- **Header fields (Base).** HyperSync's Base rows lack header fields in large stretches before
  block 13.5 M. Of 52 chunks sampled across 0 to 52.19 M (2026-10-05), every block in the chunks
  at 0, 1.04 M, 2.09 M, 3.13 M, 6.26 M, 10.4 M and 11.5 M lacks `mix_hash` and
  `base_fee_per_gas`, 12.52 M lacks `mix_hash` only, and the chunks at 4.17 M, 5.22 M, 7.31 M,
  8.35 M, 9.39 M and from 13.57 M on are complete: millions of blocks, patchy. Neither field can
  be rebuilt (`mix_hash` is the L1 origin's randomness). For a block whose header row lacks any
  field of its forks, the header is fetched (`eth_getBlockByNumber` without transactions) and
  its fork's header fields are kept; whether the later forks' fields (`withdrawals_root`, the
  blob gas fields, the parent beacon block root) are missing there too shows in the scan's
  report, per field.
- **Deposit source hashes (Base).** In the same stretches the deposit rows lack `source_hash`
  (every deposit of the first 251 chunks, 9,404 of them, in the user's run of 2026-10-05). A
  deposit's encoding, so the transactions root, includes it. For a block whose deposits lack
  it, the block is read with its transactions (`eth_getBlockByNumber` with full transactions)
  and each deposit's source hash kept, by transaction index; the header fields come from the
  same answer, so such a block costs one call. With `--fill-from l1` (the default) they are
  rebuilt from L1 instead (`fill/derive.rs`): an L1-attributes deposit's from its L1 origin
  hash and sequence number, a user deposit's from its L1 block hash and its log's index in
  that block.
- **User deposits' `mint` (Base).** Base's rows also lack a user deposit's `mint` (first seen
  at block 1,322,905, three ETH deposits), which the deposit's hash covers. The rebuild from
  L1 takes it from the deposit's `TransactionDeposited` log (its opaque data packs mint,
  value, gas limit, creation flag and calldata). It also checks the row's `from`, `to`,
  `value`, `gas` and `input` against the log, and a difference stops the run, naming the
  field. Only an epoch's first block has user deposits, so only there is a missing mint
  rebuilt; an upgrade deposit mints nothing, which `verify` reads for a missing one. The RPC
  fill reads it with the source hash; a deposit the RPC gives without a mint mints nothing.
  The scan lists `mint` missing for every deposit after the first, upgrade deposits included.

- **Checked after every download.** Once the chunks are on disk, `download` reads every chunk
  not sealed yet (one per core at a time, within 256 MiB of downloaded bytes in flight, the
  bound `verify` uses too) and
  lists every field its rows lack that the
  rebuild needs, or fills with a default the header hash must then prove: header fields of
  the block's forks (`mix_hash` and `base_fee_per_gas` from Bedrock, `withdrawals_root` from
  Canyon, the blob gas fields and the parent beacon block root from Ecotone), the fields of
  each transaction type, a receipt's status, and a deposit receipt's nonce and version from
  Canyon. Each is logged once, at warn, with its count and its first block: one pass shows
  them all, where `verify` would stop at the first. Progress is logged every 10 seconds
  (chunks per second, time left). The pass costs what reading the rows costs `verify`
  (decompress and parse; no hashing, no signature, nothing written): by section 6's
  measurements about 20 ms of processor time per 1,000 legacy blocks, of `verify`'s 0.17 s.
  It runs again on every `download` over the chunks not sealed yet, and skips those the
  sealed records cover (their files are gone).
- **Left out** means: a type-4 row with no `authorization_list`, no bytes, or a list of zero
  entries (the service writes an empty list as a count of zero, as it does `access_list`), and
  no fill. The scan and `verify` use the same rule (`TransactionRow::lacks_authorization_list`).
- **Fetched as the scan goes**: what a chunk lacks is fetched as soon as the chunk is read,
  while the next ones are read. `eth_getBlockByNumber` without transactions for a header,
  with full transactions for a list (Unichain's public endpoint does not allow
  `eth_getTransactionByBlockNumberAndIndex`), and for a hole its receipts too, with
  `eth_getBlockReceipts` (else `eth_getTransactionReceipt` per transaction, for an endpoint
  without it). `--rpc-batch` calls per request as one JSON-RPC batch
  (`OP_INDEXER_IMPORT_RPC_BATCH`, default 10: Unichain's public endpoint refuses larger
  batches, with one error for the whole batch, which is reported as a refusal with its
  message), `--rpc-requests` chunks' requests at a time (`OP_INDEXER_IMPORT_RPC_REQUESTS`,
  default 4). The defaults suit a public endpoint; a provider of your own may take more of
  both. Progress is logged every 10 seconds: chunks read, blocks fetched and found, blocks per
  second, the time left for the blocks found so far. A request is
  retried up to six times with `download`'s capped, jittered backoff on a busy endpoint (408,
  5xx), a broken connection or a malformed answer; on a rate limit (429) it waits what
  `Retry-After` asks (up to 10 minutes), else 15 s doubling to 2 minutes, minutes in all.
  Ctrl-C drops the requests in flight at once. A refused call, a block the endpoint does not
  have, or a block hash that is not the downloaded one (an endpoint of another chain) fails at
  once, naming the endpoint.
- **Resumable**: a chunk's fill is written once its requests are all answered, and the scan
  reads the fill with the rows, so a chunk whose fill holds what it lacks fetches nothing on
  the next run. A run stopped part way loses at most the chunks being fetched. Each run reads
  every chunk not sealed yet again (decompress and parse), which on Base's 2.57 TB takes a
  while; the fetching goes on meanwhile.
- **Kept apart.** What is fetched goes to the chunk's fill, `raw/<from>-<to>.fill.json`,
  written atomically and durably, in the RPC's JSON form (a list per transaction; a hole's
  transactions and receipts with their logs; a header's fork fields, without its hash: about
  150 bytes a block before Canyon, 400 after Ecotone); the downloaded chunk stays as
  received. A
  chunk downloaded again loses its old fill first. `verify` reads no RPC: it puts the fill
  into the rows before rebuilding, a list into its transaction, a hole's transactions and
  logs as rows like the service's (the receipt's fields with the transaction's, the access
  list in the service's layout), a header field into the header row where the row lacks it
  (what the service sent is kept). A type-4 row with no list, or a block after Bedrock with no
  transactions, and no fill, fails `verify` with a message to run `download`, not the generic
  hash mismatch. A fill is deleted with its downloaded chunk once that chunk is sealed.
- **Trust unchanged.** What is filled goes into the rebuilt block; the transactions and
  receipts roots, so the header hash, prove it. A hole's senders are the endpoint's `from`,
  which `verify` recovers from the signatures and checks like every other. A wrong fill fails `verify` like a wrong row (checked: one
  changed signature byte gives a header hash mismatch).
- **Endpoint**: `--rpc-endpoint` (`OP_INDEXER_IMPORT_RPC_ENDPOINT`), by default
  `https://mainnet.unichain.org` for Unichain, `https://mainnet.base.org` for Base, and none
  for OP Mainnet, whose rows need none so far; without one, `download` stops with a message
  when something is missing. Only its scheme and host are shown, in logs and errors alike (a
  provider's key is in the path or the query: the rest shows as `/…`), and the HTTP client's
  errors are kept without the URL.
- **Counted**: `blocks_to_fetch`, `blocks_fetched`, `rpc_filled_transactions` and
  `rpc_filled_headers` in `download`'s summary, and one warning per missing field with its
  count and first block (counted before that run's fills); `rpc_filled_transactions` (lists
  filled and hole transactions added) in `verify`'s; each hole is also a
  `block`/`transactions` line of the missing-field report.
- Not done: asking HyperSync again for a hole's chunk before using the RPC; the RPC answer is
  proven the same way.

**Header fields rebuilt from L1** (`--headers-from l1`, the default;
`OP_INDEXER_IMPORT_HEADERS_FROM`; `rpc` fetches them all as above). Reading Base's millions of
headers from a public endpoint takes days, while each missing field follows from what the
download and L1 have (`bin/op-indexer-import/src/fill/derive.rs`):

- **`mix_hash`** is the L1 origin's `mix_hash` (`prevrandao`), and from Ecotone on the **parent
  beacon block root** is the L1 origin's own `parent_beacon_block_root`. The L1 origin is named
  by the block's L1-attributes deposit (its first transaction): its number at bytes 28..36 of
  the calldata and its hash at 100..132, the same bytes in the Bedrock form (ABI words) and in
  the packed forms of Ecotone, Isthmus and later. The L1 header read for that number must have
  that hash. L1 headers come from L1's HyperSync (`--l1-endpoint`, the API token's), a span of
  at least 10,000 L1 blocks per request (about a week of L2 blocks), paged and retried like
  the rest.
- **The base fee** follows from the parent's by EIP-1559: the gas target is the gas limit over
  the elasticity (6), the change at most one denominator-th per block (50, and 250 from Canyon:
  `ChainSpec::eip1559`), and from Holocene on with the elasticity and denominator in the
  parent's `extraData`. Computed in block order across chunks, from the block before the run's
  first (the chunk read before it, or, when that chunk is sealed and gone, that one header
  from the RPC). Not from Jovian on (its minimum base fee and data footprint are not rebuilt):
  those blocks go to the RPC.
- **The withdrawals root** from Canyon is the empty trie's root until Isthmus; from Isthmus it
  is the message passer's storage root, which cannot be rebuilt (RPC). **The blob gas used**
  and **the excess blob gas** from Ecotone are zero (the blob gas used until Jovian, which makes
  it the data availability footprint: RPC).
- **Checked before written.** The chunks are read in parallel and rebuilt in block order. A
  chunk's rebuilt fields are kept in memory while the chunk is rebuilt whole, as `verify` does,
  with them on top of its fill (on every core, within the same 256 MiB bound); only if every
  block hashes are they written to the fill. If a block does not hash, the chunk's headers are
  fetched from the RPC instead; without an endpoint the run stops naming the chunk and the
  block. Nothing unchecked is written, so a stopped run leaves nothing behind that would hide
  a missing field from the next scan. A chunk that needs the RPC for anything else (a block
  whose field cannot be rebuilt, as above, an L1 origin not found, a list, a hole) has all its
  header fields fetched with it.
- **One base fee leads to the next**: a stretch without base fees is rebuilt from the block
  before it, block after block, so a block that cannot be rebuilt (or one wrong, caught by its
  chunk's check) leaves the rest of its chunk to the RPC; the next chunk starts again from a
  parent read from the RPC.
- **Counted**: `l1_rebuilt_headers` and `l1_rebuilt_not_hashing` in `download`'s summary.
- **Checked against Base**, 2026-10-05, from its public endpoint and an L1 one (not through the
  importer): blocks 1.04 M (Bedrock deposit, denominator 50), 11.5 M (Canyon, empty-trie
  withdrawals root), 12.52 M (Ecotone deposit, parent beacon block root, blob gas zero) and
  30 M (Holocene `extraData`): every field as rebuilt here, every L1 origin hash matching.
- **Run through the importer**, 2026-10-05, on OP Mainnet blocks 140,000,063 to 140,000,162
  (Isthmus deposits, Holocene base fees) with `mix_hash`, the base fee and the parent beacon
  block root taken out of all 100 header rows, and L1 HyperSync replaced by a local stub
  answering with the 16 L1 headers recorded from a public L1 endpoint (this machine may not
  call HyperSync): one L1 request, 100 headers rebuilt and all hashing, the fill pass 0.49 s
  with its check (one RPC call: the first block's parent), `verify` accepted the chunk. With one
  recorded L1 `mix_hash` corrupted, the check caught block 140,000,090 and the chunk's headers
  were fetched from the RPC instead (3.3 s), then `verify` accepted it; without an endpoint the
  run stopped with the message. Not run: against L1 HyperSync itself, on Base's own rows, a
  Bedrock-form or pre-Holocene chunk through the importer (the rules for those are the ones
  checked against Base above), or from Jovian on.

Checked on 2026-10-04, offline but for the endpoint: a chunk of block 16,068,511 built from
the endpoint's block and receipts in HyperSync's row format, without `authorization_list`.
`verify` failed with the message to run `download`; `download` fetched the list and wrote the
fill; `verify` then rebuilt the header to the block's hash 0xb8a5…c3e6 and accepted the range.
A second `download` fetched nothing. The same with the row's list written as a count of zero.
Holes, the same way: block 55,142,810 with its header row only (no transaction or log rows)
failed `verify` with the message to run `download`; `download` fetched the block and its
receipts in one batch (8 transactions, 6 logs) and `verify` rebuilt the header to its hash
0x8f43…ca26. Block 16,068,511 as a hole (its type-4 transaction, with its authorization list
and access list, coming from the endpoint) verified too. The receipt-by-receipt fallback has
not run: Unichain's endpoint has `eth_getBlockReceipts`. Not checked against HyperSync's own
Unichain answers. (These checks ran with the build of the time, whose `verify` wrote a
verified copy instead of sealing.)

Deposit source hashes, checked on 2026-10-05: the same chunk with, besides `mix_hash` and
`base_fee_per_gas`, the `source_hash` of all 103 deposits taken out. `verify` stopped at the
first block; `download` reported the three fields (`deposit`/`source_hash` 103 times), read the
100 blocks with their transactions (100 calls: the headers came with them) in 8.1 s, and filled
103 source hashes and 100 headers; a second `download` fetched nothing; `verify` accepted the
chunk (`rpc_filled_transactions=103`). An endpoint given as `http://127.0.0.1:18998/<key>?token=<key>`
showed as `http://127.0.0.1:18998/…` in the log and the error, and the key nowhere.

Header fields, checked on 2026-10-05: a chunk of OP Mainnet (blocks 140,000,063 to
140,000,162) with `mix_hash` and `base_fee_per_gas` taken out of all 100 header rows.
`download` (every chunk present: no request to HyperSync) went straight to the scan, reported
both fields missing 100 times, and fetched the 100 headers from `https://mainnet.optimism.io`
in 2.8 s (10 calls a request, 2 requests at a time: about 35 blocks a second); the fill is
41 KB. A second `download` fetched nothing. `verify` rebuilt every header to its hash, sealed
the chunk and accepted it at its anchor. Not run against Base's own rows or endpoint.

### 3.2 `verify`: check, seal and upload

`verify` is the one step that checks the downloaded blocks and puts them in object storage.
It reads the downloaded chunks only (never HyperSync or the chain's RPC) and writes sealed
chunks, their manifest and the global hash index, in the layout `docs/serving.md` describes
(sections 1 to 3), to Cloudflare R2 or a local directory, so a `server` can read the history
from there. It is the importer's only path to object storage, through `crates/chunks`. It
replaced the pair `verify` (which wrote a verified copy of every chunk to `<state>/verified/`)
and `export` (which sealed and uploaded that copy) on 2026-10-05 (user decision): Base is 2.57
TB of downloaded chunks on a disk with 0.99 TB free, and the copy would not have fit.

In order:

1. **Check**, on every core (`--threads`, default one per CPU). For every block of a downloaded
   chunk it rebuilds the transactions and the receipts from the downloaded rows (with the
   chunk's fill, section 3.1a), computes the transactions root over the transaction encodings
   and the receipts root over the receipts, rebuilds the header with those roots and the bloom
   of the logs, and requires the header to hash to the block's hash and to name the previous
   block as its parent. Then it checks every sender (below).
2. **Seal**, in block order: a `ChunkWriter` (`crates/chunks`) turns the checked blocks into the
   chunks of `docs/serving.md` section 1. A chunk ends at 256 MiB uncompressed, at 100,000
   blocks, before the Bedrock block, or at the range's last block. The cuts depend on the
   blocks only, so the same range always gives the same chunks, those `export` produced.
3. **Upload**: each chunk goes to R2 (or `--to-dir`), `--uploads` at once (default 4).
4. **Record**: once a chunk's upload succeeds, its manifest entry is written to
   `<state>/sealed/<first>-<last>.json`, in block order, and the downloaded chunks the records
   now cover whole are deleted, with their fills.
5. **At the end**: the last block is checked against the anchor (section 11). Only then are
   the chunks listed in the manifest (segments of 16 chunks) and a new generation of the hash
   index written.

- **What is proven** for every block: its header hashes to its block hash and links to its
  parent up to the anchor; its transactions root is the root over the stored transaction
  encodings; its receipts root is the root over the stored receipts; the sender of every
  signed transaction is the one recovered from its signature, and a deposit's is its `from`.
  The first three are what a peer checks when the bytes are served to it.
- **Header fields `verify` rebuilds.** The transactions root, the receipts root and the logs
  bloom are never downloaded: `verify` computes them from the transactions, receipts and logs,
  and the header hash proves them (they are not compared with anything the service says).
  `mix_hash` is downloaded, but the service leaves it out of some pre-Bedrock rows (seen on
  OP Mainnet in chunks around block 47,705,000, 47,740,000 and 47,745,000). Before Bedrock
  every header has a zero `mix_hash`, so a row without it is rebuilt with zero, and the
  header hash decides: a wrong guess is refused, never accepted. The blocks rebuilt this way
  are logged per chunk and counted in the summary (`rebuilt_header_fields`). From Bedrock on
  a row without `mix_hash` is refused ("the header lacks `mix_hash`").
- **Senders.** The sender recorded with each transaction is the `from` HyperSync reports;
  `verify` proves it before the block is sealed (user decision, 2026-10-04, then in
  `export`): for every signed transaction it recovers the sender from the signature and
  compares; for a deposit it compares with the `from` in the deposit's encoding, which the
  transactions root and so the block hash cover. A mismatch stops `verify` before that block
  is sealed, naming the block, the transaction's index and both addresses. So every sender
  in the chunks is proven (recovered, or hashed for deposits) except one kind: a legacy
  transaction signed with all zeros (an L1-to-L2 message of OP Mainnet's client before
  Bedrock) has no signer, keeps the reported zero address, and is counted
  (`zero_signature_transactions`). The recovery runs on libsecp256k1 (alloy's `secp256k1`
  backend), which does it in about a fifth of the time of k256: about 32 µs per transaction on
  one core, so a whole chain is about 13 CPU-hours, spread over every core.
- **Links, then the anchor.** The links are checked as the chunks are sealed: inside a chunk
  by the writer, between chunks by each chunk's first parent against the last hash before it,
  and the first chunk against the manifest's last chunk when the range continues one the
  manifest lists (for example a catch-up of a chain whose earlier range is in the bucket). A
  broken link stops the run at once, naming the block. The anchor is checked once the whole
  range is sealed: a hash is compared with the last chunk's last hash; a dispute game's claim
  needs the last block's header, which is read back from the store (one GET of the chunk's
  index, one of the block's segment). Only when it matches are the chunks listed. If it does
  not, nothing is listed: the chunks uploaded stay in the bucket, unlisted and
  content-addressed, and nobody reads them.
- **A downloaded chunk that fails a check** is kept, nothing from its first failing block on
  is sealed, and the run stops with the block number, the check and the file. If the data is
  wrong, delete the file (and its fill, if there is one) and run `download` again; a row left
  without a field says to run `download`, which fills it (section 3.1a). With the roots not
  downloaded, a wrong transaction, receipt or log shows as the header hash not matching, for
  its block. A chunk not downloaded at all stops `verify` before anything is read, naming its
  blocks and its file.
- **Resumable.** A restart continues after the last record in `sealed/` with a fresh
  `ChunkWriter`, which cuts the same chunks an unbroken run would. The records are checked
  first: they must follow each other and the manifest's last chunk without a gap or a broken
  link. One listing of the bucket at the start finds the chunks it already holds (a run
  stopped between upload and record, or an earlier run), and those are not uploaded again:
  their names carry their root, and a PUT is create-only anyway. Downloaded chunks a record
  covers but a stopped run did not delete are deleted at the start. When the manifest already
  lists the whole range and the hash index covers it, `verify` says so and exits. Ctrl-C lets
  the uploads in flight finish and records them; nothing is listed until a run reaches the end.
- **The hash index.** Every block's hash and number go to the index's input as the blocks are
  sealed; a resumed run first reads them back from the chunks the manifest lists and the
  records name (one GET of each chunk's index, 32 at once). At the end the index is written
  as a new generation of the manifest, covering every listed block.
- **Layout**: one bucket per chain, `<chain>-snapshot` (`op-snapshot`, `unichain-snapshot`,
  `base-snapshot`), and the folder `archive/` in it: `archive/chunks/…`,
  `archive/manifest/…`, `archive/index/…`. Both can be changed (`--r2-bucket`,
  `OP_INDEXER_R2_BUCKET`; `--r2-prefix`, `OP_INDEXER_R2_PREFIX`). The manifest records the
  chain (id and genesis hash), and a run of another chain refuses it. The manifest's exporter
  id is `import` (it was `import-export`); nothing reads it but people.
- **Target**: R2, from `OP_INDEXER_R2_ACCOUNT_ID` (`--r2-account-id`),
  `OP_INDEXER_R2_ACCESS_KEY_ID` and `OP_INDEXER_R2_SECRET_ACCESS_KEY`, and optionally the bucket,
  the prefix and `OP_INDEXER_R2_ENDPOINT` (`--r2-endpoint`, another S3-compatible store). The
  keys are read from the environment, hidden from `--help`, and never logged. Or
  `--to-dir <dir>`: a local directory with the same layout, for a check or the bench without
  credentials.
- **Uploads**: one PUT per chunk (about 20 to 40 MB), create-only as a guard; an object above
  64 MiB goes up in parts. No HEAD is sent. Failed requests (timeouts, connection errors, 5xx)
  are retried with exponential backoff, from 200 ms to 30 s, up to 10 times or 3 minutes.
- **Progress** every 10 seconds: blocks done of the range, transactions, chunks sealed (and
  those skipped as already uploaded), blocks per second, MB/s of downloaded chunks read and
  MB/s uploaded (speeds over the last minute), uploads in flight, and the time left from the
  downloaded bytes still to read. A start line gives the range, the chunks sealed before and
  the free space, then one the chunks to read, their bytes and the chunks the bucket already
  holds; an end line gives the totals (blocks, transactions, `senders_recovered`,
  `zero_signature_transactions`, `rebuilt_header_fields`, `rpc_filled_transactions`, chunks,
  skipped, bytes uploaded).
- **Memory**: up to `--threads` downloaded chunks are checked at once, within 256 MiB of
  downloaded bytes in flight (one chunk is always allowed); a chunk takes about twenty times
  its downloaded size while it is checked, so that is roughly 5 GB at most. The chunk being
  sealed (up to 256 MiB uncompressed) and the chunks uploading are held in memory too.
- **Disk**: no copy of the range is written. Besides the downloaded chunks still to seal, the
  only extra disk is the hash index's input, `<state>/index-build/`: 16 bytes per block (about
  2.5 GB for OP Mainnet), emptied at the start of each run and removed once the index is
  written. The downloaded chunks shrink as the run goes. `verify` refuses to start with less
  than 16 GiB free. An old `<state>/verified/` directory is not read and can be deleted.

## 4. The local history store (removed)

`load` appended the verified blocks to the node's fjall archive through a bulk path
(`FjallArchive::bulk_append`); both were removed on 2026-10-04, when the history moved to R2
chunks (`verify`, section 3.2; `docs/serving.md`). Its measurements are in section 6.

## 5. Running it

The importer is a self-contained command-line tool, meant to be built here and run on another
machine: `cargo build --release -p op-indexer-import` produces one file to copy.

- Subcommands: `download`, `verify`, and `run` for both in order.
- `--api-token <TOKEN>` carries the HyperSync token; `OP_INDEXER_IMPORT_API_TOKEN` in the
  environment is the fallback (`ENVIO_API_TOKEN`, its former name, still works, with a
  warning). A flag is visible in the process list and the shell history, the variable is
  not. The token is never logged and never written to the state directory.
- Every other setting is a flag with an environment fallback and a default: the state
  directory (`OP_INDEXER_IMPORT_STATE_DIR`), the chain (`OP_INDEXER_CHAIN_ID`), the
  endpoints (`OP_INDEXER_IMPORT_ENDPOINT`, `OP_INDEXER_IMPORT_L1_ENDPOINT`,
  `OP_INDEXER_IMPORT_RPC_ENDPOINT`), the range (`OP_INDEXER_IMPORT_FIRST_BLOCK`,
  `_LAST_BLOCK`, `_ANCHOR_HASH`, `_LEGACY_ONLY`), the chunk size
  (`OP_INDEXER_IMPORT_CHUNK_BLOCKS`), requests in flight (`OP_INDEXER_IMPORT_REQUESTS`), the
  RPC's batch size and requests in flight (`OP_INDEXER_IMPORT_RPC_BATCH`,
  `OP_INDEXER_IMPORT_RPC_REQUESTS`; section 3.1a), where missing header fields come from
  (`OP_INDEXER_IMPORT_HEADERS_FROM`, `l1` or `rpc`), and for `verify`: `--to-dir`, the bucket and its keys (`OP_INDEXER_R2_ACCOUNT_ID`,
  `OP_INDEXER_R2_BUCKET`, `OP_INDEXER_R2_PREFIX`, `OP_INDEXER_R2_ACCESS_KEY_ID`,
  `OP_INDEXER_R2_SECRET_ACCESS_KEY`, `OP_INDEXER_R2_ENDPOINT`; section 3.2), its threads
  (`--threads`, `OP_INDEXER_IMPORT_VERIFY_THREADS`, one per CPU) and its uploads (`--uploads`,
  `OP_INDEXER_IMPORT_VERIFY_UPLOADS`, 4, from 1 to 64).
- **The importer fills the bucket servers read history from, and needs no database.**
- What the bucket holds is asked of the bucket: `verify` continues after the manifest's last
  chunk and the records in `sealed/`, and refuses a manifest of another chain.
- `verify` exits with an error if it stops before the end of the range, and says what to run.

### How to run it

Build on any machine with the Rust toolchain and copy the one file:

```bash
cargo build --release -p op-indexer-import
```

The file is `target/release/import` (the package is `op-indexer-import`). On the machine that runs it, with the defaults
(OP Mainnet, from block 0 to the last block committed to L1, state in `./import-state`):

```bash
import download --api-token <TOKEN> --requests 64
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import verify
```

`import --help` and `<command> --help` list every flag, its environment variable
and its default. Only `--state-dir` is shared by the steps: the range is decided by the first
`download` and read from `plan.json` afterwards.

- **The range**: with no range flags, block 0 to the L2 block of the newest dispute game of
  the chain on L1 (section 11). `--legacy-only` ends at the last block before Bedrock with its
  known hash and needs no lookup; `--last-block <n> --anchor-hash <hash>` gives any end whose
  hash you trust; `--first-block` any start. These are read on the first `download` only; on a
  later run a range flag that disagrees with the recorded plan is refused. To import another
  range, or to extend to a newer game, use an empty state directory.
- **State directory**: section 7.
- **Stopping and restarting**: Ctrl-C or SIGTERM stops a step within a fraction of a second
  (`verify` first finishes and records the uploads in flight) and exits with status 1 and a
  summary; `run` does not start the next step. Run the same command again: it continues with
  exactly the chunks that are missing. A killed process (SIGKILL, power loss) is as safe: a
  file is written under a `.tmp` name, synced and renamed, so a file with a final name is
  always complete, and leftover `.tmp` files are removed at the next start. A rename lost to a
  power loss only loses that file: a downloaded chunk is fetched again; a lost record means its
  chunk is sealed again from the downloaded chunks, which are deleted only after the record is
  durable, and not uploaded again, being in the bucket. The directory of the JSON files (plan,
  fills, records) is synced after each is written.
- **One process per state directory**: the directory is locked while a process runs; a second
  one exits with "another `import` process is using this state directory". The lock
  is released by the system when the process ends, however it ends.
- **Resuming**: when the request window closes, `download` stops with a summary of how many
  chunks are missing. Reset the window and run the same command again: only missing chunks are
  fetched. Ctrl-C stops any step cleanly; run it again to continue.
- **A chunk that fails `verify`**: the error names the block, the check and the file. Delete
  that file from `raw/` (and its fill) and run `download` again.
- **Disk**: a downloaded legacy chunk is 0.7 to 1.0 KB per block (zstd or gzip, as the
  service sends it): roughly 75 to 105 GB for the legacy range if the sample of section 6 is
  typical. The whole OP Mainnet chain was 589 GB downloaded (section 6). `verify` writes no
  copy: it deletes each downloaded chunk as soon as the sealed chunks covering it are uploaded
  and recorded, so the downloaded bytes shrink as it goes, and `download` counts a chunk the
  records cover as done, so a later `download` or `run` on the same state directory does not
  fetch it again.

## 6. Measured

Offline, on 1,000 saved blocks (50,000,000 to 50,000,999, one transaction each), Apple M-series,
with an earlier build, whose `verify` wrote a verified copy:

- Size: 4.7 MB of JSON with the fields requested; 0.75 MB as zstd, 1.0 MB as gzip.
- `verify`, one thread: 0.17 s of processor time per 1,000 blocks with sender recovery (what
  `verify` does now, before sealing), about 0.06 s without: reading and decoding 6 to 8 ms,
  parsing 10 to 13 ms, receipts and their blooms 8 ms, the two tries 9 ms, header, body and
  receipts encoding 5 ms, transactions 1 ms, writing the verified chunk with its sync 17 to
  22 ms (on macOS; a sync is cheaper on Linux; no longer done).
- Projection, not a measurement: 105 million legacy blocks at 0.17 s per 1,000 are about 300
  core-minutes, under 40 minutes on 8 cores if the disk keeps up, sealing not counted.

On the real service:

- 2026-10-03, 8 cores, 1 Gbit, an earlier build: the legacy range downloaded at about
  185,000 blocks per second with 64 requests in flight.
- The whole chain, a 32-core server: `download` of blocks 0 to 157,745,023 (157,745,024
  blocks, 589 GB) in about 25 minutes at 300 to 380 MB/s; `verify` of the build of the time
  (verified copy, no sender recovery) accepted the whole chain (630,336 chunks, linking 58 s),
  its top block matching the claim of the newest dispute game (a type 9 super game); the bulk
  load into the fjall archive (since removed, section 4) ran at 190,000 to 235,000 blocks/s,
  580 to 665 MB/s of RLP, on a volume `dd` measured at 325 MB/s of sequential writes.

The sealing, on the local sample of 20 downloaded chunks (OP Mainnet blocks 140,000,063 to
140,002,062, 2,000 post-Isthmus blocks, 26.8 MB downloaded, a hash anchor), `--to-dir`:

- With the `export` step (since folded into `verify`), 10 cores: sealing at about 5,400
  blocks/s (400 MB/s of records, senders recovered), one 20.5 MB chunk, and the index's 4,096
  shards in 1.6 s. Read back byte for byte equal (`docs/serving.md` section 3).
- 2026-10-05, `verify` (check, seal and upload in one step), release build: 2,000 blocks and
  50,368 transactions verified, 48,357 senders recovered, sealed into one chunk of 20.5 MB in
  0.64 s; the hash index in 1.7 s; 3.0 s wall time in all; peak RSS 680 MB; all 20 downloaded
  chunks deleted. The chunk object is byte-identical to the one `export` produced
  (`000140000063-000140002062-5ccbb6a645147824.opxc`), and the manifest segment identical but
  for its timestamp and exporter id.
- 2026-10-05, the same sample with a wrong anchor: the chunk was uploaded, the downloaded
  chunks deleted, and the run stopped with nothing listed. Run again with the right anchor, it
  continued from the record (nothing sealed again), listed the chunk and wrote a byte-identical
  index.
- 2026-10-05, the same sample with one downloaded chunk damaged: the run stopped naming its
  blocks and its file, with all 20 downloaded chunks kept and nothing uploaded.

## 7. The state directory

```text
<state>/plan.json                   the chain, the range, its anchor and the chunk size
<state>/raw/<from>-<to>.raw         downloaded chunk: the service's answers as they travelled
<state>/raw/<from>-<to>.fill.json   fields the service left out, from the chain's RPC (3.1a)
<state>/sealed/<first>-<last>.json  a sealed chunk `verify` uploaded: its manifest entry
<state>/index-build/                `verify`'s hash index input while it runs (removed at the end)
<state>/lock                        held by the one process working on the directory
```

- `plan.json` is written by the first `download` and never changed: chain id, first and last
  block, the anchor (a trusted hash, or the dispute game found on L1) and the chunk size. Every
  later run of any step reads it; `verify` takes no range flags. Files already written were cut
  by it, which is why it cannot change.
- A downloaded chunk and its fill are deleted once the records in `sealed/` cover it whole;
  `download` and its fill pass treat such a chunk as done.
- A directory written by a build with another layout is refused: it either has chunks and no
  `plan.json`, or a `plan.json` with another version. Delete it and download again. The
  layout version did not change when `verify` and `export` were merged: a state directory of
  the earlier build (Base's) keeps working, and its `verified/` and `verified.json` are not
  read and can be deleted.
- A file exists only when it is complete: it is written under a `.tmp` name, synced and
  renamed; leftover `.tmp` files are removed at the next start.
  Three short verified files (under their 64-byte header) were seen with an earlier build, on
  a server filled from a snapshot. How they came about is not known: this code syncs the data
  before the rename, so a crash cannot leave a short file under the final name; the copy of
  the snapshot is the likelier cause.

## 8. Not built

The senders and L1 metadata of L1-to-L2 messages; any source other than HyperSync; the binary
(Arrow) format of HyperSync, which measured offline would save about 20% on the wire over the
compressed JSON and was put aside; reading the deposit contract's logs on L1.

## 9. From the Bedrock block onward

Every block from Bedrock on starts with a deposit, and bridged deposits follow. `verify`
rebuilds legacy, EIP-2930, EIP-1559, EIP-7702 and deposit transactions, their receipts, and
headers of every fork (base fee, withdrawals root, blob fields, beacon root, and from Isthmus
the hash of an empty requests list, which has no column in HyperSync).

- **Deposits are rebuilt from HyperSync's `source_hash` and `mint` columns**, or what
  `download` filled where they lack them (from L1 or the RPC). A deposit without a source hash
  fails `verify` with a named check; one without a mint is read as minting nothing, which the
  header hash proves. On OP Mainnet the endpoint fills them for every deposit: the whole chain
  verified (section 6).
- The system-transaction flag, which has no column, follows the protocol's rule: only the
  L1-attributes deposit before Regolith has it.
- A deposit receipt carries the sender's nonce and the receipt version from Canyon on, when
  they became part of the hashed receipt; before Canyon nothing the root does not cover is
  stored.
- Fork times and the Bedrock block come from `op-indexer-chainspec`.
- `access_list` and `authorization_list` arrive as the bytes of the service's binary column
  (a hex string in the JSON), decoded when the transaction is rebuilt (`verify/lists.rs`).
- Verified on real data: the whole OP Mainnet chain, so every transaction type, user deposits
  and every fork up to the newest dispute game (section 6). A block that cannot be rebuilt and
  verified is not imported.

## 10. Any chain

The chain is chosen with `--chain <id>` on the first `download` and recorded in `plan.json`;
a later run with another `--chain` is refused, and one without it continues the recorded
chain. Its parameters (fork times, the Bedrock block and its time, the hash of the last
legacy block if it has a legacy chain, the block time, the dispute-game factory) come from
`op-indexer-chainspec`, which knows OP Mainnet (10), Unichain (130) and Base (8453). The
chain is also `OP_INDEXER_CHAIN_ID`, the variable the node reads, so one `.env` sets it for
both. The HyperSync endpoint is the importer's concern, not the chain specification's: a table
in the importer gives `https://optimism.hypersync.xyz` for 10, `https://unichain.hypersync.xyz`
for 130 and `https://base.hypersync.xyz` for 8453,
`--endpoint` overrides it, and a chain without an entry needs `--endpoint`. The L1 endpoint
(`--l1-endpoint`, default Ethereum's) is where the dispute games are looked up.

### Unichain (chain 130)

```bash
import --state-dir unichain-state download --chain 130 --api-token <TOKEN>
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import --state-dir unichain-state verify
```

What differs from OP Mainnet:

- **No legacy chain.** Unichain began with Bedrock at its genesis (block 0, time
  1730748359): every chunk is a post-Bedrock chunk, a tenth of `--chunk-blocks` (100 blocks
  by default), and `--legacy-only` is refused. Block 0's parent hash is zero and its header
  already has the fields of every fork through Granite, which are all active at genesis; the
  transaction and receipt rules are chosen by each block's timestamp as for OP Mainnet.
- **One-second blocks**, so about 60 million blocks today and about 600,000 downloaded chunks
  at the default size, about as many as OP Mainnet's. The sealed chunks are cut by size and
  block count, not by downloaded chunk, so there are far fewer of them. Unichain blocks are
  small, so `--chunk-blocks 10000` (1,000 blocks per chunk) means fewer requests and files; it
  is recorded in the plan and cannot change later.
- **The endpoint** is `https://unichain.hypersync.xyz`, the host the service's naming gives.
  It has not been reached from here; if it is wrong, give the right one with `--endpoint`.
- **EIP-7702 authorization lists** are missing from HyperSync's rows; `download` fetches them
  from `https://mainnet.unichain.org` (section 3.1a). A state directory downloaded before
  this needs one more `download`, which downloads nothing and fills what is missing.
- **The top anchor** is the newest dispute game of Unichain's own factory
  (`0x2F12d621a16e2d3285929C9996f478508951dFe4` on Ethereum): super games (type 9), whose
  claim is read for chain 130, with the timestamp turned into a block at one block a second
  from genesis.
- **A later range**, from where the bucket's manifest ends to a newer game, goes in an empty
  state directory with `--first-block` set to the block after the manifest's last; `verify`
  checks that its first block names the manifest's last hash as its parent before anything is
  listed.

**Run so far** (by the user, reported 2026-10-04): a download of 604,009 chunks, and a
`verify` that stopped at block 16,068,511 on the missing authorization list (section 3.1a).
Since then the range was verified on the user's server (reported by the user, 2026-10-04), by
the build that wrote a verified copy, and exported to `unichain-snapshot` by that build's
`export` (1,693 chunks, blocks 0 to 60,400,897). On 2026-10-05 a catch-up import of blocks
60,400,898 to 60,422,316 (anchored on a dispute game, no field missing) was appended as one
more chunk, its first block linked to the manifest's last.

### Base (chain 8453)

```bash
import --state-dir base-state download --chain 8453 --api-token <TOKEN> --rpc-endpoint https://mainnet.base.org
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import --state-dir base-state verify
```

What differs (`docs/base.md`):

- **No legacy chain**, as Unichain: Bedrock at block 0, every chunk 100 blocks by default.
  About 52 million blocks (2 s blocks), so about 520,000 downloaded chunks: fewer than OP
  Mainnet's 630,336.
- **The endpoint** is `https://base.hypersync.xyz` (the service's list of networks). The
  user's download of blocks 0 to 52,166,660 completed on it on 2026-10-05 (521,890 chunks,
  2.57 TB).
- **Authorization lists are in HyperSync's Base rows**, unlike Unichain's: two type-4
  transactions (block 49,194,764 index 82, block 49,194,364 index 73) carry the one entry the
  public RPC shows (checked 2026-10-05). A spot check, not the whole range: `verify` is what
  proves every block.
- **Base's own forks** (Azul, Beryl, Cobalt) change nothing the import rebuilds: the header,
  the transaction types (0, 1, 2, 4, 0x7E) and the receipts are OP's. `verify` takes its fork
  times (Regolith, Canyon, Ecotone, Isthmus) from the chain's fork list. Checked on
  2026-10-04: blocks 1,000,000, 13,000,000, 46,700,000 (Azul), 47,900,000 (Beryl) and
  52,100,000 (Cobalt) rebuilt to their hashes, from Base's public endpoints in the rows'
  form (as holes, section 3.1a).
- **The top anchor** is the newest game of Base's factory
  (`0x43edB88C4B80fDD2AdFF2412A7BebF9dF42cB40e` on Ethereum): `AggregateVerifier` games (type
  621), created with `createWithInitData`, whose extra data starts with the L2 block number and
  whose root claim is that block's output root, read through the chain spec's claim formats
  like the other types.
- **Header fields.** HyperSync's Base rows lack `mix_hash` and the base fee in large stretches
  before block 13.5 M (section 3.1a). By default `download` rebuilds them from L1 (one
  HyperSync request per 10,000 L1 blocks, and `verify`'s own work to check them); with
  `--headers-from rpc` it fetches them, which at the defaults (`--rpc-batch 10 --rpc-requests
  4`) gives tens to a hundred-odd blocks a second from a public endpoint, so hours to days for
  millions. The endpoint (for what L1 cannot give, and the fallback) defaults to
  `https://mainnet.base.org`. Two public ones work: `https://mainnet.base.org` (Base's own; it has no `eth_getBlockReceipts`, so a hole's
  receipts are read one by one, and it limits the rate with an error in the answer, which is
  waited out like HTTP 429) and `https://base.drpc.org` (has it).
- **Size.** The archive was estimated at 2 to 3.5 TB (`docs/base.md` section 6). The download
  on the user's server is 2.57 TB of downloaded chunks, on a disk with 0.99 TB free (reported
  2026-10-05). It fits: `verify` writes no copy, and deletes each downloaded chunk once the
  sealed chunks covering it are uploaded, so the 2.57 TB shrinks as it goes. The extra disk is
  the hash index's input, 16 bytes per block (under 1 GB for 52 million blocks), and `verify`
  refuses to start with less than 16 GiB free. The state directory written by the earlier
  build keeps working (section 7). Downloaded chunks are about ten times larger per block than
  OP Mainnet's, so `download`'s scan of missing fields runs fewer at once (its 256 MiB
  in-flight bound), and so does `verify` (the same bound).

## 11. Where the import stops, and the top anchor

The archive service's newest blocks are unsafe: not yet committed to L1. And parent hashes
only prove that a range is one chain, not that it is the canonical one. The legacy range has a
trusted hash at its top (section 3.2); a range that reaches the present needs one too.

- **End of a range that reaches the present:** the L2 block of the newest dispute game the
  chain's `DisputeGameFactory` created on L1 (the factory's `DisputeGameCreated` logs of the
  last day, every game type; the claim is read from the calldata of the `create` call). The
  game is recorded with the plan in the state directory, so `verify` needs no L1 lookup
  and a resumed download keeps the same end. The lookup uses the service's L1 endpoint, so it
  counts against the token's window like any download.
- **The kind of game is chosen by its type**, from the chain's table in
  `op-indexer-chainspec` (read through `ChainSpec::game_claim`): an aggregate game (type 621,
  Base) names an L2 block number and its root claim is that block's output root; a fault
  dispute game (types 0, 1, 2, 3, 8) names an L2 block number and
  its root claim is that block's output root; a super fault dispute game (types 4, 5, 7, 9;
  OP Mainnet creates type 9 as of 2026-10) carries the preimage of a super root, a timestamp
  and one output root per chain, and its root claim is the hash of that preimage. The claim
  for this chain is the output root next to its chain id, about its block at that timestamp
  (found from the Bedrock block, its time and the block time in the chain specification), and
  `verify` also requires the block to have that timestamp. A newest game of a type that is
  not in the table is refused by name, with the types the factory created.
- **The check is mandatory.** Once the range is sealed, `verify` reads the last block's header
  back from the store, computes its output root,
  `keccak256(bytes32(0) ‖ state root ‖ withdrawals root ‖ block hash)`, and requires it to
  equal the game's root claim. A mismatch fails `verify` with both values, and nothing is
  listed in the manifest. The block's hash is then the top anchor, in addition to the link to
  the block below the range.
- **What it proves:** the whole range is the chain a game on L1 claims; the proposer proposes
  blocks that are already safe, so the range is committed to L1. The import ends up to about
  an hour behind the safe head (OP Mainnet creates a game about hourly).
- **What it does not prove:** that the claim is right. A new game is a bonded claim nobody
  has challenged yet.
- **It is a consistency check, not an independent proof.** The game is read through the same
  provider as the blocks (HyperSync's L1 endpoint). A provider that served a wrong chain could
  serve a matching wrong game. Against that, give `--last-block` with an `--anchor-hash` taken
  from a source you trust.
- **Before Isthmus** the header does not carry the message passer's storage root, so the
  output root cannot be computed from what is downloaded and a game cannot anchor such a
  block. A range that ends there needs the trusted hash of its last block instead.
- A game not created by a plain call of the factory (`create`, or for type 621
  `createWithInitData`) with the extra data of its type (created through another contract, or
  another kind of game) is refused with a message rather than misread.
- From the chain specification: the factory (OP Mainnet:
  `0xe5965Ab5962eDc7477C8520243A95517CD252fA9`, superchain registry,
  `superchain/configs/mainnet/op.toml`, `DisputeGameFactoryProxy`), the Bedrock block and its
  time, and the block time. Which game type the chain's portal respects is stored on L1 and
  cannot be read through logs, so the newest game of any known type is used; the factory's
  recent games on OP Mainnet are all type 9 (seen 2026-10-03). The extra-data encodings are
  from `FaultDisputeGame.sol`, `SuperFaultDisputeGame.sol` and `Encoding.sol` in the Optimism
  monorepo.
- `--legacy-only` (0 to 105,235,062 on OP Mainnet) uses the trusted hash of the last legacy
  block from the chain specification and needs no lookup.
