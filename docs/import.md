# Import (`bin/op-indexer-import`)

How a chain's history gets into R2: `download` from Envio HyperSync, `verify` (check, seal,
upload), and `fetch` back. Status 2026-10-05: Unichain (0 to 60,422,316, 1,694 chunks) is
in R2; OP Mainnet (0 to 157,745,023) was downloaded and verified by an earlier build; Base is
downloaded (521,890 chunks, 2.57 TB) and its incomplete answers are being fetched again
(section 3.1), not verified yet.

The binary downloads a chain's blocks from HyperSync, verifies every block against a trusted
anchor (section 11), and seals the verified bytes into the chunks of
[serving.md](serving.md) §1–2, uploaded to Cloudflare R2, which servers read history from. It
is a separate process: the node never links it and never calls an external service (why:
[decisions.md](decisions.md), 2026-10-04). The HTTP client and the compression crates belong
to this package only.

## 1. Sources and trust

- **Envio HyperSync** gives every block's header, transactions, receipts and logs as JSON
  rows; the transactions and receipts roots, blooms and transaction hashes are not asked for,
  since `verify` computes them and the header hash proves them.
- **What HyperSync leaves out** of some rows comes from L1 (by default) or the chain's
  JSON-RPC, read-only (section 3.1a).
- **Neither is trusted**: every block is rebuilt and proven by its header hash, the parent
  links up to the anchor, and every sender (section 3.2).

## 2. Rate limits and incomplete answers

- **The service limits the request rate.** When it refuses (HTTP 429) three times for one
  answer, `download` starts no new chunk and ends, keeping what it has; run it again later.
  Nothing is downloaded twice, and as few bytes as possible travel (section 3.1).
- **The service's servers do not all answer alike.** On Base the same query asked five times
  came back complete four times; the fifth lacked every block's `mix_hash` and every
  deposit's `source_hash`. A sample across the chain also found answers without deposits'
  `mint` and `deposit_nonce`. So every answer is checked before it is kept and asked again
  when it lacks a field (section 3.1).

## 3. Phases

Each phase is a subcommand, works in a state directory (section 6), and can be stopped and
run again; a completed chunk is never redone.

### 3.1 `download`

- On its first run it decides the range and records it in `plan.json` (section 6).
- The range is cut into chunks: 1,000 blocks before the chain's Bedrock block, 100 from it on,
  where one block holds as much as many legacy ones (`--chunk-blocks`, and a tenth of it).
  Chunks are fetched with `--requests` requests in flight (default 64), each on its own
  HTTP/1.1 connection.
- A response may cover less than the range asked for; the chunk is complete only when every
  block of it has arrived.
- **Done chunks.** A chunk is done when its file is in `raw/`, or when the records of sealed
  chunks (`sealed/`, section 3.2) cover it whole, from the first record without a gap:
  `verify` deleted its file once it was sealed and uploaded. Only the other chunks are fetched,
  so a deleted or missing file is fetched again unless sealed records cover it; the fill's
  scan reads only the chunks with a file that no record covers.
- **Every answer is checked before it is kept.** Once a chunk's answer is on disk under a
  temporary name (`*.answer.tmp`), its rows are read and checked for every field their forks
  have, as the fill's scan does (`fill::lacking`; on as many threads as there are cores, about
  650 MB of JSON a second each). An answer lacking a field is asked for again, up to 8 answers,
  stopping after 3 in a row no more complete than the best; the most complete is kept, and the
  fill takes what it still lacks. The summary counts `retried_chunks` and `incomplete_chunks`.
- **`--refetch-incomplete`** first reads every chunk on disk not sealed yet, with its fill,
  and asks again for those whose rows still lack a field or cannot be read (a damaged file).
  A new answer replaces the chunk only if it lacks fewer fields, every row counted; the
  chunk's fill, which belonged to the old answer, is dropped with it (`replaced_chunks`,
  `unreplaced_chunks`). `--refetch-requests` (default 16) replaces `--requests` for that run.
  - **The scan reads each answer's head** (`--scan head`, the default): its first block row
    and that block's transactions. An answer comes from one of the service's servers, and one
    that leaves a field out leaves it out of every row (seen live: 100 of 100 blocks without
    `mix_hash`, 111 of 111 deposits without `source_hash`). Logs come first in an answer and
    blocks last, so the whole text is still decompressed, but nothing past the head is parsed:
    549 chunks/s against 408 for `--scan full` (200 OP chunks, 10 cores). An answer whose head
    is not found is read whole. A chunk the head scan misses is still checked whole by the
    fill's scan and by `verify`.
  - **The list is kept** in `refetch.json` once the scan is done, and shortened at each
    progress line as chunks are done: a later run asks for those, in block order, without
    scanning again (`--rescan` scans again; a list made for another plan is not used). A run
    ended by the rate limit reports what is left (`refetch_left`).
- **Stored as it travels.** The response body is written to the chunk's file exactly as
  received, in the content encoding the service chose (zstd is asked for first, then gzip),
  after one byte naming that encoding. Nothing is decompressed to be compressed again. The
  work per byte is one streaming decode, into nothing, to read the cursor (`next_block`) at
  the end of the body, and the check of the chunk's rows. Memory is about a megabyte per
  request in flight, whatever a chunk holds.
- **Fields requested**: what the header, the transactions and the receipts with their logs
  are rebuilt from. The old client's L1 fee fields are not requested: no root covers them and
  nothing reads them.
- **Errors**: HTTP 429 gets 3 attempts in all, about 15 s then 30 s apart; then no new chunk
  starts and the run ends (above). Other errors that may pass (408, 5xx, a broken connection,
  an answer cut short) get 6 attempts, from 0.5 s doubling to 20 s, jittered. 401, 403 and any
  other 4xx fail at once. When a chunk fails for good, or free disk space falls below 16 GiB,
  no new chunk is started, the requests in flight finish and are written, and the phase ends
  with a summary of what is missing. It never spins.
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

### 3.1a What the service leaves out, and the fill

OP Mainnet's whole chain verified with nothing left out. Elsewhere:

- **Authorization lists (Unichain).** HyperSync sends EIP-7702 transactions (type 4) without
  their `authorization_list`; every other field is there (first at block 16,068,511). The list
  is not optional (EIP-7702 refuses an empty one), so such a row cannot be rebuilt from the
  download alone. Base's rows carry it.
- **Holes: whole blocks without their rows (Unichain).** At 55,142,810 to 55,142,819 the
  answer has the ten header rows but no transaction and no log rows, while the chunk's other
  90 blocks have theirs and the answer was not cut short. Every block after the Bedrock block
  has at least the L1-attributes deposit, so a block from there on without transaction rows is
  a hole: the whole block is fetched.
- **Header fields (Base).** HyperSync's Base rows lack `mix_hash` and `base_fee_per_gas` in
  large, patchy stretches before block 13.5 M (some answers lack them anywhere, section 2).
- **Deposit source hashes (Base).** In the same stretches the deposit rows lack
  `source_hash`, which a deposit's encoding, so the transactions root, includes.
- **User deposits' `mint` (Base)**, which the deposit's hash covers (first seen at block
  1,322,905).

**Checked after every download.** Once the chunks are on disk, `download` reads every chunk
not sealed yet (one per core at a time, within 256 MiB of downloaded bytes in flight, the
bound `verify` uses too) and lists every field its rows lack that the rebuild needs, or fills
with a default the header hash must then prove:

- header fields of the block's forks (`mix_hash` and `base_fee_per_gas` from Bedrock,
  `withdrawals_root` from Canyon, the blob gas fields and the parent beacon block root from
  Ecotone);
- the fields of each transaction type, a receipt's status, and a deposit receipt's nonce and
  version from Canyon;
- a user deposit's `mint`, in an epoch's first block only (the deposits after the first there
  are users', or a fork's upgrade deposits, which mint nothing).

Each is logged once, at warn, with its count and its first block: one pass shows them all,
where `verify` would stop at the first. The pass costs what reading the rows costs `verify`
(decompress and parse; no hashing, no signature, nothing written), about 20 ms of processor
time per 1,000 legacy blocks. A type-4 row counts as lacking its list when it has no
`authorization_list`, no bytes, or a list of zero entries, and no fill
(`TransactionRow::lacks_authorization_list`, the rule `verify` uses too).

**Filled from L1** (`--fill-from l1`, the default; `bin/op-indexer-import/src/fill/derive.rs`).
Each missing field follows from what the download and L1 have:

- **`mix_hash`** is the L1 origin's `mix_hash` (`prevrandao`), and from Ecotone on the
  **parent beacon block root** is the L1 origin's own `parent_beacon_block_root`. The L1 origin
  is named by the block's L1-attributes deposit (its first transaction): its number at bytes
  28..36 of the calldata and its hash at 100..132, the same bytes in the Bedrock form (ABI
  words) and in the packed forms of Ecotone, Isthmus and later. The L1 header read for that
  number must have that hash. L1 headers come from L1's HyperSync (`--l1-endpoint`, the API
  token's), a span of at least 10,000 L1 blocks per request (about 33 hours, some 60,000 Base
  blocks), paged and retried like the rest.
- **The base fee** follows from the parent's by EIP-1559: the gas target is the gas limit over
  the elasticity (6), the change at most one denominator-th per block (50, and 250 from Canyon:
  `ChainSpec::eip1559`), and from Holocene on with the elasticity and denominator in the
  parent's `extraData`. Computed in block order across chunks, from the block before the run's
  first (the chunk read before it, or, when that chunk is sealed and gone, that one header
  from the RPC). Not from Jovian on (its minimum base fee and data footprint are not rebuilt).
  A stretch without base fees is rebuilt block after block, so a block that cannot be rebuilt
  (or one wrong, caught by its chunk's check) leaves the rest of its chunk to the RPC.
- **The withdrawals root** from Canyon is the empty trie's root until Isthmus; from Isthmus it
  is the message passer's storage root, which cannot be rebuilt (RPC). **The blob gas used**
  and **the excess blob gas** from Ecotone are zero (the blob gas used until Jovian, which makes
  it the data availability footprint: RPC).
- **Deposit source hashes**: an L1-attributes deposit's from its L1 origin's hash and sequence
  number; in an epoch's first block, the user deposits' from the origin's hash and the index
  of their `TransactionDeposited` log (the chain's `OptimismPortal`) in that block, in log
  order; a fork's upgrade deposits from their intents (Ecotone's and Fjord's are known;
  another fork's fails naming the block).
- **A user deposit's `mint`** from its log's opaque data (mint, value, gas limit, creation
  flag, calldata). The row's `from`, `to`, `value`, `gas` and `input` are checked against the
  log, and a difference stops the run, naming the field.
- **Checked before written.** The chunks are read in parallel and rebuilt in block order. A
  chunk's rebuilt fields are kept in memory while the chunk is rebuilt whole, as `verify` does,
  with them on top of its fill (on every core, within the same 256 MiB bound), each block
  against its own hash; only the fields of blocks that hash are written to the fill, merged
  field by field into what it holds. A block that does not hash is fetched whole from the RPC
  (transactions, receipts and header fields), which replaces the rows the service sent of it:
  the rebuilt values may be right and a transaction, receipt or log row wrong (seen on Base,
  block 3,109,100 among others). The fetch logs whether the transactions root or the receipts
  root the rows give differs from the RPC's. Without an endpoint the run stops, naming the
  block, the roots its rows give and everything rebuilt for it. Nothing unchecked is written,
  so a stopped run leaves nothing behind that would hide a missing field from the next scan.
  A chunk that needs the RPC for anything else (a field that cannot be rebuilt, an L1 origin
  not found, a list, a hole) has all its header fields fetched with it.

**Fetched from the RPC**: what L1 cannot give (lists, holes, the fields above that cannot be
rebuilt, blocks that do not hash), or everything with `--fill-from rpc`.

- **Endpoint**: `--rpc-endpoint` (`OP_INDEXER_IMPORT_RPC_ENDPOINT`). With `--fill-from l1`
  none is used unless given; then something that needs one stops the run with a message to
  give one. With `--fill-from rpc` the default is the chain's public endpoint
  (`https://mainnet.unichain.org`, `https://mainnet.base.org`; none for OP Mainnet). Only its
  scheme and host are shown, in logs and errors alike (a provider's key is in the path or the
  query: the rest shows as `/…`), and the HTTP client's errors are kept without the URL.
- **Calls**, made as the scan goes, while the next chunks are read: `eth_getBlockByNumber`
  without transactions for a header, with full transactions for a list or a source hash
  (Unichain's public endpoint does not allow `eth_getTransactionByBlockNumberAndIndex`; the
  header comes with it, so such a block costs one call), and for a hole its receipts too, with
  `eth_getBlockReceipts` (else `eth_getTransactionReceipt` per transaction). `--rpc-batch`
  calls per request as one JSON-RPC batch (default 10: Unichain's public endpoint refuses
  larger batches), `--rpc-requests` chunks' requests at a time (default 4). The defaults suit
  a public endpoint; a provider of your own may take more of both.
- **Retries**: up to six times with `download`'s capped, jittered backoff on a busy endpoint
  (408, 5xx), a broken connection or a malformed answer; on a rate limit (429, or an error in
  the answer saying so) it waits what `Retry-After` asks (up to 10 minutes), else 15 s doubling
  to 2 minutes. Ctrl-C drops the requests in flight at once. A refused call, a block the
  endpoint does not have, or a block hash that is not the downloaded one (an endpoint of
  another chain) fails at once, naming the endpoint.

**The fill file.** What is filled goes to the chunk's fill, `raw/<from>-<to>.fill.json`,
written atomically and durably, in the RPC's JSON form (a list per transaction; a hole's
transactions and receipts with their logs; a header's fork fields, without its hash: about
150 bytes a block before Canyon, 400 after Ecotone); the downloaded chunk stays as received.

- A chunk downloaded again loses its old fill first. A fill is deleted with its downloaded
  chunk once that chunk is sealed.
- **Resumable**: a chunk's fill is written once its requests are all answered, and the scan
  reads the fill with the rows, so a chunk whose fill holds what it lacks fetches nothing on
  the next run. Each run reads every chunk not sealed yet again (decompress and parse), which
  on Base's 2.57 TB takes a while; the fetching goes on meanwhile.
- **`verify` reads no RPC**: it puts the fill into the rows before rebuilding (a list into its
  transaction, a hole's transactions and logs as rows like the service's, a header field
  where the row lacks it; what the service sent is kept). A type-4 row with no list, or a
  block after Bedrock with no transactions, and no fill, fails `verify` with a message to run
  `download`, not the generic hash mismatch.
- **Trust unchanged.** What is filled goes into the rebuilt block; the transactions and
  receipts roots, so the header hash, prove it. A hole's senders are recovered and checked
  like every other. A wrong fill fails `verify` like a wrong row.
- **Counted** in `download`'s summary: `blocks_to_fetch`, `blocks_fetched`,
  `rpc_filled_transactions`, `rpc_filled_headers`, `l1_rebuilt_headers`,
  `l1_rebuilt_sources`, `l1_rebuilt_mints` and `l1_rebuilt_not_hashing`, and one warning per
  missing field with its count and first block (counted before that run's fills);
  `rpc_filled_transactions` in `verify`'s.

### 3.2 `verify`: check, seal and upload

`verify` is the one step that checks the downloaded blocks and puts them in object storage.
It reads the downloaded chunks only (never HyperSync or the chain's RPC) and writes sealed
chunks, their manifest and the global hash index, in the layout of [serving.md](serving.md)
§1–2, to Cloudflare R2 or a local directory. It is the importer's only path to object
storage, through `crates/chunks`. No copy of the range is written: a downloaded chunk is
deleted once the sealed chunks covering it are uploaded.

In order:

1. **Check**, on every core (`--threads`, default one per CPU). For every block of a downloaded
   chunk it rebuilds the transactions and the receipts from the downloaded rows (with the
   chunk's fill, section 3.1a), computes the transactions root over the transaction encodings
   and the receipts root over the receipts, rebuilds the header with those roots and the bloom
   of the logs, and requires the header to hash to the block's hash and to name the previous
   block as its parent. Then it checks every sender (below).
2. **Seal**, in block order: a `ChunkWriter` (`crates/chunks`) turns the checked blocks into
   chunks. A chunk ends at 256 MiB uncompressed, at 100,000 blocks, before the Bedrock block,
   or at the range's last block. The cuts depend on the blocks only, so the same range always
   gives the same chunks.
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
  bloom are never downloaded: `verify` computes them, and the header hash proves them.
  `mix_hash` is downloaded, but the service leaves it out of some rows. Up to and including
  the Bedrock block every header has a zero `mix_hash`, so a row without it is rebuilt with
  zero, and the Bedrock block's missing base fee is rebuilt as EIP-1559's initial one; the
  header hash decides, so a wrong guess is refused, never accepted. These are counted
  (`rebuilt_header_fields`). After Bedrock a row without `mix_hash` is refused ("the header
  lacks `mix_hash`").
- **Senders.** The sender recorded with each transaction is the `from` HyperSync reports;
  `verify` proves it before the block is sealed: for every signed transaction it recovers the
  sender from the signature and compares; for a deposit it compares with the `from` in the
  deposit's encoding, which the transactions root and so the block hash cover. A mismatch
  stops `verify` before that block is sealed, naming the block, the transaction's index and
  both addresses. One kind is not proven: a legacy transaction signed with all zeros (an
  L1-to-L2 message of OP Mainnet's client before Bedrock) has no signer, keeps the reported
  zero address, and is counted (`zero_signature_transactions`). The recovery runs on
  libsecp256k1 (alloy's `secp256k1` backend): about 32 µs per transaction on one core, so a
  whole chain is about 13 CPU-hours, spread over every core.
- **Links, then the anchor.** The links are checked as the chunks are sealed: inside a chunk
  by the writer, between chunks by each chunk's first parent against the last hash before it,
  and the first chunk against the manifest's last chunk when the range continues one the
  manifest lists. A broken link stops the run at once, naming the block. The anchor is checked
  once the whole range is sealed: a hash is compared with the last chunk's last hash; a
  dispute game's claim needs the last block's header, which is read back from the store. Only
  when it matches are the chunks listed. If it does not, nothing is listed: the chunks
  uploaded stay in the bucket, unlisted and content-addressed, and nobody reads them.
- **A downloaded chunk that fails a check** is kept, nothing from its first failing block on
  is sealed, and the run stops with the block number, the check and the file. If the data is
  wrong, delete the file (and its fill) and run `download` again; a row left without a field
  says to run `download`, which fills it (section 3.1a). With the roots not downloaded, a
  wrong transaction, receipt or log shows as the header hash not matching, for its block. A
  chunk not downloaded at all stops `verify` before anything is read, naming its blocks and
  its file.
- **Resumable.** A restart continues after the last record in `sealed/` with a fresh
  `ChunkWriter`, which cuts the same chunks an unbroken run would. The records are checked
  first: they must follow each other and the manifest's last chunk without a gap or a broken
  link. One listing of the bucket at the start finds the chunks it already holds, and those
  are not uploaded again: their names carry their root, and a PUT is create-only anyway.
  Downloaded chunks a record covers but a stopped run did not delete are deleted at the
  start. When the manifest already lists the whole range and the hash index covers it,
  `verify` says so and exits. Ctrl-C lets the uploads in flight finish and records them;
  nothing is listed until a run reaches the end.
- **The hash index.** Every block's hash and number go to the index's input as the blocks are
  sealed; a resumed run first reads them back from the chunks the manifest lists and the
  records name. At the end the index is written as a new generation of the manifest
  ([serving §2.4](serving.md#24-by-hash-lookups-a-global-index-in-r2)).
- **Bucket**: one per chain, `<chain>-snapshot` (`op-snapshot`, `unichain-snapshot`,
  `base-snapshot`), with the folder `archive/` in it (`--r2-bucket`, `--r2-prefix`). The
  manifest records the chain (id and genesis hash), and a run of another chain refuses it. Its
  exporter id is `import`.
- **Target**: R2, from the account id, the two keys and optionally the bucket, the prefix and
  `--r2-endpoint` (another S3-compatible store); the keys are flags with an environment
  fallback (prefer the environment: a flag shows in `ps`), and their values are never shown or
  logged. Or `--to-dir <dir>`: a local directory with the same layout, for a check or the
  bench without credentials.
- **Uploads**: one PUT per chunk (about 20 to 40 MB), create-only as a guard; an object above
  64 MiB goes up in parts. Failed requests (timeouts, connection errors, 5xx) are retried with
  exponential backoff, from 200 ms to 30 s, up to 10 times or 3 minutes.
- **Progress** every 10 seconds: blocks done of the range, transactions, chunks sealed (and
  those skipped as already uploaded), blocks per second, MB/s of downloaded chunks read and
  MB/s uploaded, uploads in flight, and the time left. An end line gives the totals (blocks,
  transactions, `senders_recovered`, `zero_signature_transactions`, `rebuilt_header_fields`,
  `rpc_filled_transactions`, chunks, skipped, bytes uploaded).
- **Memory**: up to `--threads` downloaded chunks are checked at once, within 256 MiB of
  downloaded bytes in flight (one chunk is always allowed); a chunk takes about twenty times
  its downloaded size while it is checked, so roughly 5 GB at most. The chunk being sealed (up
  to 256 MiB uncompressed) and the chunks uploading are held in memory too.
- **Disk**: besides the downloaded chunks still to seal, only the hash index's input,
  `<state>/index-build/`: 16 bytes per block (about 2.5 GB for OP Mainnet, under 1 GB for
  Base), emptied at the start of each run and removed once the index is written. `verify`
  refuses to start with less than 16 GiB free.

### 3.3 `fetch`: whole chunks from R2, the other way

`import fetch --balancer <url> --from <n> --to <n>` downloads the sealed chunks of a range
straight from R2, through presigned URLs a balancer hands out (`raw` plans), checks every
block (chunk root, segments, header hashes and links, transactions and receipts roots) and
writes them as RLP files. It needs no state directory and no R2 key; the balancer's URL and
an API key fall back to `OP_INDEXER_BALANCER_URL` and `OP_INDEXER_API_KEY`. See
[serving §6.8](serving.md#68-raw-chunk-download-2026-10-05).

## 4. Running it

The importer is a self-contained command-line tool, meant to be built here and run on another
machine: `cargo build --release -p op-indexer-import` produces one file to copy.

- Subcommands: `download`, `verify`, `run` (both in order) and `fetch`.
- `--api-token <TOKEN>` carries the HyperSync token; `OP_INDEXER_IMPORT_API_TOKEN` in the
  environment is the fallback (`ENVIO_API_TOKEN`, its former name, still works, with a
  warning). A flag is visible in the process list and the shell history, the variable is
  not. The token is never logged and never written to the state directory.
- Settings that are secret or name a place are flags with an environment fallback: the state
  directory (`OP_INDEXER_IMPORT_STATE_DIR`), the chain (`OP_INDEXER_CHAIN_ID`), the
  endpoints (`OP_INDEXER_IMPORT_ENDPOINT`, `OP_INDEXER_IMPORT_L1_ENDPOINT`,
  `OP_INDEXER_IMPORT_RPC_ENDPOINT`, whose URL often holds a key), and for `verify` the bucket
  and its keys (`OP_INDEXER_R2_ACCOUNT_ID`, `OP_INDEXER_R2_BUCKET`, `OP_INDEXER_R2_PREFIX`,
  `OP_INDEXER_R2_ACCESS_KEY_ID`, `OP_INDEXER_R2_SECRET_ACCESS_KEY`, `OP_INDEXER_R2_ENDPOINT`;
  section 3.2).
- Every other setting is a flag only, with a default: the range (`--first-block`,
  `--last-block`, `--anchor-hash`, `--legacy-only`), the chunk size (`--chunk-blocks`),
  requests in flight (`--requests`), the RPC's batch size and requests in flight
  (`--rpc-batch`, `--rpc-requests`; section 3.1a), where missing header fields come from
  (`--fill-from`, `l1` or `rpc`), the refetch (`--refetch-incomplete`, `--refetch-requests`,
  `--rescan`, `--scan`), and for `verify`: `--to-dir`, its threads (`--threads`, one per CPU)
  and its uploads (`--uploads`, 4, from 1 to 64). Their former variables
  (`OP_INDEXER_IMPORT_FIRST_BLOCK` and the like) are still read for this release, with a
  warning naming the flag ([configuration.md](configuration.md), "Deprecated variables").
- `--state-dir` and `--env-file` (`OP_INDEXER_ENV_FILE`; `.env` if it exists) are shared by
  every step: the range is decided by the first `download` and read from `plan.json`
  afterwards. `import --help` and `<command> --help` list every flag, its environment
  variable and its default.
- What the bucket holds is asked of the bucket: `verify` continues after the manifest's last
  chunk and the records in `sealed/`, and refuses a manifest of another chain.
- `verify` exits with an error if it stops before the end of the range, and says what to run.

### How to run it

Build on any machine with the Rust toolchain and copy the one file:

```bash
cargo build --release -p op-indexer-import
```

The file is `target/release/import` (the package is `op-indexer-import`). On the machine that
runs it, with the defaults (OP Mainnet, from block 0 to the last block committed to L1, state
in `./import-state`):

```bash
import download --api-token <TOKEN> --requests 64
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import verify
```

- **The range**: with no range flags, block 0 to the L2 block of the newest dispute game of
  the chain on L1 (section 11). `--legacy-only` ends at the last block before Bedrock with its
  known hash and needs no lookup; `--last-block <n> --anchor-hash <hash>` gives any end whose
  hash you trust; `--first-block` any start. These are read on the first `download` only; on a
  later run a range flag that disagrees with the recorded plan is refused. To import another
  range, or to extend to a newer game, use an empty state directory.
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
- **Rate limited**: `download` stops with a summary of what is missing (section 2); run the
  same command again later: only missing chunks are fetched.
- **A chunk that fails `verify`**: the error names the block, the check and the file. Delete
  that file from `raw/` (and its fill) and run `download` again.
- **Disk**: a downloaded legacy chunk is 0.7 to 1.0 KB per block; the whole OP Mainnet chain
  was 589 GB downloaded, Base 2.57 TB. `verify` writes no copy and deletes each downloaded
  chunk once the sealed chunks covering it are uploaded and recorded, so the downloaded bytes
  shrink as it goes.

## 5. Measured

- **Download**, a 32-core server: OP Mainnet's 157,745,024 blocks (589 GB) in about 25
  minutes at 300 to 380 MB/s; the legacy range at about 185,000 blocks per second with 64
  requests in flight.
- **Check**, one thread: about 0.17 s of processor time per 1,000 legacy blocks with sender
  recovery, about 0.06 s without.
- **`verify`**, release build, 2,000 post-Isthmus OP Mainnet blocks (26.8 MB downloaded,
  `--to-dir`, 10 cores): 50,368 transactions, 48,357 senders recovered, sealed into one chunk
  of 20.5 MB in 0.64 s; the hash index in 1.7 s; 3.0 s wall time; peak RSS 680 MB.
- **Fill from the RPC**: about 35 headers a second from a public endpoint at the defaults
  (10 calls a request, 2 requests at a time), so hours to days for Base's millions; the L1
  rebuild needs one HyperSync request per 10,000 L1 blocks.

## 6. The state directory

```text
<state>/plan.json                   the chain, the range, its anchor and the chunk size
<state>/raw/<from>-<to>.raw         downloaded chunk: the service's answers as they travelled
<state>/raw/<from>-<to>.fill.json   fields the service left out, from L1 or the RPC (3.1a)
<state>/refetch.json                chunks `--refetch-incomplete` still has to ask for again
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
  `plan.json`, or a `plan.json` with another version. Delete it and download again. Old
  `verified/`, `verified.json` and `export-index/` are not read and can be deleted.
- A file exists only when it is complete: it is written under a `.tmp` name (an answer being
  checked under `*.answer.tmp`), synced and renamed; leftover `.tmp` files are removed at the
  next start.

## 7. Not built

The senders and L1 metadata of L1-to-L2 messages; any source other than HyperSync; the binary
(Arrow) format of HyperSync, which measured offline would save about 20% on the wire over the
compressed JSON.

## 8. From the Bedrock block onward

Every block from Bedrock on starts with a deposit, and bridged deposits follow. `verify`
rebuilds legacy, EIP-2930, EIP-1559, EIP-7702 and deposit transactions, their receipts, and
headers of every fork (base fee, withdrawals root, blob fields, beacon root, and from Isthmus
the hash of an empty requests list, which has no column in HyperSync).

- **Deposits are rebuilt from HyperSync's `source_hash` and `mint` columns**, or what
  `download` filled where they lack them (section 3.1a). A deposit without a source hash
  fails `verify` with a named check; one without a mint is read as minting nothing, which the
  header hash proves. HyperSync's OP Mainnet rows carry both for every deposit.
- The system-transaction flag, which has no column, follows the protocol's rule: only the
  L1-attributes deposit before Regolith has it.
- A deposit receipt carries the sender's nonce and the receipt version from Canyon on, when
  they became part of the hashed receipt; before Canyon nothing the root does not cover is
  stored.
- Fork times and the Bedrock block come from `op-indexer-chainspec`.
- `access_list` and `authorization_list` arrive as the bytes of the service's binary column
  (a hex string in the JSON), decoded when the transaction is rebuilt (`verify/lists.rs`).
- Verified on real data: the whole OP Mainnet chain, so every transaction type, user deposits
  and every fork up to the newest dispute game. A block that cannot be rebuilt and verified is
  not imported.

## 9. Any chain

The chain is chosen with `--chain <id>` on the first `download` and recorded in `plan.json`;
a later run with another `--chain` is refused, and one without it continues the recorded
chain. Its parameters (fork times, the Bedrock block and its time, the hash of the last
legacy block if it has a legacy chain, the block time, the dispute-game factory) come from
`op-indexer-chainspec`, which knows OP Mainnet (10), Unichain (130) and Base (8453). The
chain is also `OP_INDEXER_CHAIN_ID`, the variable the node reads, so one `.env` sets it for
both. The HyperSync endpoint is the importer's concern, not the chain specification's: a table
in the importer gives `https://optimism.hypersync.xyz` for 10, `https://unichain.hypersync.xyz`
for 130 and `https://base.hypersync.xyz` for 8453, `--endpoint` overrides it, and a chain
without an entry needs `--endpoint`. The L1 endpoint (`--l1-endpoint`, default Ethereum's) is
where the dispute games are looked up and L1 headers and deposit logs read.

A later range, from where the bucket's manifest ends to a newer game, goes in an empty state
directory with `--first-block` set to the block after the manifest's last; `verify` checks
that its first block names the manifest's last hash as its parent before anything is listed.

## 10. Unichain and Base

### Unichain (chain 130)

```bash
import --state-dir unichain-state download --chain 130 --api-token <TOKEN> --rpc-endpoint https://mainnet.unichain.org
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import --state-dir unichain-state verify
```

- **No legacy chain.** Unichain began with Bedrock at its genesis (block 0, time
  1730748359): every chunk is a post-Bedrock chunk, a tenth of `--chunk-blocks` (100 blocks
  by default), and `--legacy-only` is refused. Block 0's parent hash is zero and its header
  already has the fields of every fork through Granite.
- **One-second blocks**, so about 60 million blocks and about 600,000 downloaded chunks at the
  default size. Unichain blocks are small, so `--chunk-blocks 10000` (1,000 blocks per chunk)
  means fewer requests and files; it is recorded in the plan and cannot change later.
- **Authorization lists and holes** need the RPC (section 3.1a), hence `--rpc-endpoint`.
- **The top anchor** is the newest dispute game of Unichain's own factory
  (`0x2F12d621a16e2d3285929C9996f478508951dFe4` on Ethereum): super games (type 9), whose
  claim is read for chain 130, with the timestamp turned into a block at one block a second
  from genesis.

### Base (chain 8453)

```bash
import --state-dir base-state download --chain 8453 --api-token <TOKEN> --rpc-endpoint https://base.drpc.org
```

```bash
OP_INDEXER_R2_ACCOUNT_ID=<account id> OP_INDEXER_R2_ACCESS_KEY_ID=<key id> OP_INDEXER_R2_SECRET_ACCESS_KEY=<secret> import --state-dir base-state verify
```

- **No legacy chain**, as Unichain: about 52 million blocks (2 s blocks), 521,890 downloaded
  chunks for blocks 0 to 52,166,660.
- **Incomplete answers** (section 2): run `download --refetch-incomplete` until it reports
  nothing left, then `verify`.
- **Header fields, source hashes and mints** are rebuilt from L1 by default (section 3.1a).
  What L1 cannot give, and blocks that do not hash, need `--rpc-endpoint`. Two public ones
  work: `https://base.drpc.org` (has `eth_getBlockReceipts`) and `https://mainnet.base.org`
  (Base's own; without it, so a hole's receipts are read one by one, and it rate-limits with
  an error in the answer, which is waited out like HTTP 429).
- **Base's own forks** (Azul, Beryl, Cobalt) change nothing the import rebuilds: the header,
  the transaction types (0, 1, 2, 4, 0x7E) and the receipts are OP's
  ([base.md](base.md)).
- **The top anchor** is the newest game of Base's factory, an `AggregateVerifier` game (type
  621; claim format in [base.md](base.md) §4).
- Downloaded chunks are about ten times larger per block than OP Mainnet's, so the scan and
  `verify` run fewer at once (their 256 MiB in-flight bound).

## 11. Where the import stops, and the top anchor

The archive service's newest blocks are unsafe: not yet committed to L1. And parent hashes
only prove that a range is one chain, not that it is the canonical one. The legacy range has a
trusted hash at its top (the chain spec's last legacy hash, below); a range that reaches the
present needs one too.

- **End of a range that reaches the present:** the L2 block of the newest dispute game the
  chain's `DisputeGameFactory` created on L1 (the factory's `DisputeGameCreated` logs of the
  last day, every game type; the claim is read from the calldata of the `create` call). The
  game is recorded with the plan in the state directory, so `verify` needs no L1 lookup
  and a resumed download keeps the same end.
- **The kind of game is chosen by its type**, from the chain's table in
  `op-indexer-chainspec` (read through `ChainSpec::game_claim`): an aggregate game (type 621,
  Base) and a fault dispute game (types 0, 1, 2, 3, 8) name an L2 block number and their root
  claim is that block's output root; a super fault dispute game (types 4, 5, 7, 9; OP Mainnet
  and Unichain create type 9) carries the preimage of a super root, a timestamp and one output
  root per chain, and its root claim is the hash of that preimage. The claim for this chain is
  the output root next to its chain id, about its block at that timestamp (found from the
  Bedrock block, its time and the block time in the chain specification), and `verify` also
  requires the block to have that timestamp. A newest game of a type that is not in the table
  is refused by name, with the types the factory created.
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
  `createWithInitData`) with the extra data of its type is refused with a message rather than
  misread.
- From the chain specification: the factory (OP Mainnet:
  `0xe5965Ab5962eDc7477C8520243A95517CD252fA9`), the Bedrock block and its time, and the block
  time. Which game type the chain's portal respects is stored on L1 and cannot be read through
  logs, so the newest game of any known type is used.
- `--legacy-only` (0 to 105,235,062 on OP Mainnet) uses the trusted hash of the last legacy
  block from the chain specification and needs no lookup.
