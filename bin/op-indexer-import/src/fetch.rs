//! The `fetch` step: sealed chunks of a range, downloaded straight from R2 (`docs/serving.md`,
//! raw chunk download).
//!
//! ```text
//! balancer GetFlightInfo(raw:from:to) ─▶ per chunk: presigned URL + manifest entry
//!     ─▶ GET (several at once) ─▶ checked ─▶ <out>/<first>-<last>.rlp (in block order)
//! ```
//!
//! What is checked, for every chunk: what `decode_chunk` checks (its size and footer, its
//! index against the root, every segment against the index, every header against its hash,
//! the parent links from the entry's first parent through its last hash); for every block,
//! its transactions and receipts roots against its header; between chunks, each first parent
//! against the last hash before it (for a whole plan, before anything is downloaded). The entries are the balancer's: it is trusted for them,
//! as a server is for what it streams.
//!
//! A plan's URLs expire (after about 10 minutes, the balancer's setting):
//! a GET refused once they have (403) asks the balancer for a new plan from that chunk on.
//! A file already in the directory is kept, so a stopped run goes on where it was.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use alloy_consensus::Header;
use alloy_primitives::{B256, BlockNumber};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, FlightEndpoint};
use bytes::Bytes;
use eyre::{WrapErr, bail, ensure, eyre};
use futures_util::{StreamExt as _, stream};
use op_alloy_consensus::OpReceiptEnvelope;
use op_indexer_chainspec::{ChainSpec, OP_MAINNET};
use op_indexer_chunks::{ChunkEntry, decode_chunk};
use op_indexer_primitives::{ArchivedBlock, receipts_root, split_body, transactions_root};
use tracing::{info, warn};

use crate::backoff::Backoff;
use crate::cli::FetchArgs;

/// How long one chunk's GET may take: about 40 MB, at a slow 1 MB/s.
const GET_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What became of one planned chunk.
#[derive(Debug, Clone, Copy)]
enum Fetched {
    /// Checked and written: the blocks written.
    Written(u64),
    /// Its file was already there.
    Kept,
    /// Its URL was refused (403): expired. The plan is asked for again.
    Expired,
}

/// Runs `fetch`.
///
/// # Errors
///
/// Returns an error if the balancer cannot plan the range, a chunk cannot be downloaded after
/// the retries, a chunk or block fails a check, or a file cannot be written.
pub(crate) async fn run(args: &FetchArgs) -> eyre::Result<()> {
    ensure!(args.from <= args.to, "--to is below --from");
    let chain_id = args.chain.unwrap_or(OP_MAINNET.chain_id);
    let chain =
        ChainSpec::by_chain_id(chain_id).ok_or_else(|| eyre!("unsupported chain id {chain_id}"))?;
    let canyon_time = chain.canyon_time();
    tokio::fs::create_dir_all(&args.out)
        .await
        .wrap_err_with(|| format!("failed to create {}", args.out.display()))?;
    let mut balancer = connect(args).await?;
    let http = reqwest::Client::builder()
        .timeout(GET_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()?;
    let downloads = usize::try_from(args.downloads).unwrap_or(1);

    // The next block to fetch, and the chunk taken last.
    let (mut next, mut taken): (BlockNumber, Option<ChunkEntry>) = (args.from, None);
    let (mut chunks, mut blocks) = (0_u64, 0_u64);
    while next <= args.to {
        let planned_from = next;
        let Some(plan) = plan(&mut balancer, args, next).await? else {
            // Past the sealed chunks: the rest is a server's to stream.
            let last = taken.map(|entry| entry.last);
            ensure!(last.is_some(), "no sealed chunk holds block {next}");
            warn!(
                ?last,
                to = args.to,
                "the sealed chunks end here; read the rest from a server"
            );
            break;
        };
        // The links first, so nothing is written for a plan that does not chain.
        let mut previous = taken;
        for (entry, _) in &plan {
            link(previous.as_ref(), entry)?;
            previous = Some(*entry);
        }
        // Downloaded, checked and written several at once; taken in block order.
        let fetched = stream::iter(plan)
            .map(|(entry, url)| {
                let http = &http;
                async move { (entry, fetch(http, args, entry, &url, canyon_time).await) }
            })
            .buffered(downloads);
        tokio::pin!(fetched);
        while let Some((entry, outcome)) = fetched.next().await {
            match outcome? {
                Fetched::Expired => {
                    // A fresh plan refused at once is not asked for again and again.
                    ensure!(
                        next != planned_from,
                        "a fresh URL of chunk {} was refused",
                        entry.key()
                    );
                    warn!(from = next, "chunk URLs expired; asking the balancer again");
                    break;
                }
                Fetched::Kept => {}
                Fetched::Written(written) => {
                    info!(
                        first = entry.first,
                        last = entry.last,
                        written,
                        "fetched a chunk"
                    );
                    (chunks, blocks) = (chunks.saturating_add(1), blocks.saturating_add(written));
                }
            }
            (taken, next) = (Some(entry), entry.last.saturating_add(1));
        }
    }
    info!(chunks, blocks, out = %args.out.display(), "fetched");
    Ok(())
}

/// Downloads, checks and writes one planned chunk, unless its file is already there.
async fn fetch(
    http: &reqwest::Client,
    args: &FetchArgs,
    entry: ChunkEntry,
    url: &str,
    canyon_time: u64,
) -> eyre::Result<Fetched> {
    let range = clip(args, &entry);
    let path = file(&args.out, &range);
    if tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return Ok(Fetched::Kept);
    }
    let Some(bytes) = get(http, url, &entry).await? else {
        return Ok(Fetched::Expired);
    };
    write(path, entry, bytes, range, canyon_time)
        .await
        .map(Fetched::Written)
}

/// A channel to the balancer.
async fn connect(args: &FetchArgs) -> eyre::Result<FlightServiceClient<tonic::transport::Channel>> {
    let channel = tonic::transport::Endpoint::from_shared(args.balancer.clone())
        .wrap_err("--balancer is not a URL")?
        .connect()
        .await
        .wrap_err_with(|| format!("failed to connect to the balancer at {}", args.balancer))?;
    Ok(FlightServiceClient::new(channel))
}

/// The balancer's raw plan from `from` to the range's end: each chunk's entry and URL, in
/// block order. `None` if no sealed chunk holds `from`.
async fn plan(
    balancer: &mut FlightServiceClient<tonic::transport::Channel>,
    args: &FetchArgs,
    from: BlockNumber,
) -> eyre::Result<Option<Vec<(ChunkEntry, String)>>> {
    let mut request =
        tonic::Request::new(FlightDescriptor::new_cmd(format!("raw:{from}:{}", args.to)));
    if let Some(key) = &args.api_key {
        let value = format!("Bearer {}", key.expose())
            .parse()
            .map_err(|_invalid| eyre!("the API key is not a valid header value"))?;
        request.metadata_mut().insert("authorization", value);
    }
    let info = match balancer.get_flight_info(request).await {
        Ok(info) => info.into_inner(),
        Err(status) if status.code() == tonic::Code::OutOfRange => return Ok(None),
        Err(status) => bail!("the balancer refused the plan: {}", status.message()),
    };
    let plan = info
        .endpoint
        .iter()
        .map(endpoint)
        .collect::<eyre::Result<Vec<_>>>()?;
    ensure!(
        !plan.is_empty(),
        "the balancer planned no chunk from block {from}"
    );
    Ok(Some(plan))
}

/// One endpoint of a raw plan: the chunk's entry (its app metadata) and its URL.
fn endpoint(endpoint: &FlightEndpoint) -> eyre::Result<(ChunkEntry, String)> {
    let entry: ChunkEntry = serde_json::from_slice(&endpoint.app_metadata)
        .wrap_err("a planned chunk has no manifest entry")?;
    let url = endpoint
        .location
        .first()
        .map(|location| location.uri.clone())
        .ok_or_else(|| eyre!("planned chunk {} has no URL", entry.key()))?;
    Ok((entry, url))
}

/// Checks that `entry` follows `previous`, the chunk before it.
fn link(previous: Option<&ChunkEntry>, entry: &ChunkEntry) -> eyre::Result<()> {
    if let Some(previous) = previous {
        ensure!(
            entry.first == previous.last.saturating_add(1)
                && entry.first_parent == previous.last_hash,
            "chunk {} does not follow chunk {}",
            entry.key(),
            previous.key()
        );
    }
    Ok(())
}

/// The blocks of `entry` in the range asked for.
fn clip(args: &FetchArgs, entry: &ChunkEntry) -> RangeInclusive<BlockNumber> {
    args.from.max(entry.first)..=args.to.min(entry.last)
}

/// The file of the blocks in `range`.
fn file(out: &Path, range: &RangeInclusive<BlockNumber>) -> PathBuf {
    out.join(format!("{:012}-{:012}.rlp", range.start(), range.end()))
}

/// Downloads `entry`'s object, retrying what may pass. `None` if the URL is refused (403):
/// it has expired.
async fn get(http: &reqwest::Client, url: &str, entry: &ChunkEntry) -> eyre::Result<Option<Bytes>> {
    let mut backoff = Backoff::new();
    loop {
        let failure = match http.get(url).send().await {
            Ok(response) if response.status() == reqwest::StatusCode::FORBIDDEN => return Ok(None),
            Ok(response) if response.status().is_success() => match response.bytes().await {
                Ok(bytes) => return Ok(Some(bytes)),
                Err(err) => eyre::Report::new(err),
            },
            Ok(response) => eyre!("status {}", response.status()),
            Err(err) => eyre::Report::new(err),
        };
        let Some(wait) = backoff.next() else {
            return Err(failure.wrap_err(format!("failed to download chunk {}", entry.key())));
        };
        warn!(chunk = entry.key(), attempt = backoff.attempt(), err = %failure, "chunk download failed; trying again");
        tokio::time::sleep(wait).await;
    }
}

/// Checks `bytes` as `entry`'s chunk and writes its blocks in `range` to `path` (through a
/// temporary file, so the file exists only whole). Returns the blocks written.
async fn write(
    path: PathBuf,
    entry: ChunkEntry,
    bytes: Bytes,
    range: RangeInclusive<BlockNumber>,
    canyon_time: u64,
) -> eyre::Result<u64> {
    tokio::task::spawn_blocking(move || {
        let blocks = decode_chunk(&entry, &bytes)?;
        // The download is not needed past the check: the blocks hold their own bytes.
        drop(bytes);
        let partial = path.with_extension("rlp.partial");
        let mut out = BufWriter::new(
            File::create(&partial)
                .wrap_err_with(|| format!("failed to create {}", partial.display()))?,
        );
        let mut written = 0_u64;
        for (number, block) in (entry.first..).zip(&blocks) {
            check_roots(block, canyon_time)
                .wrap_err_with(|| format!("block {number} of chunk {}", entry.key()))?;
            if range.contains(&number) {
                encode(block, &mut out)?;
                written = written.saturating_add(1);
            }
        }
        out.into_inner()
            .map_err(std::io::IntoInnerError::into_error)
            .and_then(|file| file.sync_all())
            .and_then(|()| std::fs::rename(&partial, &path))
            .wrap_err_with(|| format!("failed to write {}", path.display()))?;
        Ok(written)
    })
    .await?
}

/// Checks the block's transactions root and receipts root against its header.
fn check_roots(block: &ArchivedBlock, canyon_time: u64) -> eyre::Result<()> {
    let encoded = &block.encoded;
    let header: Header =
        alloy_rlp::decode_exact(&encoded.header).wrap_err("its header does not decode")?;
    let body = split_body(&encoded.body).ok_or_else(|| eyre!("its body is not a block body"))?;
    ensure!(
        transactions_root(&body.transactions) == header.transactions_root,
        "its transactions are not its header's"
    );
    let receipts = encoded
        .receipts
        .as_ref()
        .ok_or_else(|| eyre!("it has no receipts"))?;
    let receipts: Vec<OpReceiptEnvelope> =
        alloy_rlp::decode_exact(receipts).wrap_err("its receipts do not decode")?;
    let root: B256 = receipts_root(&receipts, header.timestamp, canyon_time);
    ensure!(
        root == header.receipts_root,
        "its receipts are not its header's"
    );
    Ok(())
}

/// Writes the block as an RLP list of its header, body and receipts, each as it is encoded.
fn encode(block: &ArchivedBlock, out: &mut impl Write) -> std::io::Result<()> {
    let encoded = &block.encoded;
    let receipts: &[u8] = encoded.receipts.as_ref().map_or(&[], AsRef::as_ref);
    let payload_length = encoded
        .header
        .len()
        .saturating_add(encoded.body.len())
        .saturating_add(receipts.len());
    let mut header = Vec::with_capacity(9);
    alloy_rlp::Header {
        list: true,
        payload_length,
    }
    .encode(&mut header);
    out.write_all(&header)?;
    out.write_all(&encoded.header)?;
    out.write_all(&encoded.body)?;
    out.write_all(receipts)
}
