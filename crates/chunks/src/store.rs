//! The object store a chain's chunks live in: R2 through its S3 API, or any `object_store`
//! backend (a local directory, memory) with the same layout.
//!
//! Reads keep no data: a chunk is fetched with ranged GETs over pooled keep-alive connections,
//! checked, decoded and handed out. A stream reads ahead a bounded number of ranges (its
//! channel), and only while it lives.

use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use alloy_primitives::B256;
use bytes::Bytes;
use futures_util::{Stream, StreamExt as _, TryStreamExt as _};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions,
    PutPayload, RetryConfig, WriteMultipart,
};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::ArchivedBlock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::ChunksError;
use crate::format::{ChunkIndex, SealedChunk, decode_segment};
use crate::manifest::ChunkEntry;

/// Idle pooled connections kept per host: enough for every read in flight.
const POOL_PER_HOST: usize = 64;
/// How long an idle pooled connection is kept.
const POOL_IDLE: Duration = Duration::from_secs(90);
/// How long one request may take, and its connection setup.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Retries of a failed request (timeouts, connection errors, 5xx), with exponential backoff
/// from 200 ms to 30 s, for at most 3 minutes per request (`object_store` retries those).
const RETRIES: usize = 10;
const RETRY_FOR: Duration = Duration::from_secs(180);
/// Objects above this size are uploaded in parts of this size (multipart). A chunk is about
/// 40 MB by construction (D4), so in practice only an unusually large one is split.
const PART_BYTES: usize = 64 << 20;

/// Where a chain's chunks are, on R2. The secret never appears in `Debug` output.
#[derive(Clone)]
pub struct R2Config {
    /// The Cloudflare account id: the endpoint is `https://<account id>.r2.cloudflarestorage.com`.
    pub account_id: String,
    /// The bucket (one per chain, e.g. `op-snapshot`).
    pub bucket: String,
    /// The folder in the bucket the chunks, manifest and index go under (e.g. `archive`).
    pub prefix: String,
    /// The R2 API token's access key id.
    pub access_key_id: String,
    /// The R2 API token's secret access key.
    pub secret_access_key: String,
    /// Another endpoint than the account's, if set (an S3-compatible store for tests).
    pub endpoint: Option<String>,
}

impl fmt::Debug for R2Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("R2Config")
            .field("account_id", &self.account_id)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

/// How reads are made.
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions {
    /// Bytes of segments one GET of a stream covers: whole ranges of segments, so that a TB
    /// read costs thousands of requests, not millions (`docs/serving.md` §6.6).
    pub range_bytes: u64,
    /// GETs a stream has in flight: its read-ahead, bounded.
    pub ranges_in_flight: usize,
    /// Send a second, identical GET when the first has not answered after this long, and use
    /// whichever answers first (a hedged read). `None` sends one.
    pub hedge_after: Option<Duration>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            range_bytes: 8 << 20,
            ranges_in_flight: 4,
            hedge_after: None,
        }
    }
}

/// One chain's chunks in one store. Cheap to clone.
#[derive(Clone)]
pub struct ChunkStore {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    options: ReadOptions,
    /// The chain the manifest must be of.
    chain_id: u64,
    genesis_hash: B256,
}

impl fmt::Debug for ChunkStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChunkStore")
            .field("store", &self.inner.store.to_string())
            .field("prefix", &self.inner.prefix)
            .field("options", &self.inner.options)
            .finish()
    }
}

impl ChunkStore {
    /// The chain's chunks on R2.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the client cannot be built from `config`.
    pub fn r2(
        config: &R2Config,
        chain: &ChainSpec,
        options: ReadOptions,
    ) -> Result<Self, ChunksError> {
        let endpoint = config
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{}.r2.cloudflarestorage.com", config.account_id));
        let client = ClientOptions::new()
            .with_pool_max_idle_per_host(POOL_PER_HOST)
            .with_pool_idle_timeout(POOL_IDLE)
            .with_timeout(REQUEST_TIMEOUT)
            .with_connect_timeout(CONNECT_TIMEOUT);
        let retry = RetryConfig {
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(200),
                max_backoff: Duration::from_secs(30),
                base: 2.0,
            },
            max_retries: RETRIES,
            retry_timeout: RETRY_FOR,
        };
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            // R2 takes any region; "auto" is its documented one.
            .with_region("auto")
            .with_bucket_name(&config.bucket)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_client_options(client)
            .with_retry(retry)
            .build()?;
        Ok(Self::new(Arc::new(store), &config.prefix, chain, options))
    }

    /// The chain's chunks in the local directory `dir` (created if missing), in the layout R2
    /// would hold: for tests, the bench and a conversion without credentials.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Io`] if the directory cannot be made, and [`ChunksError::Store`] if
    /// it cannot be used.
    pub fn local(
        dir: &std::path::Path,
        prefix: &str,
        chain: &ChainSpec,
        options: ReadOptions,
    ) -> Result<Self, ChunksError> {
        std::fs::create_dir_all(dir)?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(dir)?;
        Ok(Self::new(Arc::new(store), prefix, chain, options))
    }

    /// The chain's chunks in `store` (any backend: a local directory, memory), under `prefix`.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        chain: &ChainSpec,
        options: ReadOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                prefix: prefix.trim_matches('/').to_owned(),
                options,
                chain_id: chain.chain_id,
                genesis_hash: chain.genesis_hash,
            }),
        }
    }

    /// The chain the store is for: its id and genesis hash, which the manifest records.
    pub(crate) fn chain(&self) -> (u64, B256) {
        (self.inner.chain_id, self.inner.genesis_hash)
    }

    fn path(&self, key: &str) -> Path {
        if self.inner.prefix.is_empty() {
            return Path::from(key);
        }
        Path::from(format!("{}/{key}", self.inner.prefix))
    }

    /// The keys under `dir`, after `after` if given, relative to the chain's prefix.
    pub(crate) async fn list(
        &self,
        dir: &str,
        after: Option<&str>,
    ) -> Result<Vec<String>, ChunksError> {
        let prefix = self.path(dir);
        let listed = match after {
            Some(after) => self
                .inner
                .store
                .list_with_offset(Some(&prefix), &self.path(after)),
            None => self.inner.store.list(Some(&prefix)),
        };
        let strip = if self.inner.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.inner.prefix)
        };
        listed
            .map_ok(|meta| {
                let key = meta.location.to_string();
                key.strip_prefix(&strip).map_or(key.clone(), str::to_owned)
            })
            .try_collect()
            .await
            .map_err(ChunksError::from)
    }

    /// The whole object at `key`.
    pub(crate) async fn get(&self, key: &str) -> Result<Bytes, ChunksError> {
        Ok(self.inner.store.get(&self.path(key)).await?.bytes().await?)
    }

    /// `range` of the object at `key`, hedged if the options say so.
    pub(crate) async fn get_range(
        &self,
        key: &str,
        range: Range<u64>,
    ) -> Result<Bytes, ChunksError> {
        let path = self.path(key);
        let first = self.inner.store.get_range(&path, range.clone());
        tokio::pin!(first);
        let Some(after) = self.inner.options.hedge_after else {
            return Ok(first.await?);
        };
        tokio::select! {
            got = &mut first => return Ok(got?),
            () = tokio::time::sleep(after) => {}
        }
        let second = self.inner.store.get_range(&path, range);
        tokio::pin!(second);
        tokio::select! {
            got = &mut first => Ok(got?),
            got = &mut second => Ok(got?),
        }
    }

    /// Writes `bytes` at `key` unless an object is there. `false` if one was. Uses a
    /// create-only PUT (`If-None-Match: *`) where the store honours it, a plain PUT where it
    /// does not: with one writer per chain the guard is not needed (D8).
    pub(crate) async fn put_once(&self, key: &str, bytes: Bytes) -> Result<bool, ChunksError> {
        let path = self.path(key);
        // A large object goes up in parts, without the guard: one writer per chain (D8).
        if bytes.len() > PART_BYTES {
            let upload = self.inner.store.put_multipart(&path).await?;
            let mut parts = WriteMultipart::new_with_chunk_size(upload, PART_BYTES);
            parts.put(bytes);
            parts.finish().await?;
            return Ok(true);
        }
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .inner
            .store
            .put_opts(&path, PutPayload::from(bytes.clone()), create)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            Err(object_store::Error::NotImplemented { .. }) => {
                self.inner.store.put(&path, PutPayload::from(bytes)).await?;
                Ok(true)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Stores a sealed chunk. Idempotent: its name carries its root, so an object already
    /// stored under it holds the same bytes, and the create-only PUT leaves it be. No HEAD is
    /// sent.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails.
    pub async fn put_chunk(&self, chunk: &SealedChunk) -> Result<(), ChunksError> {
        self.put_once(&chunk.entry.key(), chunk.bytes.clone())
            .await?;
        Ok(())
    }

    /// The keys of every chunk object stored (one LIST), for a writer that resumes: a chunk
    /// already there under its name (which carries its root) need not be uploaded again.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails.
    pub async fn stored_chunks(&self) -> Result<std::collections::HashSet<String>, ChunksError> {
        Ok(self
            .list(crate::CHUNKS_DIR, None)
            .await?
            .into_iter()
            .collect())
    }

    /// Reads a chunk's index: one ranged GET of its tail, checked against `entry`.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails, and [`ChunksError::Integrity`] or
    /// [`ChunksError::Malformed`] if the index is not the manifest's.
    pub async fn index(&self, entry: &ChunkEntry) -> Result<ChunkIndex, ChunksError> {
        let tail = self
            .get_range(&entry.key(), entry.footer_offset..entry.size)
            .await?;
        ChunkIndex::parse(entry, &tail)
    }

    /// Reads one block of a chunk: one ranged GET of its segment, checked and decoded.
    /// `None` if the chunk does not hold `number`.
    ///
    /// # Errors
    ///
    /// Returns [`ChunksError::Store`] if the store fails, and [`ChunksError::Integrity`] or
    /// [`ChunksError::Malformed`] if the segment does not check.
    pub async fn block(
        &self,
        entry: &ChunkEntry,
        index: &ChunkIndex,
        number: u64,
    ) -> Result<Option<ArchivedBlock>, ChunksError> {
        let Some(at) = index.segment_of(number) else {
            return Ok(None);
        };
        let Some(segment) = index.segments.get(at).copied() else {
            return Ok(None);
        };
        let bytes = self.get_range(&entry.key(), segment.range()).await?;
        let parent = index.parent_of(entry, segment.first);
        let blocks = tokio::task::spawn_blocking({
            let entry = *entry;
            move || {
                decode_segment(
                    &entry,
                    &segment,
                    &bytes,
                    parent,
                    number..number.saturating_add(1),
                )
            }
        })
        .await??;
        Ok(blocks.into_iter().next())
    }

    /// Streams the blocks of a chunk from `from` (its first block if `from` is before it),
    /// in order: its index, then GETs of [`ReadOptions::range_bytes`] of segments, up to
    /// [`ReadOptions::ranges_in_flight`] at once, each checked and decoded on a blocking thread
    /// as it arrives. The GETs start at once, so a stream opened for the next chunk while one
    /// is served reads ahead (D4). Dropping the stream stops its reads.
    pub fn stream(&self, entry: &ChunkEntry, from: u64) -> BlockStream {
        // The GETs in flight are the read-ahead; the channel only hands one batch over.
        let (tx, rx) = mpsc::channel(1);
        let store = self.clone();
        let entry = *entry;
        let task = tokio::spawn(async move {
            if let Err(err) = store.produce(entry, from, &tx).await {
                // A reader that left is not told.
                drop(tx.send(Err(err)).await);
            }
        });
        BlockStream {
            rx,
            task,
            batch: Vec::new().into_iter(),
        }
    }

    async fn produce(
        &self,
        entry: ChunkEntry,
        from: u64,
        tx: &mpsc::Sender<Result<Vec<ArchivedBlock>, ChunksError>>,
    ) -> Result<(), ChunksError> {
        let from = from.max(entry.first);
        let index = Arc::new(self.index(&entry).await?);
        let Some(start) = index.segment_of(from) else {
            return Ok(());
        };
        let ranges = ranges(&index, start, self.inner.options.range_bytes);
        let mut decoded = futures_util::stream::iter(ranges)
            .map(|segments| {
                let (store, index) = (self.clone(), Arc::clone(&index));
                async move { store.read_range(entry, index, segments, from).await }
            })
            .buffered(self.inner.options.ranges_in_flight.max(1));
        while let Some(blocks) = decoded.next().await {
            if tx.send(Ok(blocks?)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Reads segments `segments` of a chunk with one GET and decodes their blocks from `from`
    /// on.
    async fn read_range(
        &self,
        entry: ChunkEntry,
        index: Arc<ChunkIndex>,
        segments: Range<usize>,
        from: u64,
    ) -> Result<Vec<ArchivedBlock>, ChunksError> {
        let parts = index.segments.get(segments).unwrap_or_default().to_vec();
        let (Some(first), Some(last)) = (parts.first(), parts.last()) else {
            return Ok(Vec::new());
        };
        let start = first.offset;
        let bytes = self
            .get_range(&entry.key(), start..last.range().end)
            .await?;
        tokio::task::spawn_blocking(move || {
            let mut blocks = Vec::new();
            for segment in &parts {
                let range = segment.range();
                let at = usize::try_from(range.start.saturating_sub(start)).unwrap_or(usize::MAX);
                let end = usize::try_from(range.end.saturating_sub(start)).unwrap_or(usize::MAX);
                let part = bytes.get(at..end).ok_or_else(|| ChunksError::Malformed {
                    key: entry.key(),
                    reason: "a GET returned fewer bytes than its range",
                })?;
                let parent = index.parent_of(&entry, segment.first);
                blocks.extend(decode_segment(
                    &entry,
                    segment,
                    part,
                    parent,
                    from..u64::MAX,
                )?);
            }
            Ok(blocks)
        })
        .await?
    }
}

/// Splits segments `start..` into consecutive runs of about `range_bytes` each (at least one
/// segment per run), each read with one GET.
fn ranges(index: &ChunkIndex, start: usize, range_bytes: u64) -> Vec<Range<usize>> {
    let mut runs = Vec::new();
    let mut run_start = start;
    let mut bytes = 0_u64;
    for (at, segment) in index.segments.iter().enumerate().skip(start) {
        if at > run_start && bytes.saturating_add(u64::from(segment.length)) > range_bytes {
            runs.push(run_start..at);
            run_start = at;
            bytes = 0;
        }
        bytes = bytes.saturating_add(u64::from(segment.length));
    }
    if run_start < index.segments.len() {
        runs.push(run_start..index.segments.len());
    }
    runs
}

/// The blocks of a chunk, from [`ChunkStore::stream`]. Dropping it stops the reads.
#[derive(Debug)]
pub struct BlockStream {
    rx: mpsc::Receiver<Result<Vec<ArchivedBlock>, ChunksError>>,
    task: JoinHandle<()>,
    batch: std::vec::IntoIter<ArchivedBlock>,
}

impl Stream for BlockStream {
    type Item = Result<ArchivedBlock, ChunksError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(block) = self.batch.next() {
                return Poll::Ready(Some(Ok(block)));
            }
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(Ok(batch))) => self.batch = batch.into_iter(),
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for BlockStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}
