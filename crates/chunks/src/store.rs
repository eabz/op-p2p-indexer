//! The object store a chain's chunks live in: R2 through its S3 API, or any `object_store`
//! backend (a local directory, memory) with the same layout.
//!
//! Reads keep no data: a chunk is fetched with ranged GETs over pooled keep-alive connections,
//! checked, decoded and handed out. A stream reads ahead a bounded number of ranges (its
//! channel), and only while it lives.

use std::collections::VecDeque;
use std::fmt;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use alloy_primitives::B256;
use bytes::Bytes;
use futures_util::{Stream, TryStreamExt as _};
use object_store::http::HttpBuilder;
use object_store::path::Path;
use object_store::{
    BackoffConfig, ClientOptions, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions,
    PutPayload, RetryConfig, WriteMultipart,
};
use op_indexer_chainspec::ChainSpec;
use op_indexer_primitives::ArchivedBlock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::debug;

use crate::format::{ChunkIndex, SealedChunk, decode_segment};
use crate::manifest::ChunkEntry;
use crate::{CHUNKS_DIR, ChunksError};

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
    /// The bucket's public custom domain behind Cloudflare's cache (e.g.
    /// `https://chunks.example.com`), if set: sealed chunks are read from it over plain
    /// HTTPS, the S3 API taking over when it fails. The manifest and index are always read
    /// through the S3 API (`docs/serving.md`, Cloudflare cache).
    pub public_url: Option<String>,
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
            .field("public_url", &self.public_url)
            .finish()
    }
}

/// How reads are made.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadOptions {
    /// Send a second, identical GET when the first has not answered after this long, and use
    /// whichever answers first (a hedged read). `None` sends one.
    pub hedge_after: Option<Duration>,
}

/// How one chunk stream reads (see [`ChunkStore::stream`]).
#[derive(Debug, Clone)]
pub struct StreamReads {
    /// Bytes of segments one GET covers.
    pub range_bytes: u64,
    /// GETs the stream always has in flight.
    pub in_flight: usize,
    /// More GETs a budget shared with other streams lends while it has room, so a stream
    /// reads wider when few others do; none lends nothing.
    pub lend: Option<Lend>,
}

impl StreamReads {
    /// Reads of `range_bytes`, `in_flight` at once, lent none.
    #[must_use]
    pub const fn fixed(range_bytes: u64, in_flight: usize) -> Self {
        Self {
            range_bytes,
            in_flight,
            lend: None,
        }
    }
}

/// What a budget shared with other streams lends one.
#[derive(Debug, Clone)]
pub struct Lend {
    /// The budget, whose permits a GET past the stream's base holds until its range is
    /// decoded.
    pub budget: Arc<Semaphore>,
    /// Permits one such GET holds.
    pub per_get: u32,
    /// GETs in flight in all, at most.
    pub max_in_flight: usize,
}

/// One chain's chunks in one store. Cheap to clone.
#[derive(Clone)]
pub struct ChunkStore {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn ObjectStore>,
    /// The bucket's public domain, for chunk reads: see [`R2Config::public_url`].
    public: Option<Arc<dyn ObjectStore>>,
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
        // HTTP/1.1: over HTTP/2 every request in flight is multiplexed on one connection,
        // which caps a whole export or server near one connection's throughput (~150 MB/s
        // measured to R2); separate pooled connections scale with the requests in flight.
        let client = ClientOptions::new()
            .with_http1_only()
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
        let public = config
            .public_url
            .as_ref()
            .map(|url| {
                // One quick retry: the S3 API is the fallback.
                let retry = RetryConfig {
                    max_retries: 1,
                    ..retry.clone()
                };
                HttpBuilder::new()
                    .with_url(url)
                    .with_client_options(client.clone())
                    .with_retry(retry)
                    .build()
            })
            .transpose()?;
        let store = config
            .s3()
            .with_client_options(client)
            .with_retry(retry)
            .build()?;
        let public = public.map(|public| Arc::new(public) as Arc<dyn ObjectStore>);
        Ok(Self::with_stores(
            Arc::new(store),
            public,
            &config.prefix,
            chain,
            options,
        ))
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
        Self::with_stores(store, None, prefix, chain, options)
    }

    /// The chain's chunks in `store`, chunks read from `public` first if given.
    fn with_stores(
        store: Arc<dyn ObjectStore>,
        public: Option<Arc<dyn ObjectStore>>,
        prefix: &str,
        chain: &ChainSpec,
        options: ReadOptions,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                public,
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
        let first = self.read_range(key, &path, range.clone());
        tokio::pin!(first);
        let Some(after) = self.inner.options.hedge_after else {
            return Ok(first.await?);
        };
        tokio::select! {
            got = &mut first => return Ok(got?),
            () = tokio::time::sleep(after) => {}
        }
        let second = self.read_range(key, &path, range);
        tokio::pin!(second);
        tokio::select! {
            got = &mut first => Ok(got?),
            got = &mut second => Ok(got?),
        }
    }

    /// One GET of `range` of the object at `key`: a chunk's from the public domain if one is
    /// set, through the S3 API if that fails; anything else through the S3 API.
    async fn read_range(
        &self,
        key: &str,
        path: &Path,
        range: Range<u64>,
    ) -> Result<Bytes, object_store::Error> {
        if let Some(public) = &self.inner.public
            && key.starts_with(CHUNKS_DIR)
        {
            match public.get_range(path, range.clone()).await {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    debug!(%err, key, "public chunk read failed; reading through the S3 API");
                }
            }
        }
        self.inner.store.get_range(path, range).await
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
        Ok(self.list(CHUNKS_DIR, None).await?.into_iter().collect())
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
    /// in order: its index, then GETs of segments as `reads` says. The GETs run on their own
    /// tasks, so they keep arriving while ranges are decoded; ranges are decoded in order on
    /// blocking threads, two at once, so a stream holds its GETs in flight,
    /// compressed, and three ranges decoded (one handed on). The GETs start at once, so a
    /// stream opened for the next chunk while one is served reads ahead (D4). Dropping the
    /// stream stops its reads and gives back what it was lent.
    pub fn stream(&self, entry: &ChunkEntry, from: u64, reads: StreamReads) -> BlockStream {
        // The GETs in flight are the read-ahead; the channel only hands one batch over.
        let (tx, rx) = mpsc::channel(1);
        let store = self.clone();
        let entry = *entry;
        let task = Aborting(tokio::spawn(async move {
            if let Err(err) = store.produce(entry, from, reads, &tx).await {
                // A reader that left is not told.
                drop(tx.send(Err(err)).await);
            }
        }));
        BlockStream {
            rx,
            _task: task,
            batch: Vec::new().into_iter(),
        }
    }

    async fn produce(
        &self,
        entry: ChunkEntry,
        from: u64,
        reads: StreamReads,
        tx: &mpsc::Sender<Result<Vec<ArchivedBlock>, ChunksError>>,
    ) -> Result<(), ChunksError> {
        let from = from.max(entry.first);
        let index = Arc::new(self.index(&entry).await?);
        let Some(start) = index.segment_of(from) else {
            return Ok(());
        };
        let mut ranges = ranges(&index, start, reads.range_bytes)
            .into_iter()
            .peekable();
        let base = reads.in_flight.max(1);
        let most = reads
            .lend
            .as_ref()
            .map_or(base, |lend| lend.max_in_flight.max(base));
        // In order: the GETs in flight, and the ranges being decoded.
        let mut getting: VecDeque<Getting> = VecDeque::new();
        let mut decoding: VecDeque<Decoding> = VecDeque::new();
        loop {
            while getting.len() < most && ranges.peek().is_some() {
                let lent = if getting.len() < base {
                    None
                } else {
                    let Some(permit) = reads.lend.as_ref().and_then(|lend| {
                        Arc::clone(&lend.budget)
                            .try_acquire_many_owned(lend.per_get)
                            .ok()
                    }) else {
                        break;
                    };
                    Some(permit)
                };
                let Some(segments) = ranges.next() else {
                    break;
                };
                let get = self.get_segments(entry, &index, segments.clone());
                getting.push_back((get, segments, lent));
            }
            // A range decoded is handed on once the next is decoding too, or nothing is left.
            if decoding.len() >= DECODED_AHEAD || (getting.is_empty() && !decoding.is_empty()) {
                let Some((decoded, _lent)) = decoding.pop_front() else {
                    return Ok(());
                };
                if tx.send(Ok(decoded.await??)).await.is_err() {
                    return Ok(());
                }
                continue;
            }
            let Some((got, segments, lent)) = getting.pop_front() else {
                return Ok(());
            };
            let bytes = got.await??;
            let index = Arc::clone(&index);
            let decode = Aborting(tokio::task::spawn_blocking(move || {
                decode_range(&entry, &index, segments, &bytes, from)
            }));
            decoding.push_back((decode, lent));
        }
    }

    /// Starts the GET of `segments` of a chunk, on its own task.
    fn get_segments(
        &self,
        entry: ChunkEntry,
        index: &ChunkIndex,
        segments: Range<usize>,
    ) -> Aborting<Result<Bytes, ChunksError>> {
        let parts = index.segments.get(segments).unwrap_or_default();
        let span = match (parts.first(), parts.last()) {
            (Some(first), Some(last)) => first.offset..last.range().end,
            _ => 0..0,
        };
        let store = self.clone();
        Aborting(tokio::spawn(async move {
            if span.is_empty() {
                return Ok(Bytes::new());
            }
            store.get_range(&entry.key(), span).await
        }))
    }
}

/// Ranges of a stream decoded at once at most.
const DECODED_AHEAD: usize = 2;

/// What a budget lent a GET, if anything.
type Lent = Option<OwnedSemaphorePermit>;
/// A GET in flight, of segments of a chunk.
type Getting = (Aborting<Result<Bytes, ChunksError>>, Range<usize>, Lent);
/// A range being decoded.
type Decoding = (Aborting<Result<Vec<ArchivedBlock>, ChunksError>>, Lent);

/// A task, aborted if dropped before it ends: a stream dropped stops its reads.
#[derive(Debug)]
struct Aborting<T>(JoinHandle<T>);

impl<T> Future for Aborting<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

impl<T> Drop for Aborting<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Decodes the blocks from `from` on of `segments` of a chunk, read with one GET from the
/// first one's offset as `bytes`. Blocking, CPU-bound.
fn decode_range(
    entry: &ChunkEntry,
    index: &ChunkIndex,
    segments: Range<usize>,
    bytes: &Bytes,
    from: u64,
) -> Result<Vec<ArchivedBlock>, ChunksError> {
    let parts = index.segments.get(segments).unwrap_or_default();
    let Some(start) = parts.first().map(|segment| segment.offset) else {
        return Ok(Vec::new());
    };
    let mut blocks = Vec::new();
    for segment in parts {
        let range = segment.range();
        let at = usize::try_from(range.start.saturating_sub(start)).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end.saturating_sub(start)).unwrap_or(usize::MAX);
        let part = bytes.get(at..end).ok_or_else(|| ChunksError::Malformed {
            key: entry.key(),
            reason: "a GET returned fewer bytes than its range",
        })?;
        let parent = index.parent_of(entry, segment.first);
        blocks.extend(decode_segment(
            entry,
            segment,
            part,
            parent,
            from..u64::MAX,
        )?);
    }
    Ok(blocks)
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
    /// Aborted with the stream.
    _task: Aborting<()>,
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
