//! Arrow Flight: bulk history as columnar record batches, for data pipelines and analytics.
//!
//! A ticket names a table, an inclusive block range and an optional status cap, as text:
//! `table:from:to[:cap]`, with `cap` one of `finalized`, `safe`, `any` (the default). `DoGet`
//! streams the table's rows for the range, one record batch per read of the stores (at most 64
//! blocks or 16 MiB). A task reads the stores in order and hands each read to a blocking
//! thread, which converts it to a record batch and encodes it as Flight messages (cut to
//! gRPC-sized pieces, IPC buffers compressed when asked); up to [`PARALLEL_BUILDS`] reads are
//! built at once, so one stream uses several cores, and their messages are sent in order. A
//! `DoGet` asks for compression with the [`COMPRESSION_HEADER`] metadata (`lz4` or `zstd`);
//! readers such as pyarrow undo it transparently. A range longer than [`MAX_FLIGHT_BLOCKS`] is
//! cut to that; the response's `op-indexer-range-to` header gives the last block it covers. A
//! consumer that does not read for 30 s is ended with `RESOURCE_EXHAUSTED`. The live chain,
//! with reorgs, is the gRPC subscription's: Flight serves ranges.
//!
//! `ListFlights`, `GetFlightInfo` and `GetSchema` describe the four tables
//! ([`Table`]) and the range each ticket covers; everything else is `UNIMPLEMENTED`.

mod tables;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use alloy_primitives::{B256, BlockNumber};
use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use arrow_ipc::CompressionType;
use arrow_ipc::writer::IpcWriteOptions;
use op_indexer_storage::{ArchiveStore, UnsafeStore};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tokio_util::task::TaskTracker;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Response, Status, Streaming};

use self::tables::TableRows;
use crate::Sent;
use crate::convert::Prepared;
use crate::sink::Sink;
use crate::source::{Source, read_status};
use op_indexer_api::ticket::{Cap, MAX_FLIGHT_BLOCKS, Query, Table};

/// Reads of one `DoGet` built at once, each on a blocking thread: the cores one stream may
/// use. A build runs to its end even while the consumer is slow, so each holds up to a read
/// (16 MiB), its record batch and its encoded messages: about 100 MiB per stream at most.
const PARALLEL_BUILDS: usize = 2;

/// Encoded Flight messages (about 2 MiB each) queued ahead of the consumer.
const MESSAGES_AHEAD: usize = 4;

/// The request metadata a `DoGet` names the compression of its record batches' IPC buffers
/// with: `lz4` (LZ4 frame), `zstd`, or `none` (the default).
const COMPRESSION_HEADER: &str = "op-indexer-compression";

/// A response stream.
type Responses<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The Flight service.
#[derive(Debug)]
pub(crate) struct Flight<U, A> {
    pub(crate) source: Source<U, A>,
    /// One permit per `DoGet` at once.
    pub(crate) streams: Arc<Semaphore>,
    pub(crate) tasks: TaskTracker,
    /// The bytes sent, which each `FlightData` adds to as it leaves.
    pub(crate) sent: Sent,
}

impl<U, A> Flight<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore,
{
    /// The range `query` covers now: `from` the lowest block held when it names none, `to`
    /// lowered to what its cap allows, below a gap above `from`, and to [`MAX_FLIGHT_BLOCKS`]
    /// blocks.
    ///
    /// # Errors
    ///
    /// `OUT_OF_RANGE` if the stores do not hold `from` and never will (see
    /// [`crate::source::Holdings::held_from`]), or nothing is held from `from` under the cap; `UNAVAILABLE`
    /// if `from` is in a gap range sync is filling.
    async fn resolve(&self, query: Query) -> Result<Query, Status> {
        let asked = query.from.unwrap_or(0);
        let (holdings, (unsafe_head, heads)) =
            tokio::try_join!(self.source.holdings(), self.source.heads())
                .map_err(|err| read_status(&err))?;
        if query.from.is_some() {
            holdings.ensure_held(asked)?;
        }
        let from = holdings.held_from(asked);
        // Only with range sync can `from` be in the gap: it is being filled.
        if let Some((start, end)) = holdings.gap_at(from) {
            return Err(Status::unavailable(format!(
                "blocks {start} to {} are not held yet: range sync is filling them",
                end.saturating_sub(1)
            )));
        }
        let (range, gap) = (holdings.archive, holdings.gap());
        // `None` sorts below every number: a missing head, or an empty archive, holds nothing.
        let archive_tip = range.map(|(_, tip)| tip.number);
        let reach = match query.cap {
            Cap::Finalized => heads.finalized.map(|head| head.number).min(archive_tip),
            Cap::Safe => heads.safe.map(|head| head.number).min(archive_tip),
            Cap::Any => unsafe_head.map(|head| head.number).max(archive_tip),
        };
        let Some(reach) = reach.filter(|reach| *reach >= from) else {
            return Err(Status::out_of_range(match query.cap {
                Cap::Any => format!("the node holds no block from {from} on"),
                cap @ (Cap::Finalized | Cap::Safe) => format!(
                    "the {} head is below block {from}, or not known yet",
                    cap.name()
                ),
            }));
        };
        // A range stops below the gap: what is above it is read once the gap is filled.
        let below_gap = gap
            .filter(|(start, _)| from < *start)
            .map_or(BlockNumber::MAX, |(start, _)| start.saturating_sub(1));
        let to = query
            .to
            .min(reach)
            .min(below_gap)
            .min(from.saturating_add(MAX_FLIGHT_BLOCKS - 1));
        Ok(Query {
            from: Some(from),
            to,
            ..query
        })
    }

    /// The `FlightInfo` of `query`, resolved.
    fn info(query: Query, descriptor: FlightDescriptor) -> Result<FlightInfo, Status> {
        let schema = query.table.schema();
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::from(FlightError::Arrow(err)))?;
        Ok(info
            .with_descriptor(descriptor)
            .with_endpoint(FlightEndpoint::new().with_ticket(query.ticket()))
            .with_total_records(-1)
            .with_total_bytes(-1))
    }
}

/// A `DoGet`'s queue of encoded Flight messages.
type Messages = Sink<FlightData, FlightError>;

/// Reads `query` (resolved) and sends its record batches on `messages`, encoded with
/// `options`, until it is done, the consumer leaves or stops reading, a read fails, or the node
/// shuts down; the last two end the stream with their error.
async fn produce<U: UnsafeStore, A: ArchiveStore>(
    source: Source<U, A>,
    query: Query,
    options: IpcWriteOptions,
    mut messages: Messages,
    _permit: OwnedSemaphorePermit,
) {
    // A consumer that leaves ends it at once, even while a store call is being retried.
    let left = messages.closed();
    let read = tokio::select! {
        biased;
        () = source.cancel.cancelled() => Err(Status::unavailable("the node is shutting down").into()),
        () = left => Ok(()),
        read = read_range(&source, query, &options, &mut messages) => read,
    };
    if let Err(err) = read {
        messages.end(err);
    }
}

/// A read being converted and encoded off the runtime.
type Building = JoinHandle<Result<Vec<FlightData>, FlightError>>;

async fn read_range<U: UnsafeStore, A: ArchiveStore>(
    source: &Source<U, A>,
    query: Query,
    options: &IpcWriteOptions,
    messages: &mut Messages,
) -> Result<(), FlightError> {
    let mut next = query.from.unwrap_or(0);
    let mut history = crate::source::History::default();
    let mut parent: Option<B256> = None;
    // The reads being built, oldest first, each sent once it is built and those before it are.
    let mut building: VecDeque<Building> = VecDeque::with_capacity(PARALLEL_BUILDS);
    while next <= query.to {
        let heads = source
            .archive_heads()
            .await
            .map_err(|err| read_status(&err))?;
        let mut blocks: Vec<Prepared> = source
            .blocks_from(next, &mut history)
            .await
            .map_err(|err| read_status(&err))?;
        blocks.retain(|block| block.at.number <= query.to);
        let Some(last) = blocks.last().map(|block| block.at) else {
            return Err(Status::unavailable(format!(
                "block {next} is not held (a gap in the unsafe chain, or expired from it unpromoted)"
            ))
            .into());
        };
        for block in &blocks {
            if parent.is_some_and(|parent| parent != block.parent) {
                return Err(Status::aborted(format!(
                    "the chain was reorganized at block {} during the read",
                    block.at.number
                ))
                .into());
            }
            parent = Some(block.at.hash);
        }
        let (table, options) = (query.table, options.clone());
        building.push_back(tokio::task::spawn_blocking(move || {
            encode(table.batch(&blocks, &heads)?, options)
        }));
        if !flush(&mut building, PARALLEL_BUILDS - 1, messages).await? {
            return Ok(());
        }
        next = last.number.saturating_add(1);
    }
    flush(&mut building, 0, messages).await?;
    Ok(())
}

/// Sends the oldest reads, in order, as each is built, until `keep` are left being built.
/// `false` when the stream has ended (the consumer left, or was told it is too slow).
async fn flush(
    building: &mut VecDeque<Building>,
    keep: usize,
    messages: &mut Messages,
) -> Result<bool, FlightError> {
    while building.len() > keep {
        let Some(oldest) = building.pop_front() else {
            break;
        };
        let built = oldest
            .await
            .map_err(|err| FlightError::ExternalError(Box::new(err)))??;
        for message in built {
            if messages.send(message).await.is_err() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Encodes `batch` as Flight messages with `options`: arrow-flight's encoder, which cuts it
/// into gRPC-sized pieces and compresses as asked, without the schema message it starts with
/// (the stream sends that once). CPU work, for a blocking thread of the runtime.
fn encode(batch: RecordBatch, options: IpcWriteOptions) -> Result<Vec<FlightData>, FlightError> {
    let encoder = FlightDataEncoderBuilder::new()
        .with_options(options)
        .build(tokio_stream::iter([Ok(batch)]));
    // Its input is ready at once: this returns as soon as the batch is encoded.
    let messages: Vec<FlightData> =
        tokio::runtime::Handle::current().block_on(encoder.collect::<Result<_, _>>())?;
    Ok(messages.into_iter().skip(1).collect())
}

/// The IPC options a `DoGet` asked for with [`COMPRESSION_HEADER`].
fn write_options(metadata: &MetadataMap) -> Result<IpcWriteOptions, Status> {
    let compression = match metadata.get(COMPRESSION_HEADER).map(|value| value.to_str()) {
        None | Some(Ok("none")) => None,
        Some(Ok("lz4")) => Some(CompressionType::LZ4_FRAME),
        Some(Ok("zstd")) => Some(CompressionType::ZSTD),
        Some(_) => {
            return Err(Status::invalid_argument(format!(
                "{COMPRESSION_HEADER} must be lz4, zstd or none"
            )));
        }
    };
    IpcWriteOptions::default()
        .try_with_compression(compression)
        .map_err(|err| Status::internal(err.to_string()))
}

fn unimplemented<T>() -> Result<T, Status> {
    Err(Status::unimplemented(
        "not served: only DoGet and the descriptions",
    ))
}

#[tonic::async_trait]
impl<U, A> FlightService for Flight<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore,
{
    type HandshakeStream = Responses<HandshakeResponse>;
    type ListFlightsStream = Responses<FlightInfo>;
    type DoGetStream = Responses<FlightData>;
    type DoPutStream = Responses<PutResult>;
    type DoExchangeStream = Responses<FlightData>;
    type DoActionStream = Responses<arrow_flight::Result>;
    type ListActionsStream = Responses<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        unimplemented()
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // Every table covers the same blocks: the range is resolved once.
        let resolved = self.resolve(Query::whole(Table::Blocks)).await;
        let infos: Vec<_> = Table::ALL
            .into_iter()
            .map(|table| {
                let descriptor = FlightDescriptor::new_path(vec![table.name().to_owned()]);
                resolved
                    .clone()
                    .and_then(|query| Self::info(Query { table, ..query }, descriptor))
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = self.resolve(Query::try_from(&descriptor)?).await?;
        Self::info(query, descriptor).map(Response::new)
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented()
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let query = Query::try_from(&request.into_inner())?;
        let schema = query.table.schema();
        SchemaResult::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
            .map(Response::new)
            .map_err(|err| Status::from(FlightError::Arrow(err)))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let options = write_options(request.metadata())?;
        let query = Query::parse(&request.into_inner().ticket)?;
        let permit = Arc::clone(&self.streams)
            .try_acquire_owned()
            .map_err(|_full| Status::resource_exhausted("too many Flight streams at once"))?;
        let query = self.resolve(query).await?;
        let schema: FlightData = SchemaAsIpc::new(&query.table.schema(), &options).into();
        let (messages, rx) = Sink::channel(MESSAGES_AHEAD);
        self.tasks.spawn(produce(
            self.source.clone(),
            query,
            options,
            messages,
            permit,
        ));
        let sent = self.sent.clone();
        let encoded = tokio_stream::once(Ok(schema))
            .chain(ReceiverStream::new(rx))
            .map(move |data| {
                let data = data.map_err(Status::from)?;
                sent.message(&data);
                Ok(data)
            });
        let mut response = Response::new(Box::pin(encoded) as Self::DoGetStream);
        response
            .metadata_mut()
            .insert("op-indexer-range-to", MetadataValue::from(query.to));
        Ok(response)
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        unimplemented()
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        unimplemented()
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        unimplemented()
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        unimplemented()
    }
}
