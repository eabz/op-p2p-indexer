//! Arrow Flight: bulk history as columnar record batches, for data pipelines and analytics.
//!
//! A ticket names a table, an inclusive block range and an optional status cap, as text:
//! `table:from:to[:cap]`, with `cap` one of `finalized`, `safe`, `any` (the default). `DoGet`
//! streams the table's rows for the range, one record batch per read of the stores (at most 64
//! blocks or 16 MiB), produced by a task that reads the next batch while it builds one and
//! stays at most two ahead of the consumer. The live chain, with reorgs, is the gRPC
//! subscription's: Flight serves ranges.
//!
//! `ListFlights`, `GetFlightInfo` and `GetSchema` describe the four tables
//! ([`tables::Table`]) and the range each ticket covers; everything else is `UNIMPLEMENTED`.

mod tables;

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
use arrow_ipc::writer::IpcWriteOptions;
use op_indexer_storage::{ArchiveStore, UnsafeStore};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tokio_util::task::TaskTracker;
use tonic::{Request, Response, Status, Streaming};

use self::tables::Table;
use crate::convert::Prepared;
use crate::source::{Source, read_status};

/// Record batches a `DoGet` producer may hold ready ahead of the consumer.
const BATCHES_AHEAD: usize = 2;

/// A response stream.
type Responses<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// How far a range may reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cap {
    /// Up to the finalized head.
    Finalized,
    /// Up to the safe head.
    Safe,
    /// Up to the unsafe head: blocks above the archive come from the unsafe store, and may
    /// still be reorged.
    Any,
}

impl Cap {
    const ALL: [Self; 3] = [Self::Finalized, Self::Safe, Self::Any];

    const fn name(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Safe => "safe",
            Self::Any => "any",
        }
    }
}

/// What a ticket or a descriptor asks for.
#[derive(Debug, Clone, Copy)]
struct Query {
    table: Table,
    /// `None` for the lowest block held.
    from: Option<BlockNumber>,
    to: BlockNumber,
    cap: Cap,
}

/// A request that names something not served.
fn not_served(what: &str, all: impl Iterator<Item = &'static str>) -> Status {
    Status::invalid_argument(format!("{what}: {}", all.collect::<Vec<_>>().join(", ")))
}

impl Query {
    /// The whole of `table`, at any status.
    const fn whole(table: Table) -> Self {
        Self {
            table,
            from: None,
            to: BlockNumber::MAX,
            cap: Cap::Any,
        }
    }

    /// Reads `table:from:to[:cap]`.
    fn parse(text: &[u8]) -> Result<Self, Status> {
        let invalid = || Status::invalid_argument("a ticket is `table:from:to[:cap]`");
        let text = std::str::from_utf8(text).map_err(|_not_text| invalid())?;
        let mut parts = text.split(':');
        let table = parts
            .next()
            .and_then(Table::parse)
            .ok_or_else(|| not_served("tables", Table::ALL.into_iter().map(Table::name)))?;
        let mut number = || {
            parts
                .next()
                .and_then(|part| part.parse::<BlockNumber>().ok())
                .ok_or_else(invalid)
        };
        let (from, to) = (number()?, number()?);
        let cap = match parts.next() {
            None => Cap::Any,
            Some(name) => Cap::ALL
                .into_iter()
                .find(|cap| cap.name() == name)
                .ok_or_else(|| not_served("caps", Cap::ALL.into_iter().map(Cap::name)))?,
        };
        if parts.next().is_some() {
            return Err(invalid());
        }
        Ok(Self {
            table,
            from: Some(from),
            to,
            cap,
        })
    }

    fn ticket(self) -> Ticket {
        let text = format!(
            "{}:{}:{}:{}",
            self.table.name(),
            self.from.unwrap_or(0),
            self.to,
            self.cap.name()
        );
        Ticket {
            ticket: text.into(),
        }
    }
}

impl TryFrom<&FlightDescriptor> for Query {
    type Error = Status;

    /// A path of one table (all of it), or a ticket's text as the command.
    fn try_from(descriptor: &FlightDescriptor) -> Result<Self, Status> {
        match descriptor.path.as_slice() {
            [table] => Table::parse(table)
                .map(Self::whole)
                .ok_or_else(|| not_served("tables", Table::ALL.into_iter().map(Table::name))),
            [] => Self::parse(&descriptor.cmd),
            _ => Err(Status::invalid_argument("a path names one table")),
        }
    }
}

/// The Flight service.
#[derive(Debug)]
pub(crate) struct Flight<U, A> {
    pub(crate) source: Source<U, A>,
    /// One permit per `DoGet` at once.
    pub(crate) streams: Arc<Semaphore>,
    pub(crate) tasks: TaskTracker,
}

impl<U, A> Flight<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    /// The range `query` covers now: `from` the lowest block held when it names none, `to`
    /// lowered to what its cap allows.
    ///
    /// # Errors
    ///
    /// `OUT_OF_RANGE` if `from` is below the archive's first block, or nothing is held from
    /// `from` under the cap.
    async fn resolve(&self, query: Query) -> Result<Query, Status> {
        let range = self
            .source
            .archive_range()
            .await
            .map_err(|err| read_status(&err))?;
        let (unsafe_head, heads) = self.source.heads().await.map_err(|err| read_status(&err))?;
        let first = range
            .map(|(first, _)| first.number)
            .or(unsafe_head.map(|head| head.number));
        let from = match (query.from, first) {
            (Some(from), Some(first)) if from < first => {
                return Err(Status::out_of_range(format!(
                    "the node holds blocks from {first} on"
                )));
            }
            (Some(from), _) => from,
            (None, first) => first.unwrap_or(0),
        };
        // `None` sorts below every number: a missing head, or an empty archive, holds nothing.
        let archive_tip = range.map(|(_, tip)| tip.number);
        let reach = match query.cap {
            Cap::Finalized => heads.finalized.map(|head| head.number).min(archive_tip),
            Cap::Safe => heads.safe.map(|head| head.number).min(archive_tip),
            Cap::Any => unsafe_head.map(|head| head.number).max(archive_tip),
        };
        let to = reach
            .map(|reach| query.to.min(reach))
            .filter(|to| *to >= from)
            .ok_or_else(|| {
                Status::out_of_range(format!(
                    "no {} block is held from {from} on",
                    query.cap.name()
                ))
            })?;
        Ok(Query {
            from: Some(from),
            to,
            ..query
        })
    }

    /// The `FlightInfo` of `query`, resolved.
    async fn info(&self, query: Query, descriptor: FlightDescriptor) -> Result<FlightInfo, Status> {
        let query = self.resolve(query).await?;
        let schema = query.table.schema().map_err(Status::from)?;
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

/// Reads `query` (resolved) and sends its record batches on `batches`, until it is done, the
/// consumer leaves, or a read fails (sent as the stream's last item).
async fn produce<U: UnsafeStore, A: ArchiveStore>(
    source: Source<U, A>,
    query: Query,
    batches: mpsc::Sender<Result<RecordBatch, FlightError>>,
    _permit: OwnedSemaphorePermit,
) {
    if let Err(err) = read_range(&source, query, &batches).await {
        // A consumer that left is not told.
        drop(batches.send(Err(err)).await);
    }
}

/// A batch being built off the runtime.
type Building = JoinHandle<Result<RecordBatch, FlightError>>;

async fn read_range<U: UnsafeStore, A: ArchiveStore>(
    source: &Source<U, A>,
    query: Query,
    batches: &mpsc::Sender<Result<RecordBatch, FlightError>>,
) -> Result<(), FlightError> {
    let (mut next, mut archive_tip) = (query.from.unwrap_or(0), None);
    let mut parent: Option<B256> = None;
    // The previous batch, built while the next one is read.
    let mut building: Option<Building> = None;
    while next <= query.to {
        if source.cancel.is_cancelled() {
            return Err(Status::unavailable("the node is shutting down").into());
        }
        let heads = source
            .archive_heads()
            .await
            .map_err(|err| read_status(&err))?;
        let mut blocks: Vec<Prepared> = source
            .blocks_from(next, &mut archive_tip)
            .await
            .map_err(|err| read_status(&err))?;
        blocks.retain(|block| block.at.number <= query.to);
        let Some(last) = blocks.last().map(|block| block.at) else {
            return Err(Status::unavailable(format!(
                "block {next} is no longer held (trimmed, or a gap in the unsafe chain)"
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
        let table = query.table;
        let built = tokio::task::spawn_blocking(move || table.batch(&blocks, &heads));
        if let Some(previous) = building.replace(built)
            && !send(previous, batches).await?
        {
            return Ok(());
        }
        next = last.number.saturating_add(1);
    }
    if let Some(last) = building {
        send(last, batches).await?;
    }
    Ok(())
}

/// Waits for a batch and sends it. `false` when the consumer has left.
async fn send(
    building: Building,
    batches: &mpsc::Sender<Result<RecordBatch, FlightError>>,
) -> Result<bool, FlightError> {
    let batch = building
        .await
        .map_err(|err| FlightError::ExternalError(Box::new(err)))??;
    Ok(batches.send(Ok(batch)).await.is_ok())
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
    A: ArchiveStore + Clone + Send + Sync + 'static,
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
        let mut infos = Vec::with_capacity(Table::ALL.len());
        for table in Table::ALL {
            let descriptor = FlightDescriptor::new_path(vec![table.name().to_owned()]);
            infos.push(self.info(Query::whole(table), descriptor).await);
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let query = Query::try_from(&descriptor)?;
        self.info(query, descriptor).await.map(Response::new)
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
        let schema = query.table.schema().map_err(Status::from)?;
        SchemaResult::try_from(SchemaAsIpc::new(&schema, &IpcWriteOptions::default()))
            .map(Response::new)
            .map_err(|err| Status::from(FlightError::Arrow(err)))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let query = Query::parse(&request.into_inner().ticket)?;
        let permit = Arc::clone(&self.streams)
            .try_acquire_owned()
            .map_err(|_full| Status::resource_exhausted("too many Flight streams at once"))?;
        let query = self.resolve(query).await?;
        let schema = query.table.schema().map_err(Status::from)?;
        let (tx, rx) = mpsc::channel(BATCHES_AHEAD);
        self.tasks
            .spawn(produce(self.source.clone(), query, tx, permit));
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(ReceiverStream::new(rx))
            .map(|data| data.map_err(Status::from));
        Ok(Response::new(Box::pin(encoded)))
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
