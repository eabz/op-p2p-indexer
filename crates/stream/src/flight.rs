//! Arrow Flight: bulk history as columnar record batches, for data pipelines and analytics.
//!
//! A ticket names a table, an inclusive block range and an optional status cap, as text:
//! `table:from:to[:cap]`, with `cap` one of `finalized`, `safe`, `any` (the default). `DoGet`
//! streams the table's rows for the range, one record batch per read of the stores (at most 64
//! blocks or 16 MiB), produced by a task that reads the next batch while it builds one and
//! stays at most one ahead of the consumer. A range longer than [`MAX_FLIGHT_BLOCKS`] is cut
//! to that; the response's `op-indexer-range-to` header gives the last block it covers. A
//! consumer that does not read for 30 s is ended with `RESOURCE_EXHAUSTED`. The live chain,
//! with reorgs, is the gRPC subscription's: Flight serves ranges.
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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt as _};
use tokio_util::task::TaskTracker;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

pub use self::tables::Table;
use crate::Sent;
use crate::convert::Prepared;
use crate::sink::Sink;
use crate::source::{Source, read_status};

/// Record batches a `DoGet` producer may hold ready ahead of the consumer: with the one being
/// built and the one being read, about 50 MiB per stream at most.
const BATCHES_AHEAD: usize = 1;
/// The most blocks one `DoGet` covers; a longer range is cut, so one stream does not hold a
/// permit for days.
pub const MAX_FLIGHT_BLOCKS: u64 = 100_000;

/// A response stream.
type Responses<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// How far a range may reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cap {
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

    /// The cap's name in a ticket.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Safe => "safe",
            Self::Any => "any",
        }
    }
}

/// What a ticket or a descriptor asks for: a table, an inclusive block range and a cap.
#[derive(Debug, Clone, Copy)]
pub struct Query {
    /// The table.
    pub table: Table,
    /// `None` for the lowest block held.
    pub from: Option<BlockNumber>,
    /// The last block, inclusive.
    pub to: BlockNumber,
    /// How far the range may reach.
    pub cap: Cap,
}

/// A request that names something not served.
fn not_served(what: &str, all: impl Iterator<Item = &'static str>) -> Status {
    Status::invalid_argument(format!("{what}: {}", all.collect::<Vec<_>>().join(", ")))
}

impl Query {
    /// The whole of `table`, at any status.
    pub const fn whole(table: Table) -> Self {
        Self {
            table,
            from: None,
            to: BlockNumber::MAX,
            cap: Cap::Any,
        }
    }

    /// Reads `table:from:to[:cap]`.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if the text is not a ticket, or names a table or cap not served.
    pub fn parse(text: &[u8]) -> Result<Self, Status> {
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
        if to < from {
            return Err(Status::invalid_argument("`to` is below `from`"));
        }
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

    /// The ticket of the query: `table:from:to:cap`, `from` 0 when it names none.
    #[must_use]
    pub fn ticket(self) -> Ticket {
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
    /// The bytes sent, which each `FlightData` adds to as it leaves.
    pub(crate) sent: Sent,
}

impl<U, A> Flight<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
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

/// A `DoGet`'s queue of record batches.
type Batches = Sink<RecordBatch, FlightError>;

/// Reads `query` (resolved) and sends its record batches on `batches`, until it is done, the
/// consumer leaves or stops reading, a read fails, or the node shuts down; the last two end
/// the stream with their error.
async fn produce<U: UnsafeStore, A: ArchiveStore>(
    source: Source<U, A>,
    query: Query,
    mut batches: Batches,
    _permit: OwnedSemaphorePermit,
) {
    // A consumer that leaves ends it at once, even while a store call is being retried.
    let left = batches.closed();
    let read = tokio::select! {
        biased;
        () = source.cancel.cancelled() => Err(Status::unavailable("the node is shutting down").into()),
        () = left => Ok(()),
        read = read_range(&source, query, &mut batches) => read,
    };
    if let Err(err) = read {
        batches.end(err);
    }
}

/// A batch being built off the runtime.
type Building = JoinHandle<Result<RecordBatch, FlightError>>;

async fn read_range<U: UnsafeStore, A: ArchiveStore>(
    source: &Source<U, A>,
    query: Query,
    batches: &mut Batches,
) -> Result<(), FlightError> {
    let (mut next, mut archive_tip) = (query.from.unwrap_or(0), None);
    let mut parent: Option<B256> = None;
    // The previous batch, built while the next one is read.
    let mut building: Option<Building> = None;
    while next <= query.to {
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

/// Waits for a batch and sends it. `false` when the stream has ended (the consumer left, or
/// was told it is too slow).
async fn send(building: Building, batches: &mut Batches) -> Result<bool, FlightError> {
    let batch = building
        .await
        .map_err(|err| FlightError::ExternalError(Box::new(err)))??;
    Ok(batches.send(batch).await.is_ok())
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
        let (batches, rx) = Sink::channel(BATCHES_AHEAD);
        self.tasks
            .spawn(produce(self.source.clone(), query, batches, permit));
        let sent = self.sent.clone();
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(ReceiverStream::new(rx))
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
