//! Arrow Flight on the balancer (D16): `GetFlightInfo` and `ListFlights` only. A range becomes
//! jobs, which the client fetches with `DoGet` from the servers, in parallel:
//!
//! - one per sealed chunk the range touches, its ticket clipped to the chunk; every healthy
//!   server serves it;
//! - the part above the last sealed chunk; only a server whose head (under the ticket's cap)
//!   reaches the job's last block, and that holds every block through it
//!   (`contiguous_through`: a server without range sync has a gap above the sealed chunks),
//!   serves it.
//!
//! A `raw` descriptor (`raw:from:to`, or the path `raw`) asks for whole sealed chunks
//! instead: one endpoint per chunk, its location a presigned GET URL of the chunk's object on
//! R2 (good for [`RAW_URL_TTL`]), its app metadata the chunk's manifest entry as JSON (size,
//! root, first parent, last hash), from which the client checks what it downloads. No server
//! is involved; the balancer needs a presign key for it ([`ChunkSigner`]).
//!
//! Every job is also cut to the servers' own limit, [`ticket::MAX_FLIGHT_BLOCKS`] blocks.
//!
//! Each job names up to [`LOCATIONS`] servers, the least loaded first
//! ([`Picker`](crate::table::Picker): the load counts the jobs already handed out for the same
//! range, so a large range spreads over every server); the client moves to the next if one
//! fails. Tickets and descriptors are the servers'
//! own ([`ticket`]): a ticket from here works on any server. The range is clipped by the best
//! reach the servers report (head and contiguity); each server clips again by its own when it
//! serves.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::BlockNumber;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use op_indexer_api::ticket::{self, Cap, Query};
use op_indexer_chunks::{ChunkEntry, ChunkSigner};
use tokio::sync::watch;
use tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::table::{Slot, Table};

/// Servers named by each job: the client moves to the next if one fails.
const LOCATIONS: usize = 3;
/// How long a presigned chunk URL is good for: time to start every download of a plan.
const RAW_URL_TTL: Duration = Duration::from_mins(10);
/// Chunks in one raw plan (about 40 MB each, ~1 KB of plan each): the client asks again from
/// the block after the last for more, and the plan stays well under a gRPC message's 4 MB.
const MAX_RAW_CHUNKS: usize = 1024;

/// A response stream.
type Responses<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

/// The Flight service.
#[derive(Debug)]
pub(crate) struct Flight {
    pub(crate) table: Table,
    /// The sealed chunks, in block order, as the manifest last read lists them.
    pub(crate) chunks: watch::Receiver<Arc<[ChunkEntry]>>,
    /// Signs the raw plans' URLs; none: raw plans are refused.
    pub(crate) signer: Option<ChunkSigner>,
}

/// One job: an inclusive block range and the addresses of the servers to fetch it from, in
/// order.
#[derive(Debug)]
struct Job {
    from: BlockNumber,
    to: BlockNumber,
    locations: Vec<String>,
}

impl Flight {
    /// The jobs of `query`'s range, clipped to what the servers hold under its cap.
    ///
    /// # Errors
    ///
    /// `UNAVAILABLE` if no healthy server is registered; `OUT_OF_RANGE` if the range starts
    /// below the first sealed chunk, or nothing is held from its first block under the cap.
    fn jobs(&self, query: Query) -> Result<Vec<Job>, Status> {
        let mut picker = self.table.picker();
        if picker.servers().is_empty() {
            return Err(Status::unavailable("no healthy server is registered"));
        }
        let chunks = Arc::clone(&self.chunks.borrow());
        // A whole table starts at the first sealed chunk, or at block 0 before any is sealed.
        let from = query
            .from
            .or_else(|| chunks.first().map(|chunk| chunk.first))
            .unwrap_or(0);
        if let Some(first) = chunks.first().filter(|chunk| from < chunk.first) {
            return Err(Status::out_of_range(format!(
                "the chain's history starts at block {}",
                first.first
            )));
        }
        let best = picker
            .servers()
            .iter()
            .filter_map(|server| server.reach(query.cap))
            .max();
        let Some(to) = best.map(|best| best.min(query.to)).filter(|to| *to >= from) else {
            return Err(Status::out_of_range(format!(
                "no server holds block {from} under the cap {}",
                query.cap.name()
            )));
        };

        let mut jobs = Vec::new();
        let first = chunks.partition_point(|chunk| chunk.last < from);
        let touched = chunks
            .get(first..)
            .unwrap_or_default()
            .iter()
            .take_while(|chunk| chunk.first <= to);
        let mut next = from;
        for chunk in touched {
            for (piece_from, piece_to) in pieces(next.max(chunk.first), chunk.last.min(to)) {
                let locations = picker.pick(LOCATIONS, Slot::Flight, |_| true);
                jobs.push(Job {
                    from: piece_from,
                    to: piece_to,
                    locations,
                });
            }
            next = chunk.last.saturating_add(1);
        }
        // Above the sealed chunks: only a server that holds every block through the piece
        // under the cap. The best reaches `to`, so every piece has one.
        for (piece_from, piece_to) in pieces(next, to) {
            let locations = picker.pick(LOCATIONS, Slot::Flight, |server| {
                server.reach(query.cap).is_some_and(|last| last >= piece_to)
            });
            jobs.push(Job {
                from: piece_from,
                to: piece_to,
                locations,
            });
        }
        Ok(jobs)
    }

    /// The raw plan of `from..=to` (`from` none: the first sealed chunk): an endpoint per
    /// sealed chunk it touches, at most [`MAX_RAW_CHUNKS`].
    ///
    /// # Errors
    ///
    /// `FAILED_PRECONDITION` without a signer; `OUT_OF_RANGE` if no sealed chunk holds `from`;
    /// `INTERNAL` if a URL cannot be signed.
    async fn raw(
        &self,
        (from, to): (Option<BlockNumber>, BlockNumber),
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let Some(signer) = &self.signer else {
            return Err(Status::failed_precondition(
                "this balancer hands out no raw chunks (it has no presign key)",
            ));
        };
        let chunks = Arc::clone(&self.chunks.borrow());
        let from = from.unwrap_or(0);
        let first = chunks.partition_point(|chunk| chunk.last < from);
        let touched: Vec<ChunkEntry> = chunks
            .get(first..)
            .unwrap_or_default()
            .iter()
            .take_while(|chunk| chunk.first <= to)
            .take(MAX_RAW_CHUNKS)
            .copied()
            .collect();
        if touched.is_empty() {
            return Err(Status::out_of_range(format!(
                "no sealed chunk holds block {from}"
            )));
        }
        let endpoints = futures_util::future::try_join_all(touched.iter().map(|entry| async {
            let url = signer
                .url(entry, RAW_URL_TTL)
                .await
                .map_err(|err| Status::internal(format!("failed to sign a chunk URL: {err}")))?;
            let metadata = serde_json::to_vec(entry)
                .map_err(|err| Status::internal(format!("failed to encode a chunk: {err}")))?;
            Ok::<_, Status>(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(format!("raw:{}:{}", entry.first, entry.last)))
                    .with_location(url.as_str())
                    .with_app_metadata(metadata),
            )
        }))
        .await?;
        let bytes = touched.iter().map(|entry| entry.size).sum::<u64>();
        Ok(FlightInfo::new()
            .with_descriptor(descriptor)
            .with_endpoints(endpoints)
            .with_ordered(true)
            .with_total_records(-1)
            .with_total_bytes(i64::try_from(bytes).unwrap_or(i64::MAX)))
    }

    /// The `FlightInfo` of `jobs` for `table` under `cap`.
    fn info(
        table: ticket::Table,
        cap: Cap,
        jobs: &[Job],
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let schema = table.schema();
        let endpoints = jobs
            .iter()
            .map(|job| {
                let ticket = Query {
                    table,
                    from: Some(job.from),
                    to: job.to,
                    cap,
                }
                .ticket();
                job.locations.iter().fold(
                    FlightEndpoint::new().with_ticket(ticket),
                    |endpoint, address| endpoint.with_location(format!("grpc+tcp://{address}")),
                )
            })
            .collect();
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::from(arrow_flight::error::FlightError::Arrow(err)))?;
        Ok(info
            .with_descriptor(descriptor)
            .with_endpoints(endpoints)
            .with_ordered(true)
            .with_total_records(-1)
            .with_total_bytes(-1))
    }
}

/// `from..=to` (empty if `to < from`) in pieces a server serves in one `DoGet`: at most
/// [`ticket::MAX_FLIGHT_BLOCKS`] blocks each, whatever the size of a chunk.
fn pieces(from: BlockNumber, to: BlockNumber) -> impl Iterator<Item = (BlockNumber, BlockNumber)> {
    let mut next = (from <= to).then_some(from);
    std::iter::from_fn(move || {
        let start = next?;
        let end = to.min(start.saturating_add(ticket::MAX_FLIGHT_BLOCKS - 1));
        next = (end < to).then(|| end.saturating_add(1));
        Some((start, end))
    })
}

/// The range of a raw descriptor: the path `raw` (every sealed chunk), or the command
/// `raw:from:to`. `None` for any other descriptor.
fn raw_range(
    descriptor: &FlightDescriptor,
) -> Result<Option<(Option<BlockNumber>, BlockNumber)>, Status> {
    if let [path] = descriptor.path.as_slice() {
        return Ok((path == RAW).then_some((None, BlockNumber::MAX)));
    }
    let Some(range) = descriptor
        .cmd
        .strip_prefix(RAW.as_bytes())
        .and_then(|rest| rest.strip_prefix(b":"))
    else {
        return Ok(None);
    };
    let invalid = || Status::invalid_argument("a raw ticket is `raw:from:to`");
    let range = std::str::from_utf8(range).map_err(|_not_text| invalid())?;
    let (from, to) = range.split_once(':').ok_or_else(invalid)?;
    let (from, to) = (
        from.parse::<BlockNumber>().map_err(|_number| invalid())?,
        to.parse::<BlockNumber>().map_err(|_number| invalid())?,
    );
    if to < from {
        return Err(Status::invalid_argument("`to` is below `from`"));
    }
    Ok(Some((Some(from), to)))
}

/// The table name of raw chunk plans.
const RAW: &str = "raw";

fn unimplemented<T>() -> Result<T, Status> {
    Err(Status::unimplemented(
        "the balancer only plans: GetFlightInfo and ListFlights; DoGet each endpoint at the \
         servers it names",
    ))
}

#[tonic::async_trait]
impl FlightService for Flight {
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
        // Every table covers the same blocks: the jobs are planned once.
        let whole = Query::whole(ticket::Table::Blocks);
        let jobs = self.jobs(whole)?;
        let infos: Vec<_> = ticket::Table::ALL
            .into_iter()
            .map(|table| {
                let descriptor = FlightDescriptor::new_path(vec![table.name().to_owned()]);
                Self::info(table, whole.cap, &jobs, descriptor)
            })
            .collect();
        Ok(Response::new(Box::pin(tokio_stream::iter(infos))))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        if let Some(range) = raw_range(&descriptor)? {
            return self.raw(range, descriptor).await.map(Response::new);
        }
        let query = Query::try_from(&descriptor)?;
        let jobs = self.jobs(query)?;
        Self::info(query.table, query.cap, &jobs, descriptor).map(Response::new)
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        unimplemented()
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        unimplemented()
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        unimplemented()
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
