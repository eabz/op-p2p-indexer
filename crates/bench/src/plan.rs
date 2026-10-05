//! Validates complete, nonoverlapping tickets and shares one admission limit per server.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightDescriptor, Ticket};
use op_indexer_api::ticket::Query;
use tokio::sync::{Mutex, Semaphore};
use tonic::transport::{Channel, Endpoint};

use crate::{Auth, BenchError, Config};

/// Bounds plan bookkeeping even when a balancer returns many tiny tickets.
const MAX_JOBS: usize = 100_000;

#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) url: String,
    endpoint: Endpoint,
    max_message_bytes: usize,
    idle: Mutex<Vec<FlightServiceClient<Channel>>>,
    pub(crate) permits: Arc<Semaphore>,
}

impl Server {
    /// Called only under a server permit: one exclusive reusable connection per active
    /// read, allocated lazily so a large cap/plan does not eagerly allocate a large pool.
    pub(crate) async fn checkout(&self) -> FlightServiceClient<Channel> {
        self.idle.lock().await.pop().unwrap_or_else(|| {
            FlightServiceClient::new(self.endpoint.connect_lazy())
                .max_decoding_message_size(self.max_message_bytes)
        })
    }

    /// The reader has been dropped before this connection is made available again.
    pub(crate) async fn checkin(&self, client: FlightServiceClient<Channel>) {
        self.idle.lock().await.push(client);
    }
}

#[derive(Debug)]
pub(crate) struct Job {
    pub(crate) index: usize,
    pub(crate) query: Query,
    pub(crate) ticket: Ticket,
    pub(crate) servers: Vec<Arc<Server>>,
}

pub(crate) fn url(value: &str) -> Result<String, BenchError> {
    let address = value
        .strip_prefix("grpc+tcp://")
        .or_else(|| value.strip_prefix("grpc://"))
        .or_else(|| value.strip_prefix("http://"))
        .ok_or(BenchError::Config(
            "use grpc://, grpc+tcp:// or http:// for a plaintext Flight endpoint",
        ))?;
    // Endpoint aliases must share a permit pool. Reject paths, userinfo and query strings.
    let address = address.trim_end_matches('/');
    if address.is_empty() || address.contains(['/', '?', '#', '@']) {
        return Err(BenchError::Config(
            "Flight endpoints must be host:port without paths or credentials",
        ));
    }
    let normalized = format!("http://{}", address.to_ascii_lowercase());
    Endpoint::from_shared(normalized.clone())
        .map_err(|_invalid| BenchError::Config("invalid Flight endpoint"))?;
    Ok(normalized)
}

fn endpoint(url: String, config: &Config) -> Result<Endpoint, BenchError> {
    Ok(Endpoint::from_shared(url)
        .map_err(|_invalid| BenchError::Config("invalid Flight endpoint"))?
        .connect_timeout(config.plan_timeout.min(config.rpc_timeout))
        .http2_adaptive_window(true))
}

pub(crate) async fn plan(config: &Config, auth: &Auth) -> Result<Vec<Job>, BenchError> {
    let mut client =
        FlightServiceClient::new(endpoint(url(&config.balancer)?, config)?.connect_lazy())
            .max_decoding_message_size(config.max_message_bytes);
    let descriptor = FlightDescriptor::new_cmd(config.query.ticket().ticket);
    let mut request = tonic::Request::new(descriptor);
    request
        .metadata_mut()
        .insert("authorization", auth.0.clone());
    request.set_timeout(config.plan_timeout);
    let info = tokio::time::timeout(config.plan_timeout, client.get_flight_info(request))
        .await
        .map_err(|_invalid| BenchError::PlanTimeout)?
        .map_err(BenchError::Planning)?
        .into_inner();
    if info.endpoint.is_empty() || info.endpoint.len() > MAX_JOBS {
        return Err(BenchError::Plan("empty plan or more than 100000 jobs"));
    }
    let mut servers = BTreeMap::new();
    let mut jobs = Vec::with_capacity(info.endpoint.len());
    for (index, endpoint) in info.endpoint.into_iter().enumerate() {
        let ticket = endpoint.ticket.ok_or(BenchError::Plan("missing ticket"))?;
        let query = Query::parse(&ticket.ticket)
            .map_err(|_invalid| BenchError::Plan("malformed ticket"))?;
        if query.table != config.query.table
            || query.cap != config.query.cap
            || query.from < config.query.from
            || query.to > config.query.to
        {
            return Err(BenchError::Plan(
                "unexpected table, finality or ticket outside requested range",
            ));
        }
        let mut locations = Vec::new();
        for location in endpoint.location {
            let address = url(&location.uri)?;
            if !servers.contains_key(&address) {
                let server = Server {
                    endpoint: self::endpoint(address.clone(), config)?,
                    max_message_bytes: config.max_message_bytes,
                    idle: Mutex::new(Vec::new()),
                    url: address.clone(),
                    permits: Arc::new(Semaphore::new(config.per_server)),
                };
                servers.insert(address.clone(), Arc::new(server));
            }
            if let Some(server) = servers.get(&address)
                && !locations
                    .iter()
                    .any(|other: &Arc<Server>| other.url == address)
            {
                locations.push(Arc::clone(server));
            }
        }
        if locations.is_empty() {
            return Err(BenchError::Plan("ticket has no server"));
        }
        jobs.push(Job {
            index,
            query,
            ticket,
            servers: locations,
        });
    }
    let mut ranges: Vec<_> = jobs
        .iter()
        .map(|job| (job.query.from, job.query.to))
        .collect();
    ranges.sort_unstable();
    let mut next = config.query.from;
    let mut last = None;
    for (from, to) in ranges {
        if from != next || from.is_none() {
            return Err(BenchError::Plan("gap or overlap in tickets"));
        }
        next = to.checked_add(1);
        last = Some(to);
    }
    if last != Some(config.query.to) {
        return Err(BenchError::Plan("plan does not reach requested end"));
    }
    Ok(jobs)
}
