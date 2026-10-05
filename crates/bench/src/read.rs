//! Runs bounded jobs with fallback locations and finite retries. Blocking workers decode and
//! validate one batch at a time; dropping a failed stream cancels its RPC before releasing a slot.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use arrow_array::{Array as _, RecordBatch, UInt64Array};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_schema::DataType;
use futures_util::{StreamExt as _, TryStreamExt as _};
use op_indexer_api::ticket::{Query, Table};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

use crate::plan::{Job, Server};
use crate::{Auth, BenchError, Config, JobReport, Progress};

/// Polling free fallback slots is local, and never sends an RPC without a permit.
const SLOT_POLL: Duration = Duration::from_millis(20);
const BACKOFF_FIRST: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct Context {
    pub(crate) config: Config,
    pub(crate) auth: Arc<Auth>,
    pub(crate) progress: Arc<Progress>,
    pub(crate) cancel: CancellationToken,
    pub(crate) began: Instant,
}

#[derive(Debug, thiserror::Error)]
enum ReadError {
    #[error("flight read failed: {0}")]
    Flight(#[from] FlightError),
    #[error("arrow byte accounting failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("invalid streamed data: {0}")]
    Invalid(&'static str),
    #[error("read deadline exceeded")]
    Timeout,
    #[error("cancelled")]
    Cancelled,
}

impl ReadError {
    fn code(&self) -> tonic::Code {
        match self {
            Self::Flight(FlightError::Tonic(status)) => status.code(),
            Self::Timeout => tonic::Code::DeadlineExceeded,
            Self::Cancelled => tonic::Code::Cancelled,
            Self::Flight(_) | Self::Arrow(_) | Self::Invalid(_) => tonic::Code::DataLoss,
        }
    }
}

pub(crate) async fn job(job: Job, context: Context) -> Result<JobReport, BenchError> {
    let mut result = JobReport::new(&job, context.began.elapsed());
    let job = Arc::new(job);
    let deadline = Instant::now() + context.config.retry_for;
    let mut attempts = 0_u64;
    let mut backoff = BACKOFF_FIRST;
    let mut last_error = None;
    'retry: loop {
        if context.cancel.is_cancelled() {
            result.error = Some("cancelled".to_owned());
            break;
        }
        if Instant::now() >= deadline {
            result.error = Some(format!(
                "job budget expired; last failure: {}",
                last_error
                    .as_deref()
                    .unwrap_or("waiting for local server slots")
            ));
            break;
        }
        let mut exhausted = false;
        let mut attempted = false;
        for server in &job.servers {
            if context.cancel.is_cancelled() || Instant::now() >= deadline {
                break;
            }
            let Ok(permit) = Arc::clone(&server.permits).try_acquire_owned() else {
                continue;
            };
            attempted = true;
            attempts += 1;
            result.retries = attempts - 1;
            result.server = Some(server.url.clone());
            let attempt_deadline = deadline.min(Instant::now() + context.config.rpc_timeout);
            let worker_context = context.clone();
            let worker_job = Arc::clone(&job);
            let worker_server = Arc::clone(server);
            let runtime = tokio::runtime::Handle::current();
            // Await even on cancellation: the decoder observes cancellation/deadlines, and
            // its RPC must be gone before this permit is released or this job is drained.
            let worker = tokio::task::spawn_blocking(move || {
                runtime.block_on(attempt(
                    &worker_job,
                    &worker_server,
                    &worker_context,
                    result,
                    attempt_deadline,
                ))
            });
            let (updated, outcome) = worker.await.map_err(BenchError::Worker)?;
            result = updated;
            drop(permit);
            match outcome {
                Ok(()) => break 'retry,
                Err(error) => {
                    let code = error.code();
                    let message = error.to_string();
                    if !record_failure(code, &mut result, context.cancel.is_cancelled()) {
                        result.error = Some(message);
                        break 'retry;
                    }
                    exhausted |= code == tonic::Code::ResourceExhausted;
                    last_error = Some(message);
                }
            }
        }
        let pause = if exhausted || attempted {
            let jitter = backoff.mul_f64(0.5 + fastrand::f64() * 0.5);
            backoff = (backoff * 2).min(BACKOFF_MAX);
            jitter
        } else {
            SLOT_POLL
        };
        let waiting = Instant::now();
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => {},
            () = sleep(pause.min(deadline.saturating_duration_since(Instant::now()))) => {},
        }
        result.wait_seconds += waiting.elapsed().as_secs_f64();
    }
    result.latency_seconds = context.began.elapsed().as_secs_f64();
    Ok(result)
}

fn record_failure(code: tonic::Code, result: &mut JobReport, cancelled: bool) -> bool {
    match code {
        tonic::Code::ResourceExhausted => result.failures.exhausted += 1,
        tonic::Code::Unavailable => result.failures.unavailable += 1,
        tonic::Code::DeadlineExceeded => result.failures.timeout += 1,
        tonic::Code::Ok
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::InvalidArgument
        | tonic::Code::NotFound
        | tonic::Code::AlreadyExists
        | tonic::Code::PermissionDenied
        | tonic::Code::FailedPrecondition
        | tonic::Code::Aborted
        | tonic::Code::OutOfRange
        | tonic::Code::Unimplemented
        | tonic::Code::Internal
        | tonic::Code::DataLoss
        | tonic::Code::Unauthenticated => {
            if !cancelled {
                result.failures.other += 1;
            }
            return false;
        }
    }
    true
}

async fn attempt(
    job: &Job,
    server: &Server,
    context: &Context,
    mut result: JobReport,
    deadline: Instant,
) -> (JobReport, Result<(), ReadError>) {
    let started = Instant::now();
    let before = result.received;
    let outcome = {
        let read = read(job, server, context, &mut result, deadline);
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => Err(ReadError::Cancelled),
            timed = tokio::time::timeout_at(deadline, read) => timed.unwrap_or(Err(ReadError::Timeout)),
        }
    };
    if outcome.is_err() {
        let failed = result.received - before;
        result.failed_bytes += failed;
        context
            .progress
            .failed_bytes
            .fetch_add(failed, Ordering::Relaxed);
        result.rows = 0;
        result.bytes = 0;
    } else {
        result.read_seconds = started.elapsed().as_secs_f64();
        result.bytes = result.received - before;
    }
    (result, outcome)
}

async fn read(
    job: &Job,
    server: &Server,
    context: &Context,
    result: &mut JobReport,
    deadline: Instant,
) -> Result<(), ReadError> {
    let mut request = tonic::Request::new(job.ticket.clone());
    request
        .metadata_mut()
        .insert("authorization", context.auth.0.clone());
    request.metadata_mut().insert(
        "op-indexer-compression",
        tonic::metadata::MetadataValue::from_static(context.config.compression.name()),
    );
    request.set_timeout(deadline.saturating_duration_since(Instant::now()));
    let stream = server
        .client
        .clone()
        .do_get(request)
        .await
        .map_err(FlightError::from)?
        .into_inner();
    let mut batches =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::from));
    let expected_schema = job.query.table.schema();
    let mut previous_block = None;
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        if batch.schema().fields() != expected_schema.fields() {
            return Err(ReadError::Invalid("unexpected table schema"));
        }
        if result.ttfb_seconds.is_none() {
            result.ttfb_seconds = Some(context.began.elapsed().as_secs_f64());
        }
        let bytes = decoded_bytes(&batch)?;
        result.received = result.received.saturating_add(bytes);
        context
            .progress
            .received
            .fetch_add(bytes, Ordering::Relaxed);
        validate(&batch, job.query, &mut previous_block)?;
        result.rows = result.rows.saturating_add(
            u64::try_from(batch.num_rows())
                .map_err(|_invalid| ReadError::Invalid("row count exceeds u64"))?,
        );
        // Always check here as well: a ready stream must not starve timeout/cancellation.
        if context.cancel.is_cancelled() {
            return Err(ReadError::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ReadError::Timeout);
        }
        tokio::task::yield_now().await;
    }
    if batches
        .schema()
        .is_none_or(|schema| schema.fields() != expected_schema.fields())
    {
        return Err(ReadError::Invalid(
            "stream ended without the expected schema",
        ));
    }
    if job.query.table == Table::Blocks && previous_block != Some(job.query.to) {
        return Err(ReadError::Invalid(
            "blocks stream ended before requested last block",
        ));
    }
    Ok(())
}

/// Flat table schemas use `PyArrow`'s logical byte convention: values, validity and one offset
/// per variable-width row, excluding the final sentinel offset and allocator padding.
fn decoded_bytes(batch: &RecordBatch) -> Result<u64, ReadError> {
    let mut total = 0_u64;
    for column in batch.columns() {
        let data = column.to_data();
        let bytes = data.get_slice_memory_size()?;
        let sentinel = if matches!(column.data_type(), DataType::Binary | DataType::Utf8) {
            4
        } else {
            0
        };
        let bytes = u64::try_from(bytes.saturating_sub(sentinel))
            .map_err(|_invalid| ReadError::Invalid("decoded byte count exceeds u64"))?;
        total = total
            .checked_add(bytes)
            .ok_or(ReadError::Invalid("decoded byte count overflow"))?;
    }
    Ok(total)
}

fn validate(
    batch: &RecordBatch,
    query: Query,
    previous: &mut Option<u64>,
) -> Result<(), ReadError> {
    let column = batch
        .columns()
        .first()
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or(ReadError::Invalid("missing block number column"))?;
    for number in column {
        let number = number.ok_or(ReadError::Invalid("null block number"))?;
        if Some(number) < query.from
            || number > query.to
            || previous.is_some_and(|last| number < last)
        {
            return Err(ReadError::Invalid(
                "block number outside ticket or out of order",
            ));
        }
        if query.table == Table::Blocks {
            let expected = match previous {
                Some(last) => last.checked_add(1),
                None => query.from,
            };
            if expected != Some(number) {
                return Err(ReadError::Invalid("missing or duplicate block"));
            }
        }
        *previous = Some(number);
    }
    Ok(())
}
