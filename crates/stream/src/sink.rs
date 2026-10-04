//! The sending side of a response stream's queue, shared by subscriptions and Flight streams:
//! a consumer that does not read for [`SLOW_CONSUMER_TIMEOUT`] is ended, and one slot is held
//! back for the error a stream ends with, so a full queue can still say why it ends.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendTimeoutError;
use tonic::Status;
use tracing::debug;

/// How long a queue may stay full before its stream is ended.
const SLOW_CONSUMER_TIMEOUT: Duration = Duration::from_secs(30);

/// The consumer left, or was told why its stream ends.
#[derive(Debug)]
pub(crate) struct Ended;

/// A queue of `Result<T, E>`, with its last slot held back for the ending error.
#[derive(Debug)]
pub(crate) struct Sink<T, E> {
    tx: mpsc::Sender<Result<T, E>>,
    last_word: Option<mpsc::OwnedPermit<Result<T, E>>>,
}

impl<T, E: From<Status>> Sink<T, E> {
    /// A sink and the receiver of a queue of `capacity` items (plus the held-back slot).
    pub(crate) fn channel(capacity: usize) -> (Self, mpsc::Receiver<Result<T, E>>) {
        let (tx, rx) = mpsc::channel(capacity.saturating_add(1));
        // A fresh queue has room, and its receiver is alive.
        let last_word = tx.clone().try_reserve_owned().ok();
        (Self { tx, last_word }, rx)
    }

    /// Sends `item`, waiting while the queue is full; a consumer that does not read for
    /// [`SLOW_CONSUMER_TIMEOUT`] is told so and ended.
    pub(crate) async fn send(&mut self, item: T) -> Result<(), Ended> {
        match self.tx.send_timeout(Ok(item), SLOW_CONSUMER_TIMEOUT).await {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Closed(_)) => Err(Ended),
            Err(SendTimeoutError::Timeout(_)) => {
                debug!("a stream consumer stopped reading; its stream ends");
                self.end(E::from(Status::resource_exhausted(format!(
                    "the consumer did not read for {} s; ask again from the last block received",
                    SLOW_CONSUMER_TIMEOUT.as_secs()
                ))));
                Err(Ended)
            }
        }
    }

    /// Ends the stream with `err`, in the held-back slot; only the first call sends.
    pub(crate) fn end(&mut self, err: E) {
        if let Some(permit) = self.last_word.take() {
            permit.send(Err(err));
        }
    }

    /// Completes when the consumer has left. Holds none of the sink, so it can be awaited
    /// while the sink is in use.
    pub(crate) fn closed(&self) -> impl Future<Output = ()> + Send + 'static
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let tx = self.tx.clone();
        async move { tx.closed().await }
    }
}
