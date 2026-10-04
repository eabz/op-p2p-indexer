//! The gRPC service: `Subscribe` starts a subscription task, `GetHeads` and `GetBlock` read
//! the stores.

use std::sync::Arc;

use alloy_primitives::B256;
use op_indexer_storage::{ArchiveStore, UnsafeStore};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Request, Response, Status};

use crate::convert::{Payload, heads_message};
use crate::follower::Live;
use crate::proto;
use crate::source::{Source, read_status};
use crate::subscription::{Item, Sink, Start, Subscription};

/// Events queued per subscription before the server waits for the consumer: HTTP/2 flow
/// control does the rest.
const SUBSCRIPTION_QUEUE: usize = 64;

/// The service's shared parts.
#[derive(Debug)]
pub(crate) struct Service<U, A> {
    pub(crate) source: Source<U, A>,
    pub(crate) live: Arc<Live>,
    /// One permit per subscription.
    pub(crate) subscriptions: Arc<Semaphore>,
    pub(crate) tasks: TaskTracker,
    pub(crate) cancel: CancellationToken,
    pub(crate) receipts: bool,
}

#[tonic::async_trait]
impl<U, A> proto::stream_server::Stream for Service<U, A>
where
    U: UnsafeStore + Clone + Send + Sync + 'static,
    A: ArchiveStore + Clone + Send + Sync + 'static,
{
    type SubscribeStream = ReceiverStream<Item>;

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        let payload = Payload::from(request.payload());
        let start = match request.start {
            Some(proto::subscribe_request::Start::FromNumber(number)) => Start::Number(number),
            Some(proto::subscribe_request::Start::FromHead(true)) => Start::Head,
            Some(proto::subscribe_request::Start::FromHead(false)) | None => {
                return Err(Status::invalid_argument(
                    "give `from_number` or `from_head`",
                ));
            }
        };
        if let Start::Number(number) = start
            && let Some((first, _)) = self
                .source
                .archive_range()
                .await
                .map_err(|err| read_status(&err))?
            && number < first.number
        {
            return Err(Status::out_of_range(format!(
                "the node holds blocks from {} on",
                first.number
            )));
        }
        let permit = Arc::clone(&self.subscriptions)
            .try_acquire_owned()
            .map_err(|_full| Status::resource_exhausted("too many subscriptions"))?;
        // One slot more than the queue: the one the subscription's ending status takes.
        let (tx, rx) = mpsc::channel(SUBSCRIPTION_QUEUE.saturating_add(1));
        let sink = Sink::new(tx)
            .await
            .ok_or_else(|| Status::internal("the subscription could not start"))?;
        let subscription = Subscription::new(
            self.source.clone(),
            Arc::clone(&self.live),
            payload,
            sink,
            permit,
            self.receipts,
        );
        self.tasks
            .spawn(subscription.run(start, self.cancel.child_token()));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_heads(
        &self,
        _request: Request<proto::GetHeadsRequest>,
    ) -> Result<Response<proto::Heads>, Status> {
        let (unsafe_head, heads) = self.source.heads().await.map_err(|err| read_status(&err))?;
        Ok(Response::new(heads_message(
            unsafe_head,
            heads,
            self.receipts,
        )))
    }

    async fn get_block(
        &self,
        request: Request<proto::GetBlockRequest>,
    ) -> Result<Response<proto::Block>, Status> {
        let request = request.into_inner();
        let payload = Payload::from(request.payload());
        let block = match request.block {
            Some(proto::get_block_request::Block::Number(number)) => {
                self.source.block_at(number).await
            }
            Some(proto::get_block_request::Block::Hash(hash)) => {
                let hash = B256::try_from(hash.as_ref())
                    .map_err(|_length| Status::invalid_argument("a hash is 32 bytes"))?;
                self.source.block_by_hash(hash).await
            }
            None => return Err(Status::invalid_argument("give `number` or `hash`")),
        }
        .map_err(|err| read_status(&err))?
        .ok_or_else(|| Status::not_found("the node does not hold this block"))?;
        let (_, heads) = self.source.heads().await.map_err(|err| read_status(&err))?;
        tokio::task::spawn_blocking(move || block.message(payload, &heads))
            .await
            .map_err(|_failed| Status::internal("the node failed to convert the block"))?
            .map(Response::new)
            .map_err(|err| read_status(&err.into()))
    }
}
