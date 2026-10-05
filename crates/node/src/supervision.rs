//! Owns task completion and staged shutdown: producers, pipeline drain, then persistence.

use std::future::Future;

use eyre::WrapErr;
use op_indexer_el::ExecutionNetwork;
use op_indexer_l1::{L1Network, LightClient};
use op_indexer_p2p::Network;
use op_indexer_pipeline::Pipeline;
use op_indexer_storage::unsafe_store::MemoryStore;
use op_indexer_stream::StreamServer;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::peers::PeerSources;
use crate::provider::NodeProvider;
use crate::{Archive, NodeView, Task, shutdown_signal};

/// Tasks with the same shutdown stage; names travel with errors and clean early exits.
#[derive(Debug, Default)]
pub(crate) struct Tasks {
    running: JoinSet<eyre::Result<&'static str>>,
}

impl Tasks {
    pub(crate) fn spawn(
        &mut self,
        name: &'static str,
        task: impl Future<Output = eyre::Result<()>> + Send + 'static,
    ) {
        self.running.spawn(async move {
            task.await.wrap_err_with(|| format!("{name} failed"))?;
            Ok(name)
        });
    }

    /// Cancel safe: only removes the task once its completion has been observed.
    async fn next(&mut self) -> eyre::Result<&'static str> {
        match self.running.join_next().await {
            Some(ended) => ended.wrap_err("node task panicked")?,
            None => std::future::pending().await,
        }
    }

    async fn drain(&mut self) -> eyre::Result<()> {
        let mut result = Ok(());
        while let Some(ended) = self.running.join_next().await {
            let ended = ended
                .wrap_err("node task panicked")
                .and_then(|result| result);
            result = result.and(ended.map(|_| ()));
        }
        result
    }
}

/// Adds a channel-driven follower, which must stay alive until its producers stop.
pub(crate) fn follow(
    followers: &mut Tasks,
    name: &'static str,
    task: impl Future<Output = ()> + Send + 'static,
) {
    followers.spawn(name, async move {
        task.await;
        Ok(())
    });
}

/// Stops producers first, then lets the pipeline drain and persistence consume final updates.
pub(crate) async fn run_components<A: Archive>(
    network: Network,
    execution: Option<ExecutionNetwork<NodeProvider<A>>>,
    l1: Option<(L1Network, LightClient)>,
    (pipeline, stream): (Pipeline<MemoryStore, A>, StreamServer<MemoryStore, A>),
    (mut followers, tasks, mut view): (Tasks, Vec<(&'static str, Task)>, NodeView),
    saves: Vec<(&'static str, JoinHandle<()>)>,
) -> eyre::Result<()> {
    let cancel = CancellationToken::new();
    let networks_cancel = cancel.child_token();
    view.peers = PeerSources::new(&network, execution.as_ref(), l1.as_ref());
    let mut producers = Tasks::default();
    let mut extra = Tasks::default();
    for (name, task) in tasks {
        extra.spawn(name, task(view.clone(), networks_cancel.clone()));
    }
    let token = networks_cancel.clone();
    producers.spawn(
        "gossip network",
        async move { Ok(network.run(token).await?) },
    );
    if let Some(execution) = execution {
        let token = networks_cancel.clone();
        producers.spawn("execution network", async move {
            Ok(execution.run(token).await?)
        });
    }
    if let Some((l1, light_client)) = l1 {
        let token = networks_cancel.clone();
        producers.spawn("L1 network", async move { Ok(l1.run(token).await?) });
        let token = networks_cancel.clone();
        producers.spawn("beacon light client", async move {
            Ok(light_client.run(token).await?)
        });
    }
    // Stream consumers stop with producers, but cannot hold up the pipeline's drain signal.
    let mut consumers = Tasks::default();
    let token = networks_cancel.clone();
    consumers.spawn("stream server", async move { Ok(stream.run(token).await?) });
    let pipeline_stop = cancel.clone();
    let token = networks_cancel.clone();
    consumers.spawn("pipeline", async move {
        Ok(pipeline.run(token, pipeline_stop).await?)
    });
    let mut persistence = Tasks::default();
    for (name, save) in saves {
        persistence.spawn(
            name,
            async move { save.await.wrap_err("state saver panicked") },
        );
    }

    let stopped = tokio::select! {
        signal = shutdown_signal() => signal.map(|signal| info!(signal, "shutting down")),
        result = producers.next() => unexpected(result),
        result = consumers.next() => unexpected(result),
        result = extra.next() => unexpected(result),
        result = followers.next() => unexpected(result),
        result = persistence.next() => unexpected(result),
    };
    networks_cancel.cancel();
    let producers = producers.drain().await;
    cancel.cancel();
    let consumers = consumers.drain().await;
    let extra = extra.drain().await;
    // These tasks terminate when the networks and pipeline drop their channel ends.
    let followers = followers.drain().await;
    let persistence = persistence.drain().await;
    stopped
        .and(producers)
        .and(consumers)
        .and(extra)
        .and(followers)
        .and(persistence)
}

fn unexpected(result: eyre::Result<&'static str>) -> eyre::Result<()> {
    let name = result?;
    Err(eyre::eyre!("{name} ended before shutdown"))
}
