# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.99
ARG DEBIAN_RELEASE=trixie

# ---- Chef: toolchain + cargo-chef --------------------------------------------
FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS chef

# No extra build packages: the native builds in the indexer's dependency graph (`ring`, `blst`,
# `secp256k1-sys`) use the image's cc.
WORKDIR /app

# ---- Planner: compute the dependency recipe ----------------------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---- Builder: build dependencies (cached layer), then the app ----------------
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Rebuilt only when Cargo.toml / Cargo.lock change, not on source edits.
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY . .
RUN cargo build --release --locked --bin indexer

# ---- Runtime ----------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin indexer \
    && install -d -o indexer -g indexer /data

COPY --from=builder /app/target/release/indexer /usr/local/bin/indexer

USER indexer

# Node state (`node/`: the node identity and known peers; `archive/`: the block archive). Mount a volume here to keep the
# peer id across container recreation.
ENV OP_INDEXER_DATA_DIR=/data
VOLUME /data

# The gRPC stream listens on every interface inside the container; publish the port only
# where consumers may reach it: the stream has no authentication.
ENV OP_INDEXER_STREAM_LISTEN_ADDR=0.0.0.0:50051

# OP Stack p2p (libp2p TCP + discv5 UDP) on 9222; the chain's execution p2p (RLPx TCP + discv5
# UDP) on 30303, used only when OP_INDEXER_EL_ENABLED=true; L1's execution p2p on 30304 and
# the beacon light client on 9001, used only when OP_INDEXER_L1_ENABLED=true; the gRPC stream
# on 50051.
EXPOSE 9222/tcp 9222/udp 30303/tcp 30303/udp 30304/tcp 30304/udp 9001/tcp 9001/udp 50051/tcp

ENTRYPOINT ["/usr/local/bin/indexer"]
