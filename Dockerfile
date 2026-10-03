# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.99
ARG DEBIAN_RELEASE=trixie

# ---- Chef: toolchain + cargo-chef --------------------------------------------
FROM lukemathwalker/cargo-chef:latest-rust-${RUST_VERSION}-slim-${DEBIAN_RELEASE} AS chef

# clang/libclang: bindgen for reth's MDBX bindings. pkg-config: native deps.
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

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
RUN cargo build --release --locked --bin op-p2p-indexer

# ---- Runtime ----------------------------------------------------------------
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin indexer

COPY --from=builder /app/target/release/op-p2p-indexer /usr/local/bin/op-p2p-indexer

USER indexer

# OP Stack p2p (libp2p TCP + discv5 UDP).
EXPOSE 9222/tcp 9222/udp

ENTRYPOINT ["/usr/local/bin/op-p2p-indexer"]
