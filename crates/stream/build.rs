//! Compiles `proto/opindexer/v1/stream.proto` into the server code, with `protox` as the
//! protobuf compiler: no system `protoc` is needed.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    const PROTO: &str = "proto/opindexer/v1/stream.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["proto"])?;
    // `bytes` fields as `Bytes`, so the archive's bytes are sent without a copy.
    tonic_prost_build::configure()
        .bytes(".")
        .build_client(false)
        .compile_fds(descriptors)?;
    Ok(())
}
