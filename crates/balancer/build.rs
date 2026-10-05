//! Compiles `proto/opindexer/balancer/v1/balancer.proto` into the server and client code, with
//! `protox` as the protobuf compiler: no system `protoc` is needed.

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    const PROTO: &str = "proto/opindexer/balancer/v1/balancer.proto";
    println!("cargo:rerun-if-changed={PROTO}");
    let descriptors = protox::compile([PROTO], ["proto"])?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
