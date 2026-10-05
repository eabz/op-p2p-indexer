//! Shared wire contracts and authentication for servers and their directory.
mod auth;
mod schema;
pub mod ticket;
pub use auth::ApiKeys;
