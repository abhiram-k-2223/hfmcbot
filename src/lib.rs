//! hfmcbot — SOL meme "new-launch spray + let winners run" bot.
//!
//! Library crate so integration tests and (later) the live binary share the
//! same engine code paths.

pub mod alerts;
pub mod config;
pub mod decode;
pub mod engine;
pub mod exec;
pub mod ingest;
pub mod keys;
pub mod live;
pub mod liveloop;
pub mod metrics;
pub mod persist;
#[cfg(feature = "pg")]
pub mod pg_audit;
pub mod risk;
pub mod strategy;
pub mod types;
pub mod wsfeed;
