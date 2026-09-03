//! hfmcbot — SOL meme "new-launch spray + let winners run" bot.
//!
//! Library crate so integration tests and (later) the live binary share the
//! same engine code paths.

pub mod config;
pub mod engine;
pub mod exec;
pub mod ingest;
pub mod keys;
pub mod metrics;
pub mod persist;
pub mod risk;
pub mod strategy;
pub mod types;
