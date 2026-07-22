//! LionClaw mission engine: a mission = an objective → a contract of
//! falsifiable assertions → a team of runtime-backed roles → a drive loop →
//! a verified finish.
//!
//! Thin deterministic spine (event-sourced store, pure fold, moat predicate)
//! plus fat dynamic body (mission types = directories of prose the engine loads as
//! data). The loop decision logic is ported from Zenith (Apache-2.0,
//! Intelligent Internet) — see LICENSE-zenith.

pub mod authority;
pub mod cli;
pub mod config;
mod driver_lock;
mod effect_cleanup;
pub use effect_cleanup::LocalEffectCleaner;
pub mod engine;
pub mod evidence;
pub mod mission_type;
pub use lionclaw_model as model;
pub mod oracle;
pub mod ports;
pub mod prompt;
pub mod reference_materialization;
mod resources;
pub mod runner;
pub mod selftest;
pub mod store;
pub mod workspace;

pub mod activity;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
