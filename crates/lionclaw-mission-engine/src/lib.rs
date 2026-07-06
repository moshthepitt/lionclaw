//! LionClaw mission engine: a mission = an objective → a contract of
//! falsifiable assertions → a team of runtime-backed roles → a drive loop →
//! a verified finish.
//!
//! Thin deterministic spine (event-sourced store, pure fold, moat predicate)
//! plus fat dynamic body (plugins = directories of prose the engine loads as
//! data). The loop decision logic is ported from Zenith (Apache-2.0,
//! Intelligent Internet) — see LICENSE-zenith.

pub mod engine;
pub mod model;
pub mod plugin;
pub mod ports;
pub mod prompt;
pub mod store;

#[cfg(any(test, feature = "testing"))]
pub mod testing;
