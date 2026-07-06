//! LionClaw mission engine: a mission = an objective → a contract of
//! falsifiable assertions → a team of runtime-backed roles → a drive loop →
//! a verified finish.
//!
//! Thin deterministic spine (event-sourced store, pure fold, moat predicate)
//! + fat dynamic body (plugins = directories of prose the engine loads as
//! data). The loop decision logic is ported from Zenith (Apache-2.0,
//! Intelligent Internet) — see LICENSE-zenith.
