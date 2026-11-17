//! Resident processes and authenticated local command composition.
pub mod bootstrap;
pub mod ci_run;
pub mod completion;
pub mod context;
pub mod control;
pub mod doctor;
pub mod estimate;
mod forward;
pub mod github_status;
pub mod input;
pub mod logs;
pub mod macos;
pub mod output_export;
pub mod outputs;
pub mod query;
pub mod resident;
pub mod submit;

pub mod init;

/// A worker waits here while its commands still run in another scope.
mod traffic_hold;
/// Standalone persistent worker role, independent of SSH bridge lifetime.
pub mod worker;

/// Worker-host install and lifecycle helper (macOS).
pub mod worker_service;
/// Parent-side `workers` commands.
pub mod workers;

pub mod planning;
