//! belt: distribute CI and heavy commands across your own machines.

pub mod cli;
pub mod config;
pub mod coordinator;
pub mod error;
pub mod forecast;
pub mod ids;
pub mod outputs;
pub mod platform;
pub mod protocol;
pub mod results;
pub mod scheduler;
pub mod secrets;
pub mod service;
pub mod setup;
pub mod source;
pub mod state;
pub mod storage;
pub mod supervisor;
pub mod toolchain;
pub mod wire;
pub mod worker;
pub mod workspace;

pub mod ci;

// The test-only helpers name this crate the way the integration tests do.
#[cfg(test)]
extern crate self as belt;

#[cfg(test)]
mod test_child;
#[cfg(test)]
mod test_tmp;
