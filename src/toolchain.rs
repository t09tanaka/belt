//! Toolchain declaration and installation.

pub mod adapters;
pub mod capability;
pub mod contract;
pub mod preparation;
pub mod profile;
pub mod registry;

pub mod declarations;
pub mod envdiff;
pub mod envreport;
pub mod envsync;
pub mod snapshot;
pub mod versions;

#[cfg(test)]
mod tests;
