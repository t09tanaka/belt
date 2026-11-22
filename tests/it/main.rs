//! All integration tests in one binary: one link step instead of one per file.
//! Each former test file is a module; `common` and `proto_common` are shared
//! helpers.

mod common;
mod proto_common;
#[path = "../../src/test_tmp.rs"]
mod test_tmp;

mod bridge;
mod collection;
mod config;
mod coordinator;
mod direct_exec;
mod doctor;
mod forecast;
mod not_implemented;
mod one_host;
mod protocol;
mod scheduler;
mod secrets;
mod setup;
mod source_capture;
mod stop;
mod supervisor;
mod toolchain_preparation;
mod worker_agent;
mod worker_blobs;
mod workspace;

mod runtime;

mod resident;

mod bootstrap;

mod init;

mod worker_ephemeral;
mod worker_role;

mod remote_links;

mod test_tmp_root;

mod workers_lifecycle;
mod workers_upgrade;

mod retry_dependents;
mod top;

mod group_join;

mod group_rotate;

mod fixture_process;
