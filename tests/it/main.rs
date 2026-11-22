//! All integration tests in one binary: one link step instead of one per file.
//! Each former test file is a module; `common` and `proto_common` are shared
//! helpers.

mod common;
mod proto_common;

/// Runs before `main`: the stop signals are at their default for this process
/// and so for every child a test starts. A test binary started from a
/// background job of a non-interactive shell (or under `nohup`) inherits them
/// as ignored, and belt keeps a signal that was ignored at start ignored, so
/// the tests that send SIGINT or SIGHUP to a belt they spawned would hang.
/// A test about an inherited ignore sets `SIG_IGN` itself in `pre_exec`.
extern "C" fn default_stop_signals() {
    for sig in [libc::SIGINT, libc::SIGQUIT, libc::SIGTERM, libc::SIGHUP] {
        // SAFETY: signal() with SIG_DFL, before any thread exists.
        unsafe { libc::signal(sig, libc::SIG_DFL) };
    }
}

#[used]
#[cfg_attr(target_vendor = "apple", link_section = "__DATA,__mod_init_func")]
#[cfg_attr(not(target_vendor = "apple"), link_section = ".init_array")]
static DEFAULT_STOP_SIGNALS: extern "C" fn() = default_stop_signals;
#[path = "../../src/test_tmp.rs"]
mod test_tmp;

mod bridge;
mod collection;
mod config;
mod coordinator;
mod direct_exec;
mod doctor;
mod forecast;
mod gaps_cli;
mod gaps_worker;
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

mod status_repost;

mod init;

mod ephemeral_stop;
mod warmup_stop;
mod worker_ephemeral;
mod worker_role;

mod remote_links;

mod test_tmp_root;

mod workers_lifecycle;
mod workers_listing;
mod workers_upgrade;

mod retry_dependents;
mod top;

mod group_join;

mod group_rotate;

mod fixture_process;
