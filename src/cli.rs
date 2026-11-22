//! The `fast-worktree` command line: argument parsing and dispatch to the
//! commands. `main` only calls [`run`]; a program that embeds the command
//! calls [`run_with_helper`].

use crate::util::{self, errln};
use crate::{cmd, pool};
use clap::{Parser, Subcommand};
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "fast-worktree",
    version,
    about = "Create git worktrees with their dependencies in seconds, remove them in one"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a worktree on a new branch and install its dependencies
    Add {
        /// Where to create the worktree
        path: PathBuf,
        /// The new branch
        #[arg(short = 'b', long = "branch", value_name = "BRANCH")]
        branch: String,
        /// Commit to start from (default: origin's default branch, fetched)
        start_point: Option<String>,
        /// Seconds the fetch of origin's default branch may take; after
        /// that, or when it fails, the branch starts from it as last fetched
        #[arg(
            long,
            value_name = "SECS",
            default_value_t = 30,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        fetch_timeout: u64,
    },
    /// Install dependencies into an existing worktree
    Install {
        /// Worktree to install into (default: the current one)
        #[arg(long, value_name = "DIR")]
        root: Option<PathBuf>,
        /// Also install again where the lockfile or Node version changed
        /// since the last install
        #[arg(long)]
        changed: bool,
        /// Only these projects: a directory, `root` for the one at the
        /// root, or the last part of a directory when only one ends so
        projects: Vec<String>,
        /// Run as the background install of `hook create`
        #[arg(long, hide = true)]
        deferred: bool,
    },
    /// List projects whose dependencies are missing (always exits 0)
    Check {
        /// Print nothing when nothing is missing
        #[arg(long)]
        quiet: bool,
        /// Run as a post-checkout hook, given the hook's three arguments
        #[arg(
            long,
            num_args = 3,
            value_names = ["PREV", "NEW", "FLAG"],
            allow_hyphen_values = true
        )]
        post_checkout: Option<Vec<String>>,
    },
    /// Remove a clean worktree, deleting its files in the background
    Remove {
        path: PathBuf,
        /// Also delete its branch, when it is merged into its upstream,
        /// origin's default branch or the local default branch
        #[arg(long)]
        delete_branch: bool,
    },
    /// Inspect or clean the dependency cache
    Cache {
        #[command(subcommand)]
        cmd: CacheCmd,
    },
    /// Ready worktrees that `add` takes instead of creating one
    Pool {
        #[command(subcommand)]
        cmd: PoolCmd,
    },
    /// Entry points for Claude Code's WorktreeCreate / WorktreeRemove hooks
    Hook {
        #[command(subcommand)]
        cmd: HookCmd,
    },
    /// List or prune the commits `hook remove` kept in refs/fast-worktree/kept/
    Kept {
        #[command(subcommand)]
        cmd: KeptCmd,
    },
    /// Wait for the background install that `hook create` started
    Wait {
        /// The worktree (default: the current one)
        path: Option<PathBuf>,
    },
    /// Write or check the [[clone]] entries of .fast-worktree.toml
    Config {
        #[command(subcommand)]
        cmd: ConfigCmd,
    },
    /// Show where each target's files would come from. Installs nothing,
    /// but runs the key_commands and verify
    Plan {
        /// An existing worktree (default: a new one with the files of the
        /// current worktree)
        path: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ConfigCmd {
    /// Add a [[clone]] for each tracked lockfile that has none
    Init {
        /// Print what would be added, without writing the file
        #[arg(long)]
        dry_run: bool,
    },
    /// Exit 1 when a tracked lockfile is in no [[clone]], a [[clone]]
    /// names a file that does not exist, or the file has a mistake
    Check {
        /// Check what is staged (the index), not the files on disk: for a
        /// pre-commit hook
        #[arg(long)]
        staged: bool,
    },
}

#[derive(Subcommand)]
enum PoolCmd {
    /// List the spares: commit, how far behind origin, when made, misses
    Status,
    /// Make spares until the pool has its size
    Fill {
        /// First wait for the background install of this worktree, if one
        /// is running (for the refill that `add` and `hook create` start)
        #[arg(long, hide = true, value_name = "WORKTREE")]
        after: Option<PathBuf>,
    },
    /// Drop the spares behind origin's default branch and fill again
    Refresh,
    /// Remove every spare
    Clear,
}

#[derive(Subcommand)]
enum HookCmd {
    /// WorktreeCreate: read {"name", "cwd"} on stdin, print the worktree path
    Create,
    /// WorktreeRemove: read {"worktree_path"} on stdin, delete the worktree
    Remove,
    /// Print the hooks to paste into a Claude Code settings file
    PrintConfig,
}

#[derive(Subcommand)]
enum KeptCmd {
    /// List the kept refs with their commit and when they were kept
    Ls,
    /// Delete the kept refs older than a number of days
    Prune {
        /// Days since they were kept (default: 30)
        #[arg(long, value_name = "DAYS")]
        older_than: Option<u64>,
    },
}

#[derive(Subcommand)]
enum CacheCmd {
    /// List cache entries with their repository, directory, key and last use
    Ls,
    /// Remove entries not used for a while (moved to the trash)
    Prune {
        /// Days without use (default: 7)
        #[arg(long, value_name = "DAYS")]
        older_than: Option<u64>,
        /// Instead, remove the directories of an older layout that `ls`
        /// lists under LEGACY
        #[arg(long, conflicts_with = "older_than")]
        legacy: bool,
    },
}

/// Parse the command line, run the command and return its exit code.
///
/// The background helpers (`pool fill`, `install --changed --deferred`)
/// re-exec the running binary. This is [`run_with_helper`] with the
/// process's arguments and no helper.
pub fn run() -> i32 {
    run_with_helper(std::env::args_os(), None)
}

/// Run the command line `args` and return its exit code, for a program
/// that embeds the `fast-worktree` command.
///
/// `args` starts with the program name, as `std::env::args_os()` does.
/// Usage errors, `--help` and `--version` are printed as the command
/// prints them and return their exit code (2, 0, 0) instead of exiting.
///
/// `helper` is the executable that every background helper this call starts
/// re-execs: `pool fill` after `add` and `hook create`, and `install
/// --changed --deferred`. It must behave as the `fast-worktree` command:
/// for example a link named `fast-worktree` to the embedding binary, whose
/// `main` sees that name and calls this function again with the same
/// helper. `None` re-execs [`std::env::current_exe`], as [`run`] does. On
/// Linux that resolves links to the real binary, so an embedding binary
/// started through a link should pass the link here.
///
/// Unlike [`crate::Repository::with_helper`], `None` does not turn the
/// helpers off.
///
/// ```no_run
/// use std::path::PathBuf;
///
/// let code = fast_worktree::cli::run_with_helper(
///     ["fast-worktree", "pool", "status"],
///     Some(PathBuf::from("/usr/local/bin/fast-worktree")),
/// );
/// std::process::exit(code);
/// ```
pub fn run_with_helper<I, T>(args: I, helper: Option<PathBuf>) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            return e.exit_code();
        }
    };
    match helper {
        // Only the helper: output stays on stdout and stderr.
        Some(exe) => util::with_context(None, Some(exe), || dispatch(cli)),
        None => dispatch(cli),
    }
}

fn dispatch(cli: Cli) -> i32 {
    let result = match cli.cmd {
        Cmd::Add {
            path,
            branch,
            start_point,
            fetch_timeout,
        } => cmd::add::run(cmd::add::Opts {
            path,
            branch,
            start_point,
            fetch_limit: std::time::Duration::from_secs(fetch_timeout),
        }),
        Cmd::Install {
            root,
            changed,
            projects,
            deferred,
        } => cmd::install::run(cmd::install::Opts {
            root,
            projects,
            changed,
            deferred,
        }),
        Cmd::Check {
            quiet,
            post_checkout,
        } => Ok(match post_checkout.as_deref() {
            Some([prev, new, flag]) => cmd::check::post_checkout(prev, new, flag),
            _ => cmd::check::run(quiet),
        }),
        Cmd::Remove {
            path,
            delete_branch,
        } => cmd::remove::run(cmd::remove::Opts {
            path,
            delete_branch,
        }),
        Cmd::Cache { cmd } => Ok(match cmd {
            CacheCmd::Ls => cmd::cache::ls(),
            CacheCmd::Prune { legacy: true, .. } => cmd::cache::prune_legacy(),
            CacheCmd::Prune { older_than, .. } => cmd::cache::prune(older_than),
        }),
        Cmd::Pool { cmd } => match cmd {
            PoolCmd::Status => pool::status(),
            PoolCmd::Fill { after } => pool::fill(after.as_deref()),
            PoolCmd::Refresh => pool::refresh(),
            PoolCmd::Clear => pool::clear(),
        },
        Cmd::Hook { cmd } => Ok(match cmd {
            HookCmd::Create => cmd::hook::create(),
            HookCmd::Remove => cmd::hook::remove(),
            HookCmd::PrintConfig => cmd::hook::print_config(),
        }),
        Cmd::Kept { cmd } => match cmd {
            KeptCmd::Ls => cmd::kept::ls(),
            KeptCmd::Prune { older_than } => cmd::kept::prune(older_than),
        },
        Cmd::Wait { path } => cmd::hook::wait(path.as_deref()),
        Cmd::Config { cmd } => match cmd {
            ConfigCmd::Init { dry_run } => cmd::config::init(dry_run),
            ConfigCmd::Check { staged } => cmd::config::check(staged),
        },
        Cmd::Plan { path } => cmd::plan::run(path.as_deref()),
    };
    match result {
        Ok(c) => c,
        Err(e) => {
            errln!("fast-worktree: {e}");
            1
        }
    }
}
