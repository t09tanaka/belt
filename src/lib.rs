//! fast-worktree: create a git worktree with its dependencies in seconds,
//! and remove it in one.
//!
//! This crate is the engine behind the `fast-worktree` command, for programs
//! that create and remove worktrees themselves (a task runner that gives
//! every attempt its own checkout, for example). It does what the command
//! does, with the same pool of ready worktrees, dependency cache, hooks,
//! trash and kept refs, in the same places and formats, so a program that
//! embeds it and the `fast-worktree` command can work on the same repository
//! and the same state directories at the same time.
//!
//! The entry point is [`Repository`]:
//!
//! ```no_run
//! use fast_worktree::{Checkout, CreateOptions, RemoveMode, RemoveOptions, Repository, Start};
//!
//! # fn main() -> Result<(), fast_worktree::Error> {
//! let repo = Repository::open("/work/app")?;
//!
//! // A detached worktree at a commit, dependencies installed or cloned
//! // from the cache, taken from the pool when a spare is ready.
//! let made = repo.create_worktree(&CreateOptions::new(
//!     "/work/attempts/42",
//!     Checkout::Detached(Start::Rev("3f2c1ab".into())),
//! ))?;
//! assert!(made.setup_ok);
//!
//! // ...run the attempt in `made.path`...
//!
//! // Remove it at once; the files are deleted in the background.
//! repo.remove_worktree(
//!     &made.path,
//!     &RemoveOptions::new(RemoveMode::Discard),
//! )?;
//! # Ok(()) }
//! ```
//!
//! # Where state lives
//!
//! Exactly where the command keeps it: the cache, the pool and the trash
//! under `FAST_WORKTREE_CACHE` (default `~/Library/Caches/fast-worktree` on
//! macOS, else `$XDG_CACHE_HOME/fast-worktree`), the user settings in
//! `$XDG_CONFIG_HOME/fast-worktree/config.toml`, and the per-repository
//! settings in `.fast-worktree.toml` and `.worktreeinclude`. These
//! environment variables are read when needed, as the command reads them.
//!
//! # Output
//!
//! The command prints progress. A library call sends the same lines to
//! wherever [`Output`] says: the terminal ([`Output::Inherit`], what the
//! command does), nowhere, or into the `log` of the result.
//!
//! # Background work
//!
//! After a worktree is created, the command starts `fast-worktree pool
//! fill` in the background to replace the spare it took. A library cannot
//! re-run its host program for that, so it runs the `fast-worktree`
//! executable: found on `PATH` or in `~/.cargo/bin` by [`Repository::open`],
//! or set with [`Repository::with_helper`]. This separate binary may be a
//! different version. Embedders should pin it with `with_helper(Some(path))`
//! or disable it with `with_helper(None)` and call [`Repository::fill_pool`]
//! themselves. Without a helper, spares are not
//! refilled automatically; [`Repository::fill_pool`] fills them in the
//! calling thread.
//!
//! A program that embeds the whole command (for example, one binary that
//! acts as `fast-worktree` when started through a link of that name) runs
//! it with `cli::run_with_helper(args, Some(path))`, so that the
//! background work re-execs `path` and not whatever
//! [`std::env::current_exe`] resolves to.
//!
//! # Process-wide effects
//!
//! Removal is refused when the process cwd is inside the worktree; the
//! library does not change cwd. Relative paths passed to
//! [`Repository::create_worktree`] and [`Repository::remove_worktree`] resolve
//! against the process cwd; pass absolute paths.
//! [`Repository::open`] and [`sweep_trash`] set a process-global embedded flag
//! that is never reset, even if opening fails or all repositories are dropped.
//! Output is directed per calling thread, so concurrent calls from different
//! threads do not mix their logs.
//!
//! # Features
//!
//! The `cli` feature (on by default) builds the command-line parser and the
//! `fast-worktree` binary. Programs that only embed the library can leave it
//! out with `default-features = false`.

#![warn(missing_docs)]
// Without the command line, what only it calls is unused.
#![cfg_attr(not(feature = "cli"), allow(dead_code))]

mod cache;
#[cfg(feature = "cli")]
pub mod cli;
mod clone;
mod cmd;
mod ecosystem;
mod error;
mod git;
mod hooks;
mod include;
mod paths;
mod pending;
mod pool;
mod retention;
mod submodules;
mod trash;
mod util;

use std::cell::RefCell;
use std::fmt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use crate::cmd::remove::Refusal;
pub use crate::hooks::{CloneEntry, Config, Hook, Ignore, PoolConfig, SkipLifecycle};
pub use crate::pool::Source as PoolSizeSource;

/// The result of a library call.
pub type Result<T> = std::result::Result<T, Error>;

/// Why a library call failed. The message is what the command prints after
/// `fast-worktree: `.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A removal was refused before anything changed.
    Refused {
        /// The condition that refused it.
        reason: Refusal,
        /// The full message.
        message: String,
    },
    /// A removal with `delete_branch` found the branch unmerged; nothing was
    /// removed.
    BranchNotMerged {
        /// The full message.
        message: String,
    },
    /// `.fast-worktree.toml` or `.worktreeinclude` cannot be used.
    Config {
        /// The full message.
        message: String,
    },
    /// `git` failed or could not be run.
    Git {
        /// The full message, with git's own error.
        message: String,
    },
    /// A file or directory operation failed.
    Io {
        /// The full message.
        message: String,
    },
    /// Any other failure.
    Failed {
        /// The full message.
        message: String,
    },
}

impl Error {
    /// The message, as the command would print it.
    pub fn message(&self) -> &str {
        match self {
            Error::Refused { message, .. }
            | Error::BranchNotMerged { message }
            | Error::Config { message }
            | Error::Git { message }
            | Error::Io { message }
            | Error::Failed { message } => message,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for Error {}

impl From<error::Error> for Error {
    fn from(e: error::Error) -> Self {
        use error::Kind;
        let message = e.msg;
        match e.kind {
            Kind::Refused(reason) => Error::Refused { reason, message },
            Kind::BranchNotMerged => Error::BranchNotMerged { message },
            Kind::Config => Error::Config { message },
            Kind::Git => Error::Git { message },
            Kind::Io => Error::Io { message },
            Kind::Other => Error::Failed { message },
        }
    }
}

/// Where the progress lines of a call go.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum Output {
    /// The terminal: stdout and stderr, as the command does, and what hooks
    /// and installs write goes there directly.
    #[default]
    Inherit,
    /// Nowhere. What hooks and installs write is dropped too.
    Discard,
    /// Into the `log` of the result, in order.
    Capture,
}

/// Which stream a [`LogEntry`] was written to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LogStream {
    /// What the command prints to stdout.
    Stdout,
    /// What the command prints to stderr: warnings and failures.
    Stderr,
}

/// A piece of output of a call run with [`Output::Capture`]: a progress
/// line, or the whole output of a hook or an install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The stream it was written to.
    pub stream: LogStream,
    /// The text, with its newlines.
    pub text: String,
}

/// A git repository, by its main checkout. All operations hang off it.
#[derive(Debug, Clone)]
pub struct Repository {
    main: PathBuf,
    output: Output,
    helper: Option<PathBuf>,
}

/// Where a new worktree starts.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Start {
    /// A commit or ref: a SHA, a branch, a tag, `origin/main`.
    Rev(String),
    /// Origin's default branch as it is now: fetched first, for at most
    /// `fetch_timeout`; when the fetch fails or times out, as last fetched.
    OriginDefault {
        /// How long the fetch may take.
        fetch_timeout: Duration,
    },
}

/// What a new worktree has checked out.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Checkout {
    /// A new branch `name` at `start`.
    NewBranch {
        /// The branch to create.
        name: String,
        /// Where it starts.
        start: Start,
    },
    /// An existing local branch, as it is.
    Branch(String),
    /// A detached `HEAD` at `start`: no branch is created, so nothing is left
    /// behind to clean up.
    Detached(Start),
}

/// What [`Repository::create_worktree`] needs to know.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CreateOptions {
    /// Where to create the worktree. It must not exist. A relative path is
    /// relative to the current directory.
    pub path: PathBuf,
    /// What it checks out.
    pub checkout: Checkout,
}

impl CreateOptions {
    /// Options for a worktree at `path`.
    pub fn new(path: impl Into<PathBuf>, checkout: Checkout) -> Self {
        CreateOptions {
            path: path.into(),
            checkout,
        }
    }
}

/// A worktree that [`Repository::create_worktree`] made.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Created {
    /// The worktree's real path.
    pub path: PathBuf,
    /// It was a spare taken from the pool, not made from scratch.
    pub from_pool: bool,
    /// Everything after the worktree existed succeeded: the
    /// `.worktreeinclude` files, the dependencies, the `post_add` hooks. When
    /// `false` the worktree exists all the same and the failure is in the
    /// log (`fast-worktree install` in it tries again).
    pub setup_ok: bool,
    /// The output of the call, with [`Output::Capture`].
    pub log: Vec<LogEntry>,
}

/// How a worktree is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RemoveMode {
    /// As `fast-worktree remove`: refuses a worktree with uncommitted or
    /// untracked changes, a locked one, one with submodules and one with
    /// another worktree inside ([`Error::Refused`]). With `delete_branch`,
    /// the branch is deleted too when it is merged, and the removal is
    /// refused ([`Error::BranchNotMerged`]) when it is not.
    Strict {
        /// Also delete the worktree's branch.
        delete_branch: bool,
    },
    /// As `fast-worktree hook remove`: removes the worktree whatever its
    /// state. One with changes, submodules or another worktree inside is
    /// kept in the trash for a day, and its `HEAD` (and each submodule's) in
    /// `refs/fast-worktree/kept/`. Still refuses a locked worktree, the main
    /// checkout, a spare of the pool and a path that cannot be moved. A
    /// path that is already gone only has its registration dropped.
    Discard,
}

/// What [`Repository::remove_worktree`] needs to know.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RemoveOptions {
    /// How to remove.
    pub mode: RemoveMode,
}

impl RemoveOptions {
    /// Options for `mode`.
    pub fn new(mode: RemoveMode) -> Self {
        RemoveOptions { mode }
    }
}

/// A removal that [`Repository::remove_worktree`] did.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Removed {
    /// Where the worktree's files went, being deleted in the background (or,
    /// when [`kept`](Removed::kept), waiting a day). `None` when the
    /// directory was already gone.
    pub trash: Option<PathBuf>,
    /// The files are kept for a day and the commits in kept refs
    /// ([`RemoveMode::Discard`] on a worktree with something to keep).
    pub kept: bool,
    /// The branch was deleted ([`RemoveMode::Strict`] with `delete_branch`).
    pub branch_deleted: bool,
    /// The worktree is gone, and so is everything that was asked for. When
    /// `false` something after the move did not work (git still lists the
    /// entry, the branch was kept); the log says what.
    pub complete: bool,
    /// The output of the call, with [`Output::Capture`].
    pub log: Vec<LogEntry>,
}

/// The settings that apply to a repository.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EffectiveConfig {
    /// The main checkout's `.fast-worktree.toml`, when there is one.
    pub file: Option<PathBuf>,
    /// What it says; empty without a file. A new worktree reads the file at
    /// its own commit instead, for everything except the pool.
    pub config: Config,
    /// How many spares the pool keeps: `[pool] size` in the file, else in the
    /// user's settings, else 0 (off).
    pub pool_size: u32,
    /// Where `pool_size` comes from.
    pub pool_size_source: PoolSizeSource,
    /// The patterns of the main checkout's `.worktreeinclude`, in order
    /// (comments and blank lines left out); empty without the file. A file
    /// is copied to a new worktree when a pattern matches it and git ignores
    /// it.
    pub include: Vec<String>,
    /// The `.worktreeinclude` file, when there is one.
    pub include_file: Option<PathBuf>,
    /// The output of the call, with [`Output::Capture`] (warnings about
    /// unknown keys, for one).
    pub log: Vec<LogEntry>,
}

/// A ref under `refs/fast-worktree/kept/`: a commit that a removal kept.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeptRef {
    /// The full ref name.
    pub name: String,
    /// The commit it points to.
    pub commit: String,
    /// When it was kept, read from the ref's name; `None` for a ref whose
    /// name has no time (those are never pruned).
    pub kept_at: Option<SystemTime>,
}

impl Repository {
    /// The repository that contains `dir`: a main checkout or any linked
    /// worktree of it. The helper executable is looked up here (see the
    /// crate docs). Sets a process-global embedded flag that is never reset,
    /// even if opening fails or this repository is dropped.
    pub fn open(dir: impl AsRef<Path>) -> Result<Repository> {
        util::set_embedded();
        let top = git::toplevel(dir.as_ref())?;
        let main = cmd::install::main_checkout(&top)?;
        Ok(Repository {
            main,
            output: Output::Inherit,
            helper: util::find_tool("fast-worktree"),
        })
    }

    /// Where the progress lines go (default [`Output::Inherit`]).
    pub fn with_output(mut self, output: Output) -> Self {
        self.output = output;
        self
    }

    /// The `fast-worktree` executable that runs background work (`pool
    /// fill`); `None` for no background work. By default, a separate binary
    /// is found on PATH or in `~/.cargo/bin`, and may be a different version.
    /// Embedders should pin it with `with_helper(Some(path))`, or disable it
    /// with `with_helper(None)` and call [`Self::fill_pool`] themselves.
    pub fn with_helper(mut self, exe: Option<PathBuf>) -> Self {
        self.helper = exe;
        self
    }

    /// The real path of the main checkout.
    pub fn main_checkout(&self) -> &Path {
        &self.main
    }

    /// Run `f` with this repository's output and helper in place; returns
    /// its value and the captured log.
    fn call<T>(&self, f: impl FnOnce() -> T) -> (T, Vec<LogEntry>) {
        let log = Rc::new(RefCell::new(Vec::new()));
        let sink: Option<util::Sink> = match self.output {
            Output::Inherit => None,
            Output::Discard => Some(Box::new(|_, _| {})),
            Output::Capture => {
                let log = Rc::clone(&log);
                Some(Box::new(move |stream, text| {
                    log.borrow_mut().push(LogEntry {
                        stream: match stream {
                            util::Stream::Stdout => LogStream::Stdout,
                            util::Stream::Stderr => LogStream::Stderr,
                        },
                        text: text.to_string(),
                    });
                }))
            }
        };
        let value = util::with_context(sink, self.helper.clone(), f);
        let log = log.take();
        (value, log)
    }

    /// Create a worktree, as `fast-worktree add` does: a spare from the pool
    /// when one is ready, else `git worktree add`; then the
    /// `.worktreeinclude` files; then the dependencies, cloned from the
    /// cache where an entry fits and installed where not; then the
    /// `post_add` hooks. Fails only when the worktree itself cannot be
    /// created; see [`Created::setup_ok`] for the rest.
    ///
    /// Relative paths resolve against the process cwd; pass absolute paths.
    pub fn create_worktree(&self, opts: &CreateOptions) -> Result<Created> {
        let (made, log) = self.call(|| -> error::Result<cmd::add::Created> {
            let branch = match &opts.checkout {
                Checkout::NewBranch { name, start } => cmd::add::Branch::New {
                    name: name.clone(),
                    start: self.resolve(start)?,
                },
                Checkout::Branch(name) => cmd::add::Branch::Existing(name.clone()),
                Checkout::Detached(start) => cmd::add::Branch::Detached {
                    start: self.resolve(start)?,
                },
            };
            cmd::add::create(&self.main, &opts.path, &branch, false)
        });
        let made = made?;
        Ok(Created {
            path: made.root,
            from_pool: made.from_pool,
            setup_ok: made.ok,
            log,
        })
    }

    fn resolve(&self, start: &Start) -> error::Result<String> {
        match start {
            Start::Rev(r) => Ok(r.clone()),
            Start::OriginDefault { fetch_timeout } => {
                cmd::add::fetched_start(&self.main, *fetch_timeout)
            }
        }
    }

    /// Remove the worktree at `path`, a linked worktree of this repository:
    /// its files are renamed into the trash at once and deleted in the
    /// background, after the `pre_remove` hooks ran and the cacheable
    /// directories were given to the cache. See [`RemoveMode`] for what is
    /// refused and what is kept.
    ///
    /// Relative paths resolve against the process cwd; pass absolute paths.
    /// Returns [`Error::Refused`] if the process cwd is inside the worktree,
    /// before running removal hooks or changing the worktree. The library
    /// does not change the process cwd.
    pub fn remove_worktree(&self, path: impl AsRef<Path>, opts: &RemoveOptions) -> Result<Removed> {
        let path = path.as_ref();
        let (done, log) = self.call(|| -> error::Result<Removed> {
            if let Ok(real) = path.canonicalize() {
                let of = cmd::install::main_checkout(&real).map_err(|e| {
                    error::Error::msg(format!("{}: not a git worktree ({e})", real.display()))
                })?;
                if of != self.main {
                    error::bail!(
                        "{} is a worktree of {}, not of {}",
                        real.display(),
                        of.display(),
                        self.main.display()
                    );
                }
            }
            Ok(match opts.mode {
                RemoveMode::Strict { delete_branch } => {
                    let d = cmd::remove::execute(cmd::remove::Opts {
                        path: path.to_path_buf(),
                        delete_branch,
                    })?;
                    Removed {
                        trash: Some(d.trash),
                        kept: false,
                        branch_deleted: d.branch_deleted,
                        complete: d.code == 0,
                        log: Vec::new(),
                    }
                }
                RemoveMode::Discard => {
                    let d = cmd::hook::discard(path, Some(&self.main))?;
                    Removed {
                        trash: d.trash,
                        kept: d.kept,
                        branch_deleted: false,
                        complete: d.complete,
                        log: Vec::new(),
                    }
                }
            })
        });
        let mut removed = done?;
        removed.log = log;
        Ok(removed)
    }

    /// The settings that apply to this repository, read from its main
    /// checkout: `.fast-worktree.toml`, the pool size (with the user's
    /// settings as the fallback) and `.worktreeinclude`.
    pub fn config(&self) -> Result<EffectiveConfig> {
        let (got, log) = self.call(|| -> error::Result<_> {
            let file = self.main.join(hooks::FILE);
            let config = hooks::Config::load(&self.main)?;
            let (pool_size, pool_size_source) = pool::size(&self.main);
            let include_path = self.main.join(include::FILE);
            let include = match std::fs::read_to_string(&include_path) {
                Ok(text) => Some(text),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    return Err(error::Error::of(
                        error::Kind::Config,
                        format!("{}: {e}", include_path.display()),
                    ))
                }
            };
            Ok((
                file,
                config,
                pool_size,
                pool_size_source,
                include_path,
                include,
            ))
        });
        let (file, config, pool_size, pool_size_source, include_path, include) = got?;
        Ok(EffectiveConfig {
            file: file.exists().then_some(file),
            config,
            pool_size,
            pool_size_source,
            include: include
                .as_deref()
                .map(|t| {
                    t.lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty() && !l.starts_with('#'))
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default(),
            include_file: include.is_some().then_some(include_path),
            log,
        })
    }

    /// Make spares until the pool has its configured size, as `fast-worktree
    /// pool fill` does, in the calling thread; returns at once when another
    /// fill is running or the pool is off. Spares take a while to make:
    /// call this from a background thread or a maintenance job. This runs
    /// synchronously. With [`Output::Capture`] or [`Output::Discard`], child
    /// output is buffered.
    pub fn fill_pool(&self) -> Result<()> {
        let (code, _log) = self.call(|| pool::fill_for(&self.main, None));
        match code? {
            0 => Ok(()),
            _ => Err(Error::Failed {
                message: "the pool could not be filled completely; see the fill log in the pool directory"
                    .into(),
            }),
        }
    }

    /// The commits that removals kept in `refs/fast-worktree/kept/`.
    pub fn kept_refs(&self) -> Result<Vec<KeptRef>> {
        let (refs, _log) = self.call(|| cmd::kept::kept_refs(&self.main));
        Ok(refs?
            .into_iter()
            .map(|(name, commit)| KeptRef {
                kept_at: cmd::kept::kept_time(&name).map(|t| UNIX_EPOCH + Duration::from_secs(t)),
                name,
                commit,
            })
            .collect())
    }

    /// Delete the kept refs that are `older_than_days` days old or older
    /// (the command's default is 30). Returns how many were deleted.
    pub fn prune_kept_refs(&self, older_than_days: u64) -> Result<usize> {
        let (done, _log) = self.call(|| cmd::kept::prune_in(&self.main, older_than_days));
        let (pruned, failed) = done?;
        if failed > 0 {
            return Err(Error::Git {
                message: format!("{failed} kept ref(s) could not be deleted"),
            });
        }
        Ok(pruned)
    }
}

/// Finish deleting what earlier removals left in the trash, and expire what
/// was kept for a day and is due, as every removal does before it moves its
/// own worktree; and, at most once an hour, remove from the dependency
/// cache what is unused for 7 days or half made by a killed run, in every
/// project of every repository, and the whole cache directory of a
/// repository whose main checkout is gone. Cheap when there is nothing to
/// do; it never waits for a repository's lock and runs no git. Sets a
/// process-global embedded flag that is never reset.
///
/// [`Repository::create_worktree`], [`Repository::remove_worktree`] and
/// [`Repository::fill_pool`] make this sweep themselves, and, for their
/// repository, also discard the spares beyond the pool size and, at most
/// once an hour, delete its kept refs that are no longer needed (one that
/// held its commit alone, after 30 days, is named in the call's log) and
/// the leftovers in its pool. So a program that calls them need call
/// nothing else; call this from a maintenance job only when it may go long
/// without any of them. A repository's own kept refs and pool are only
/// ever cleaned by the calls made for that repository.
pub fn sweep_trash() {
    util::set_embedded();
    retention::run(None);
}
