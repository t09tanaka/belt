//! Small helpers shared by the commands.

use std::cell::RefCell;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

pub fn home() -> PathBuf {
    env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"))
}

static EMBEDDED: AtomicBool = AtomicBool::new(false);

/// Mark this process as one that embeds the library: it lives on after a
/// command would have exited, so the detached children it starts must be
/// reaped (see [`release`]).
pub fn set_embedded() {
    EMBEDDED.store(true, Ordering::Relaxed);
}

pub(crate) fn is_embedded() -> bool {
    EMBEDDED.load(Ordering::Relaxed)
}

/// Let go of a detached child that is not waited for. The command exits
/// soon after, and init reaps the child; a long-lived embedding process
/// would keep a zombie for every one, so a thread waits for it there.
pub fn release(child: std::process::Child) {
    if is_embedded() {
        std::thread::spawn(move || {
            let mut child = child;
            let _ = child.wait();
        });
    }
}

/// Which stream a line of output belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

pub type Sink = Box<dyn FnMut(Stream, &str)>;

/// What the library sets for the duration of one call: where the output
/// goes instead of the terminal, and which executable runs the detached
/// helper commands (`pool fill`, `install --changed`). The CLI sets nothing:
/// output goes to stdout and stderr, and the helper is the running binary.
struct Context {
    /// `None`: stdout and stderr, and what a child writes goes there
    /// directly.
    sink: Option<Sink>,
    exe: Option<PathBuf>,
}

thread_local! {
    static CONTEXT: RefCell<Option<Context>> = const { RefCell::new(None) };
}

/// Run `f` with the output of this thread sent to `sink`, and `exe` as the
/// helper executable. Nests; the previous context is restored.
pub fn with_context<T>(sink: Option<Sink>, exe: Option<PathBuf>, f: impl FnOnce() -> T) -> T {
    let prev = CONTEXT.with(|c| c.borrow_mut().replace(Context { sink, exe }));
    struct Restore(Option<Context>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let prev = self.0.take();
            CONTEXT.with(|c| *c.borrow_mut() = prev);
        }
    }
    let _restore = Restore(prev);
    f()
}

fn to_sink(stream: Stream, s: &str) -> bool {
    CONTEXT.with(|c| match c.borrow_mut().as_mut() {
        Some(Context {
            sink: Some(sink), ..
        }) => {
            sink(stream, s);
            true
        }
        _ => false,
    })
}

/// The executable that runs `pool fill` and `install --changed` detached:
/// the running binary, or the one the embedding program configured.
pub fn helper_exe() -> std::io::Result<PathBuf> {
    let configured = CONTEXT.with(|c| c.borrow().as_ref().map(|ctx| ctx.exe.clone()));
    match configured {
        None => env::current_exe(),
        Some(Some(exe)) => Ok(exe),
        Some(None) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no `fast-worktree` executable is configured for background work",
        )),
    }
}

/// `Command::status`, except that while the library captures output, what
/// the command writes goes to the sink too and not to this process's own
/// stdout and stderr.
pub fn run_status(c: &mut std::process::Command) -> std::io::Result<std::process::ExitStatus> {
    if !CONTEXT.with(|ctx| ctx.borrow().as_ref().is_some_and(|c| c.sink.is_some())) {
        return c.status();
    }
    let out = c.output()?;
    to_sink(Stream::Stdout, &String::from_utf8_lossy(&out.stdout));
    to_sink(Stream::Stderr, &String::from_utf8_lossy(&out.stderr));
    Ok(out.status)
}

/// Write to stdout, ignoring errors. `print!` panics when stdout is a pipe
/// whose reader has gone (EPIPE); `check` promises to exit 0 whatever
/// happens to its output.
pub fn say(s: &str) {
    use std::io::Write;
    if to_sink(Stream::Stdout, s) {
        return;
    }
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(s.as_bytes()).and_then(|()| out.flush());
}

/// [`say`] for stderr.
pub fn warn(s: &str) {
    use std::io::Write;
    if to_sink(Stream::Stderr, s) {
        return;
    }
    let _ = std::io::stderr().lock().write_all(s.as_bytes());
}

/// `print!` through [`say`]: a reader that has gone (`fast-worktree pool
/// status | head -1`) ends the output, not the command.
macro_rules! out {
    ($($arg:tt)*) => {
        $crate::util::say(&format!($($arg)*))
    };
}

/// `println!` through [`say`].
macro_rules! outln {
    () => {
        $crate::util::say("\n")
    };
    ($($arg:tt)*) => {
        $crate::util::say(&format!("{}\n", format_args!($($arg)*)))
    };
}

/// `eprintln!` through [`warn`].
macro_rules! errln {
    () => {
        $crate::util::warn("\n")
    };
    ($($arg:tt)*) => {
        $crate::util::warn(&format!("{}\n", format_args!($($arg)*)))
    };
}
pub(crate) use {errln, out, outln};

/// `1.2s`, the format every timing line uses.
pub fn elapsed(start: Instant) -> String {
    format!("{:.1}s", start.elapsed().as_secs_f64())
}

/// Show a path with `$HOME` abbreviated to `~`.
pub fn tilde(path: &Path) -> String {
    tilde_with(path, &home())
}

fn tilde_with(path: &Path, home: &Path) -> String {
    if home != Path::new("/") {
        if let Ok(rest) = path.strip_prefix(home) {
            return if rest.as_os_str().is_empty() {
                "~".to_string()
            } else {
                format!("~/{}", rest.display())
            };
        }
    }
    path.display().to_string()
}

/// Find an executable on `PATH`, then in `~/.cargo/bin` (where `cargo
/// install` puts tools even when that directory is not on `PATH`).
pub fn find_tool(name: &str) -> Option<PathBuf> {
    find_tool_in(name, env::var_os("PATH").as_deref(), &home())
}

fn find_tool_in(name: &str, path: Option<&std::ffi::OsStr>, home: &Path) -> Option<PathBuf> {
    if name.contains('/') {
        let p = PathBuf::from(name);
        return is_executable(&p).then_some(p);
    }
    let on_path = path
        .into_iter()
        .flat_map(env::split_paths)
        .filter(|d| !d.as_os_str().is_empty())
        .map(|d| d.join(name));
    let cargo = std::iter::once(home.join(".cargo/bin").join(name));
    on_path.chain(cargo).find(|p| is_executable(p))
}

fn is_executable(p: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    p.metadata()
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

/// The last `n` lines of `s`.
pub fn tail(s: &str, n: usize) -> String {
    let lines: Vec<&str> = s.lines().collect();
    let start = lines.len().saturating_sub(n);
    lines[start..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn tilde_abbreviates_home_only_as_a_path_prefix() {
        let home = Path::new("/home/me");
        assert_eq!(tilde_with(Path::new("/home/me/a/b"), home), "~/a/b");
        assert_eq!(tilde_with(Path::new("/home/me"), home), "~");
        assert_eq!(tilde_with(Path::new("/home/meow/a"), home), "/home/meow/a");
        assert_eq!(tilde_with(Path::new("/x"), Path::new("/")), "/x");
    }

    #[test]
    fn find_tool_prefers_path_then_cargo_bin() {
        let t = tempfile::tempdir().unwrap();
        let bin = t.path().join("bin");
        let cargo = t.path().join(".cargo/bin");
        fs::create_dir_all(&bin).unwrap();
        fs::create_dir_all(&cargo).unwrap();
        let mk = |p: &Path| {
            fs::write(p, "#!/bin/sh\n").unwrap();
            fs::set_permissions(p, fs::Permissions::from_mode(0o755)).unwrap();
        };
        mk(&cargo.join("tool"));
        let path = std::ffi::OsString::from(bin.as_os_str());
        assert_eq!(
            find_tool_in("tool", Some(&path), t.path()),
            Some(cargo.join("tool"))
        );
        mk(&bin.join("tool"));
        assert_eq!(
            find_tool_in("tool", Some(&path), t.path()),
            Some(bin.join("tool"))
        );
        assert_eq!(find_tool_in("nope", Some(&path), t.path()), None);
        // Not executable: ignored.
        fs::write(bin.join("plain"), "").unwrap();
        assert_eq!(find_tool_in("plain", Some(&path), t.path()), None);
    }

    #[test]
    fn tail_keeps_last_lines() {
        assert_eq!(tail("a\nb\nc", 2), "b\nc");
        assert_eq!(tail("a", 5), "a");
    }
}
