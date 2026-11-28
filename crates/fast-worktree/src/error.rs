use std::fmt;

/// What went wrong, for callers that need to tell failures apart (the
/// library's public error is built from this).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Kind {
    #[default]
    Other,
    /// `git` failed or could not run.
    Git,
    /// A file or directory operation failed.
    Io,
    /// `.fast-worktree.toml` or `.worktreeinclude` is unusable.
    Config,
    /// `remove` refused before changing anything.
    Refused(crate::cmd::remove::Refusal),
    /// `remove --delete-branch` found the branch unmerged.
    BranchNotMerged,
}

/// A failure that ends the command. The message is printed after
/// `fast-worktree: ` and the process exits non-zero.
#[derive(Debug)]
pub struct Error {
    pub msg: String,
    pub kind: Kind,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        Error {
            msg: s.into(),
            kind: Kind::Other,
        }
    }

    pub fn of(kind: Kind, s: impl Into<String>) -> Self {
        Error {
            msg: s.into(),
            kind,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::of(Kind::Io, e.to_string())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::msg(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::msg(s)
    }
}

/// Return early with a formatted [`Error`].
macro_rules! bail {
    ($($arg:tt)*) => {
        return Err($crate::error::Error::msg(format!($($arg)*)))
    };
}
pub(crate) use bail;
