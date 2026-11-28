//! The `fast-worktree` command: a thin adapter over the library.

fn main() {
    std::process::exit(fast_worktree::cli::run());
}
