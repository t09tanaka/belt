//! Where task-traffic-controller keeps its files. The library never looks at `HOME` or the
//! XDG variables itself: whoever builds the [`crate::Context`] decides the
//! state directory, so tests and embedders get their own scope.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

/// The state directory, the config file and the history file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    state_dir: PathBuf,
    config_file: PathBuf,
    history_file: PathBuf,
}

impl Paths {
    /// State under `state_dir` (`ledger.json`, `leases/`, `jobs/`,
    /// `history.jsonl`), configuration read from `config_file`.
    pub fn new(state_dir: impl Into<PathBuf>, config_file: impl Into<PathBuf>) -> Self {
        let state_dir = state_dir.into();
        let history_file = state_dir.join("history.jsonl");
        Paths {
            state_dir,
            config_file: config_file.into(),
            history_file,
        }
    }

    /// The scope the `task-traffic-controller` command uses, decided from the
    /// given environment values (pass what the process would see):
    ///
    /// - state: `$XDG_STATE_HOME/task-traffic-controller`, else
    ///   `$HOME/.local/state/task-traffic-controller`;
    /// - config: `$XDG_CONFIG_HOME/task-traffic-controller/config.toml`, else
    ///   `$HOME/.config/task-traffic-controller/config.toml`.
    ///
    /// An empty XDG value counts as unset; a relative value is used as given;
    /// without `HOME` the fallback root is `/`.
    pub fn from_env(
        home: Option<&OsStr>,
        xdg_state_home: Option<&OsStr>,
        xdg_config_home: Option<&OsStr>,
    ) -> Self {
        let home = home
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/"));
        let pick = |xdg: Option<&OsStr>, fallback: &str| {
            xdg.filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(fallback))
        };
        Paths::new(
            pick(xdg_state_home, ".local/state").join("task-traffic-controller"),
            pick(xdg_config_home, ".config").join("task-traffic-controller/config.toml"),
        )
    }

    /// [`Paths::from_env`] with `HOME`, `XDG_STATE_HOME` and
    /// `XDG_CONFIG_HOME` read from this process's environment. This is the
    /// only function of the library that reads them.
    pub fn from_process_env() -> Self {
        Paths::from_env(
            std::env::var_os("HOME").as_deref(),
            std::env::var_os("XDG_STATE_HOME").as_deref(),
            std::env::var_os("XDG_CONFIG_HOME").as_deref(),
        )
    }

    /// Keep the history somewhere else than `<state_dir>/history.jsonl`.
    pub fn with_history_file(mut self, history_file: impl Into<PathBuf>) -> Self {
        self.history_file = history_file.into();
        self
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn config_file(&self) -> &Path {
        &self.config_file
    }

    pub fn history_file(&self) -> &Path {
        &self.history_file
    }

    pub fn ledger_file(&self) -> PathBuf {
        self.state_dir.join("ledger.json")
    }

    pub fn ledger_lock(&self) -> PathBuf {
        self.state_dir.join("ledger.lock")
    }

    pub fn leases_dir(&self) -> PathBuf {
        self.state_dir.join("leases")
    }

    pub fn lease_file(&self, id: &str) -> PathBuf {
        self.leases_dir().join(format!("{id}.lock"))
    }

    /// Sidecars of managed commands: `managed/<command_id>.json`. Not part
    /// of the ledger; their presence is what makes [`crate::ledger::Ledger`]
    /// save the shape an older binary refuses to load.
    pub fn managed_dir(&self) -> PathBuf {
        self.state_dir.join("managed")
    }

    pub fn managed_file(&self, id: &str) -> PathBuf {
        self.managed_dir().join(format!("{id}.json"))
    }

    /// Markers for measurements already applied to the history.
    pub fn measurements_dir(&self) -> PathBuf {
        self.state_dir.join("measurements")
    }

    pub fn history_lock(&self) -> PathBuf {
        self.state_dir.join("history.lock")
    }

    /// When the retention sweep last ran ([`crate::retention`]).
    pub fn sweep_stamp(&self) -> PathBuf {
        self.state_dir.join("last-sweep")
    }

    pub fn jobs_dir(&self) -> PathBuf {
        self.state_dir.join("jobs")
    }

    pub fn job_json(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(format!("{id}.json"))
    }

    pub fn job_log(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(format!("{id}.log"))
    }

    pub fn job_waiter(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(format!("{id}.waiter"))
    }

    pub fn job_exit(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(format!("{id}.exit"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn everything_hangs_off_the_given_state_dir() {
        let p = Paths::new("/s", "/c/config.toml");
        assert_eq!(p.ledger_file(), Path::new("/s/ledger.json"));
        assert_eq!(p.lease_file("a"), Path::new("/s/leases/a.lock"));
        assert_eq!(p.job_exit("j"), Path::new("/s/jobs/j.exit"));
        assert_eq!(p.history_file(), Path::new("/s/history.jsonl"));
        assert_eq!(p.managed_file("c"), Path::new("/s/managed/c.json"));
        assert_eq!(p.measurements_dir(), Path::new("/s/measurements"));
        assert_eq!(p.history_lock(), Path::new("/s/history.lock"));
        assert_eq!(p.config_file(), Path::new("/c/config.toml"));
    }

    #[test]
    fn history_can_live_elsewhere() {
        let p = Paths::new("/s", "/c").with_history_file("/h/h.jsonl");
        assert_eq!(p.history_file(), Path::new("/h/h.jsonl"));
        assert_eq!(p.ledger_file(), Path::new("/s/ledger.json"));
    }

    fn os(s: &str) -> Option<&OsStr> {
        Some(OsStr::new(s))
    }

    #[test]
    fn from_env_prefers_the_xdg_homes() {
        let p = Paths::from_env(os("/h"), os("/xs"), os("/xc"));
        assert_eq!(p.state_dir(), Path::new("/xs/task-traffic-controller"));
        assert_eq!(
            p.config_file(),
            Path::new("/xc/task-traffic-controller/config.toml")
        );
    }

    #[test]
    fn from_env_falls_back_to_home() {
        let p = Paths::from_env(os("/h"), None, None);
        assert_eq!(
            p.state_dir(),
            Path::new("/h/.local/state/task-traffic-controller")
        );
        assert_eq!(
            p.config_file(),
            Path::new("/h/.config/task-traffic-controller/config.toml")
        );
    }

    #[test]
    fn from_env_treats_empty_xdg_as_unset_and_keeps_relative() {
        let p = Paths::from_env(os("/h"), os(""), os("rel"));
        assert_eq!(
            p.state_dir(),
            Path::new("/h/.local/state/task-traffic-controller")
        );
        assert_eq!(
            p.config_file(),
            Path::new("rel/task-traffic-controller/config.toml")
        );
    }

    #[test]
    fn from_env_without_home_roots_at_slash() {
        let p = Paths::from_env(None, None, None);
        assert_eq!(
            p.state_dir(),
            Path::new("/.local/state/task-traffic-controller")
        );
        assert_eq!(
            p.config_file(),
            Path::new("/.config/task-traffic-controller/config.toml")
        );
    }

    #[test]
    fn from_process_env_matches_from_env_of_the_same_values() {
        let want = Paths::from_env(
            std::env::var_os("HOME").as_deref(),
            std::env::var_os("XDG_STATE_HOME").as_deref(),
            std::env::var_os("XDG_CONFIG_HOME").as_deref(),
        );
        assert_eq!(Paths::from_process_env(), want);
    }
}
