//! Pure systemd rendering and command plans. Nothing here touches the host:
//! the unit text, the `systemctl` argument sequences (as data), the strict
//! reading of `systemctl show` output and the environment `systemctl --user`
//! needs are all functions of their inputs.
//!
//! A value that would need escaping beyond systemd's own quoting is refused
//! instead (control characters, NUL). `%` and `$` are live in unit files, so
//! they are escaped where systemd reads them.
use crate::service::macos::policy::{valid_system_account, Definition, Mode, Plan};
use crate::service::macos::LoadedState;
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

const USER: &str = "--user";
const SYSTEM: &str = "--system";
/// The properties `show` is asked for, and the only ones accepted back.
pub const PROPERTIES: &str = "LoadState,ActiveState,FragmentPath,MainPID,NeedDaemonReload";
/// How long a stopping unit may take before systemd escalates; the runner's
/// own deadline for `stop` is larger than this.
pub const STOP_TIMEOUT_SECS: u32 = 60;

fn clean(s: &str) -> Result<&str, String> {
    if s.chars().any(char::is_control) {
        return Err("value contains a control character".into());
    }
    Ok(s)
}

fn absolute(path: &Path) -> Result<&str, String> {
    let s = path.to_str().ok_or("path must be UTF-8")?;
    if !path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
    {
        return Err("path must be absolute without traversal".into());
    }
    clean(s)
}

/// A word of `ExecStart=`: quoted, with `\`, `"`, the specifier `%` and the
/// variable `$` escaped.
fn exec_word(s: &str) -> Result<String, String> {
    let s = clean(s)?;
    Ok(format!(
        "\"{}\"",
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
    ))
}

/// A whole `Environment=` assignment, quoted. `$` is not expanded there.
fn env_assignment(key: &str, value: &str) -> Result<String, String> {
    let value = clean(value)?;
    Ok(format!(
        "\"{key}={}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
    ))
}

/// A path in a directive that is not split into words. Spaces stay legal;
/// the specifier `%` is escaped.
fn directive_path(path: &Path) -> Result<String, String> {
    Ok(absolute(path)?.replace('%', "%%"))
}

pub fn scope(mode: &Mode) -> &'static str {
    match mode {
        Mode::User { .. } => USER,
        Mode::SystemUser { .. } => SYSTEM,
    }
}

pub fn render(def: &Definition) -> Result<Plan, String> {
    if def.label.is_empty()
        || def.label.len() > 128
        || !def
            .label
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
        || !def.label.as_bytes()[0].is_ascii_alphanumeric()
    {
        return Err("invalid service label".into());
    }
    let (user, target) = match &def.mode {
        Mode::User { uid } if *uid > 0 => (String::new(), "default.target"),
        Mode::SystemUser { uid, username } if valid_system_account(*uid, username) => {
            (format!("User={username}\n"), "multi-user.target")
        }
        _ => return Err("service requires a non-root user identity".into()),
    };
    let mut exec = exec_word(absolute(&def.executable)?)?;
    for arg in &def.arguments {
        exec.push(' ');
        exec.push_str(&exec_word(arg)?);
    }
    let mut env = String::new();
    for (key, value) in &def.environment {
        if !matches!(
            key.as_str(),
            "PATH" | "HOME" | "TMPDIR" | "LANG" | "LC_ALL" | "LC_CTYPE"
        ) {
            return Err("only explicit non-secret service environment keys are allowed".into());
        }
        env.push_str(&format!("Environment={}\n", env_assignment(key, value)?));
    }
    let unit = format!(
        "[Unit]\nDescription=remote-task-runner worker\n\n[Service]\nType=simple\nExecStart={exec}\nWorkingDirectory={}\n{env}{user}Restart=always\nRestartSec=5\nTimeoutStopSec={STOP_TIMEOUT_SECS}\n\n[Install]\nWantedBy={target}\n",
        directive_path(&def.working_directory)?
    );
    Ok(Plan::systemd(def, scope(&def.mode), unit))
}

/// The directory a user's units are read from, below the user's config home.
pub fn user_unit_dir(config_home: &Path) -> PathBuf {
    config_home.join("systemd").join("user")
}

fn checked_unit(unit: &str) -> Result<&str, String> {
    let stem = unit.strip_suffix(".service").ok_or("not a service unit")?;
    if stem.is_empty()
        || !stem
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b))
    {
        return Err("invalid unit name".into());
    }
    Ok(unit)
}

/// The commands behind one start or stop, in order. `daemon-reload` first so
/// a replaced unit file is read, then `enable` so the unit follows the next
/// login (as a LaunchAgent with `RunAtLoad` does), then `start`. A stop only
/// stops: the unit stays enabled, as a booted-out agent is loaded again at
/// the next login.
pub fn steps(arguments: &[String]) -> Result<Vec<Vec<String>>, String> {
    let [scope, verb, unit] = arguments else {
        return Err("expected a scope, a verb and a unit".into());
    };
    if scope != USER && scope != SYSTEM {
        return Err("unknown scope".into());
    }
    let unit = checked_unit(unit)?;
    let with = |verb: &str, unit: Option<&str>| {
        let mut words = vec![scope.clone(), verb.to_owned()];
        words.extend(unit.map(str::to_owned));
        words
    };
    match verb.as_str() {
        "start" if scope == SYSTEM => Ok(vec![with("start", Some(unit))]),
        "start" => Ok(vec![
            with("daemon-reload", None),
            with("enable", Some(unit)),
            with("start", Some(unit)),
        ]),
        "stop" => Ok(vec![with("stop", Some(unit))]),
        _ => Err("unknown verb".into()),
    }
}

/// `systemctl` words that read one unit's state.
pub fn show_arguments(scope: &str, unit: &str) -> Vec<String> {
    let properties = properties(scope == SYSTEM);
    vec![
        scope.into(),
        "show".into(),
        unit.into(),
        format!("--property={properties}"),
    ]
}

/// What `systemctl --user` needs to find the user's manager. The runner
/// clears the environment, so it is built here: a given runtime directory
/// (absolute) or the one systemd makes for the user, and the bus beside it.
pub fn systemctl_env(
    runtime_dir: Option<&str>,
    bus: Option<&str>,
    uid: u32,
) -> Vec<(String, String)> {
    let runtime = runtime_dir
        .filter(|d| Path::new(d).is_absolute() && !d.chars().any(char::is_control))
        .map(str::to_owned)
        .unwrap_or_else(|| format!("/run/user/{uid}"));
    let bus = bus
        .filter(|b| !b.is_empty() && !b.chars().any(char::is_control))
        .map(str::to_owned)
        .unwrap_or_else(|| format!("unix:path={runtime}/bus"));
    vec![
        ("XDG_RUNTIME_DIR".into(), runtime),
        ("DBUS_SESSION_BUS_ADDRESS".into(), bus),
    ]
}

/// One reading of `systemctl show`.
#[derive(Debug, PartialEq, Eq)]
pub struct Show {
    system: bool,
    load: String,
    active: String,
    fragment: String,
    main_pid: u32,
    need_reload: bool,
}

/// Strict: exactly the requested properties, once each. Anything else is
/// refused, never guessed at.
pub fn parse_show(bytes: &[u8]) -> Result<Show, &'static str> {
    parse_show_for(bytes, None)
}

fn properties(system: bool) -> String {
    if system {
        format!("{PROPERTIES},User,DropInPaths,UnitFileState")
    } else {
        PROPERTIES.to_owned()
    }
}

/// Confirm the account and persistent activation of a system service in
/// systemd's effective configuration, including any manager-side overrides.
pub fn parse_show_for(bytes: &[u8], username: Option<&str>) -> Result<Show, &'static str> {
    let text = std::str::from_utf8(bytes).map_err(|_| "show output is not UTF-8")?;
    let properties = properties(username.is_some());
    let mut fields: BTreeMap<&str, &str> = BTreeMap::new();
    for line in text.lines() {
        let (key, value) = line.split_once('=').ok_or("unparsable show line")?;
        if !properties.split(',').any(|p| p == key) {
            return Err("unexpected show property");
        }
        if fields.insert(key, value).is_some() {
            return Err("repeated show property");
        }
    }
    let get = |key: &str| fields.get(key).copied().ok_or("missing show property");
    let load = get("LoadState")?;
    if let Some(username) = username {
        let user = get("User")?;
        let drop_ins = get("DropInPaths")?;
        let enabled = get("UnitFileState")?;
        if load != "not-found" && (user != username || !drop_ins.is_empty() || enabled != "enabled")
        {
            return Err("system service account, definition or boot activation differs");
        }
    }
    Ok(Show {
        system: username.is_some(),
        load: load.to_owned(),
        active: get("ActiveState")?.to_owned(),
        fragment: get("FragmentPath")?.to_owned(),
        main_pid: get("MainPID")?.parse().map_err(|_| "bad MainPID")?,
        need_reload: match get("NeedDaemonReload")? {
            "yes" => true,
            "no" => false,
            _ => return Err("bad NeedDaemonReload"),
        },
    })
}

impl Show {
    /// A stopped unit, or one systemd has not read yet, is `Absent`. A running
    /// one is `Match` only from this exact file and without a pending reload.
    pub fn classify(&self, file: &Path) -> LoadedState {
        if self.system
            && self.load == "loaded"
            && (Path::new(&self.fragment) != file || self.need_reload)
        {
            return LoadedState::Unknown;
        }
        match (self.load.as_str(), self.active.as_str()) {
            ("not-found", "inactive") => LoadedState::Absent,
            ("loaded", "inactive" | "failed") => LoadedState::Absent,
            ("loaded", "active" | "activating" | "reloading")
                if Path::new(&self.fragment) == file && !self.need_reload =>
            {
                LoadedState::Match
            }
            _ => LoadedState::Unknown,
        }
    }
    /// The main process of a unit that is active, if it has one to name.
    pub fn running_pid(&self) -> Option<u32> {
        (self.active == "active" && self.main_pid > 1).then_some(self.main_pid)
    }
}

#[cfg(test)]
mod tests;
