//! Pure launchd validation and rendering. Callers supply an existing runtime's argv.
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

/// Which service manager a plan is for. The worker-host helper is generic over
/// the manager; only the rendering and the manager's own commands differ.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Backend {
    #[default]
    Launchd,
    Systemd,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mode {
    User { uid: u32 },
    SystemUser { uid: u32, username: String },
}
#[derive(Clone, Debug)]
pub struct Definition {
    pub label: String,
    pub mode: Mode,
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    /// Only non-secret environment. Private credentials must be file references.
    pub environment: BTreeMap<String, String>,
    pub working_directory: PathBuf,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    backend: Backend,
    label: String,
    domain: String,
    /// The managed file's text: a launchd plist or a systemd unit.
    plist: String,
    domain_identity: Mode,
    executable: PathBuf,
    arguments: Vec<String>,
    working_directory: PathBuf,
    environment: BTreeMap<String, String>,
}
impl Plan {
    /// A plan whose managed file is a systemd unit (see `service::systemd`).
    /// `domain` is the `systemctl` scope flag.
    pub(crate) fn systemd(def: &Definition, domain: &str, unit: String) -> Self {
        Plan {
            backend: Backend::Systemd,
            label: def.label.clone(),
            domain: domain.into(),
            plist: unit,
            domain_identity: def.mode.clone(),
            executable: def.executable.clone(),
            arguments: std::iter::once(def.executable.to_string_lossy().into_owned())
                .chain(def.arguments.clone())
                .collect(),
            working_directory: def.working_directory.clone(),
            environment: def.environment.clone(),
        }
    }
    pub fn backend(&self) -> Backend {
        self.backend
    }
    /// The launchd domain, or the `systemctl` scope flag.
    pub fn domain(&self) -> &str {
        &self.domain
    }
    pub fn mode(&self) -> &Mode {
        &self.domain_identity
    }
    pub fn executable(&self) -> &Path {
        &self.executable
    }
    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }
    pub fn working_directory(&self) -> &Path {
        &self.working_directory
    }
    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn plist(&self) -> &str {
        &self.plist
    }
    pub fn filename(&self) -> String {
        match self.backend {
            Backend::Launchd => format!("{}.plist", self.label),
            Backend::Systemd => format!("{}.service", self.label),
        }
    }
    pub fn start_arguments(&self, installed_path: &Path) -> Result<Vec<String>, String> {
        let path = absolute(installed_path)?;
        Ok(match self.backend {
            Backend::Launchd => vec!["bootstrap".into(), self.domain.clone(), path.into()],
            Backend::Systemd => vec![self.domain.clone(), "start".into(), self.filename()],
        })
    }
    pub fn stop_arguments(&self, installed_path: &Path) -> Result<Vec<String>, String> {
        let path = absolute(installed_path)?;
        Ok(match self.backend {
            Backend::Launchd => vec!["bootout".into(), self.domain.clone(), path.into()],
            Backend::Systemd => vec![self.domain.clone(), "stop".into(), self.filename()],
        })
    }
    pub fn service_target(&self) -> String {
        match self.backend {
            Backend::Launchd => format!("{}/{}", self.domain, self.label),
            Backend::Systemd => self.filename(),
        }
    }
}
/// Whether `launchctl print` output for this value can be parsed back by the
/// strict observer. The observer applies this same predicate to every value.
pub(crate) fn observable(s: &str) -> bool {
    !(s.is_empty() || s.trim() != s || s.chars().any(|c| c.is_control() || "{}\\\"'".contains(c)))
}
fn checked_observable(s: &str) -> Result<(), String> {
    if observable(s) {
        Ok(())
    } else {
        Err(format!(
            "value {s:?} contains characters the service observer cannot parse (control characters, braces, backslash, quotes, or edge whitespace)"
        ))
    }
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
    xml(s)?;
    checked_observable(s)?;
    Ok(s)
}
fn xml(s: &str) -> Result<String, String> {
    if s.chars().any(|c| {
        !(matches!(c, '\t' | '\n' | '\r') || c >= ' ' && c != '\u{fffe}' && c != '\u{ffff}')
    }) {
        return Err("invalid XML character".into());
    }
    Ok(s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;"))
}
fn string(s: &str) -> Result<String, String> {
    Ok(format!("<string>{}</string>", xml(s)?))
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
    let (domain, user) = match &def.mode {
        Mode::User { uid } if *uid > 0 => (format!("gui/{uid}"), String::new()),
        Mode::SystemUser { uid, username }
            if *uid > 0
                && !username.is_empty()
                && username != "root"
                && username
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)) =>
        {
            (
                "system".into(),
                format!("<key>UserName</key>{}", string(username)?),
            )
        }
        _ => return Err("service requires a non-root user identity".into()),
    };
    let mut argv = string(absolute(&def.executable)?)?;
    for arg in &def.arguments {
        argv.push_str(&string(arg)?);
    }
    let mut env = String::new();
    for (key, value) in &def.environment {
        if !matches!(
            key.as_str(),
            "PATH" | "HOME" | "TMPDIR" | "LANG" | "LC_ALL" | "LC_CTYPE"
        ) {
            return Err("only explicit non-secret service environment keys are allowed".into());
        }
        checked_observable(value)?;
        env.push_str(&format!("<key>{}</key>{}", xml(key)?, string(value)?));
    }
    let plist = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key>{}<key>ProgramArguments</key><array>{argv}</array><key>WorkingDirectory</key>{}<key>EnvironmentVariables</key><dict>{env}</dict>{user}<key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n", string(&def.label)?, string(absolute(&def.working_directory)?)?);
    Ok(Plan {
        backend: Backend::Launchd,
        label: def.label.clone(),
        domain,
        plist,
        domain_identity: def.mode.clone(),
        executable: def.executable.clone(),
        arguments: std::iter::once(absolute(&def.executable)?.to_owned())
            .chain(def.arguments.clone())
            .collect(),
        working_directory: def.working_directory.clone(),
        environment: def.environment.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    pub fn definition() -> Definition {
        Definition {
            label: "org.example.worker".into(),
            mode: Mode::User { uid: 501 },
            executable: "/opt/example/bin/runner".into(),
            arguments: vec!["a<&>\"'".into()],
            environment: BTreeMap::from([("PATH".into(), "/usr/bin".into())]),
            working_directory: "/var/example".into(),
        }
    }
    #[test]
    fn rendering() {
        let d = definition();
        let p = render(&d).unwrap();
        assert_eq!(p.label(), "org.example.worker");
        assert_eq!(p.filename(), "org.example.worker.plist");
        assert_eq!(
            p.start_arguments(Path::new("/tmp/example.plist")).unwrap(),
            ["bootstrap", "gui/501", "/tmp/example.plist"]
        );
        assert_eq!(
            p.stop_arguments(Path::new("/tmp/example.plist")).unwrap(),
            ["bootout", "gui/501", "/tmp/example.plist"]
        );
        assert_eq!(p.plist(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>org.example.worker</string><key>ProgramArguments</key><array><string>/opt/example/bin/runner</string><string>a&lt;&amp;&gt;&quot;&apos;</string></array><key>WorkingDirectory</key><string>/var/example</string><key>EnvironmentVariables</key><dict><key>PATH</key><string>/usr/bin</string></dict><key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n");
        assert!(p.start_arguments(Path::new("relative")).is_err());
        let mut d = d;
        d.mode = Mode::SystemUser {
            uid: 502,
            username: "worker_user-2".into(),
        };
        let p = render(&d).unwrap();
        assert!(p
            .plist()
            .contains("<key>UserName</key><string>worker_user-2</string>"));
        assert_eq!(
            p.stop_arguments(Path::new("/tmp/example.plist")).unwrap(),
            ["bootout", "system", "/tmp/example.plist"]
        );
        assert_eq!(xml("\t\n\r😀").unwrap(), "\t\n\r😀");
    }
    #[test]
    fn a_systemd_plan_speaks_systemctl_and_a_launchd_plan_does_not() {
        let d = definition();
        let launchd = render(&d).unwrap();
        assert_eq!(launchd.backend(), Backend::Launchd);
        assert_eq!(launchd.domain(), "gui/501");
        assert_eq!(launchd.service_target(), "gui/501/org.example.worker");
        let unit = Plan::systemd(&d, "--user", "[Unit]\n".into());
        assert_eq!(unit.backend(), Backend::Systemd);
        assert_eq!(unit.domain(), "--user");
        assert_eq!(unit.plist(), "[Unit]\n");
        assert_eq!(unit.filename(), "org.example.worker.service");
        assert_eq!(unit.service_target(), "org.example.worker.service");
        let file = Path::new("/tmp/x/org.example.worker.service");
        assert_eq!(
            unit.start_arguments(file).unwrap(),
            ["--user", "start", "org.example.worker.service"]
        );
        assert_eq!(
            unit.stop_arguments(file).unwrap(),
            ["--user", "stop", "org.example.worker.service"]
        );
        assert!(unit.start_arguments(Path::new("rel")).is_err());
        assert_eq!(unit.arguments(), ["/opt/example/bin/runner", "a<&>\"'"]);
        assert_eq!(Backend::default(), Backend::Launchd);
    }
    #[test]
    fn invalid_inputs() {
        for label in ["", ".x", "x/y", "x y", &"x".repeat(129)] {
            let mut d = definition();
            d.label = label.into();
            assert!(render(&d).is_err());
        }
        for mode in [
            Mode::User { uid: 0 },
            Mode::SystemUser {
                uid: 0,
                username: "worker".into(),
            },
            Mode::SystemUser {
                uid: 1,
                username: "root".into(),
            },
            Mode::SystemUser {
                uid: 1,
                username: "".into(),
            },
            Mode::SystemUser {
                uid: 1,
                username: "bad/name".into(),
            },
        ] {
            let mut d = definition();
            d.mode = mode;
            assert!(render(&d).is_err());
        }
        for path in ["/home/o'brien", "/a{b}", "/a\\b", "/a\"b", "/a/b "] {
            let mut d = definition();
            d.executable = path.into();
            assert!(render(&d).is_err(), "{path}");
            d = definition();
            d.working_directory = path.into();
            assert!(render(&d).is_err(), "{path}");
        }
        let mut d = definition();
        d.environment.insert("HOME".into(), "/home/o'brien".into());
        assert!(render(&d).is_err());
        for path in ["relative", "/x/../y", "/x/\0"] {
            let mut d = definition();
            d.executable = path.into();
            assert!(render(&d).is_err());
            d = definition();
            d.working_directory = path.into();
            assert!(render(&d).is_err());
        }
        use std::os::unix::ffi::OsStringExt;
        assert!(absolute(&PathBuf::from(std::ffi::OsString::from_vec(vec![
            b'/', 255
        ])))
        .is_err());
        for key in ["", "1A", "A-B"] {
            let mut d = definition();
            d.environment.insert(key.into(), "v".into());
            assert!(render(&d).is_err());
        }
        let mut d = definition();
        d.arguments.push("\0".into());
        assert!(render(&d).is_err());
        let mut d = definition();
        d.environment.insert("HOME".into(), "\0".into());
        assert!(render(&d).is_err());
        for s in ["\u{fffe}", "\u{ffff}", "\u{1}"] {
            assert!(xml(s).is_err());
        }
        assert_eq!(xml(" ").unwrap(), " ");
        for label in ["x", "a_b-2", &"x".repeat(128)] {
            let mut d = definition();
            d.label = label.into();
            assert!(render(&d).is_ok());
        }
    }
}
