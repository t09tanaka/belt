//! Pure launchd validation and rendering. Callers supply an existing runtime's argv.
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

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
    label: String,
    domain: String,
    plist: String,
}
impl Plan {
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn plist(&self) -> &str {
        &self.plist
    }
    pub fn filename(&self) -> String {
        format!("{}.plist", self.label)
    }
    pub fn start_arguments(&self, installed_path: &Path) -> Result<Vec<String>, String> {
        let path = absolute(installed_path)?;
        Ok(vec!["bootstrap".into(), self.domain.clone(), path.into()])
    }
    pub fn stop_arguments(&self) -> Vec<String> {
        vec!["bootout".into(), format!("{}/{}", self.domain, self.label)]
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
        env.push_str(&format!("<key>{}</key>{}", xml(key)?, string(value)?));
    }
    let plist = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key>{}<key>ProgramArguments</key><array>{argv}</array><key>WorkingDirectory</key>{}<key>EnvironmentVariables</key><dict>{env}</dict>{user}<key>RunAtLoad</key><true/><key>KeepAlive</key><true/></dict></plist>\n", string(&def.label)?, string(absolute(&def.working_directory)?)?);
    Ok(Plan {
        label: def.label.clone(),
        domain,
        plist,
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
            p.stop_arguments(),
            ["bootout", "gui/501/org.example.worker"]
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
        assert_eq!(p.stop_arguments(), ["bootout", "system/org.example.worker"]);
        assert_eq!(xml("\t\n\r😀").unwrap(), "\t\n\r😀");
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
