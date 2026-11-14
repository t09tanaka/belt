//! Strict adapter for the tab-indented launchctl print layout observed on an
//! Apple daemon (2026-11-14). Human output is not an API: any unknown layout is
//! rejected. This is observation under trusted same-UID/root, not atomic removal.
use super::policy::{Mode, Plan};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const LIMIT: usize = 64 * 1024;
#[derive(PartialEq, Eq)]
pub struct Observed {
    target: String,
    fields: BTreeMap<String, String>,
    arguments: Vec<String>,
    environment: BTreeMap<String, String>,
    defaults: BTreeMap<String, String>,
    inherited: BTreeMap<String, String>,
}
fn value(s: &str) -> Result<&str, &'static str> {
    if s.is_empty() || s.trim() != s || s.chars().any(|c| c.is_control() || "{}\\\"'".contains(c)) {
        return Err("ambiguous value");
    }
    Ok(s)
}
fn scalar(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "type"
            | "state"
            | "program"
            | "domain"
            | "username"
            | "group"
            | "working directory"
            | "active count"
            | "minimum runtime"
            | "base minimum runtime"
            | "exit timeout"
            | "runs"
            | "pid"
            | "immediate reason"
            | "forks"
            | "execs"
            | "initialized"
            | "trampolined"
            | "started suspended"
            | "proxy started suspended"
            | "checked allocations"
            | "checked allocations reason"
            | "checked allocations flags"
            | "last exit code"
            | "spawn type"
            | "jetsam priority"
            | "jetsam memory limit (active, soft)"
            | "jetsam memory limit (inactive, soft)"
            | "jetsamproperties category"
            | "jetsam thread limit"
            | "cpumon"
            | "exponential throttling grace limit"
            | "sanitizer flags"
            | "properties"
            | "asid"
            | "spawn role"
            | "job state"
    )
}
impl Observed {
    pub fn running_pid(&self) -> Result<u32, &'static str> {
        if self.fields.get("state").map(String::as_str) != Some("running") {
            return Err("service is not running");
        }
        let text = self.fields.get("pid").ok_or("missing running pid")?;
        let pid = text.parse::<u32>().map_err(|_| "invalid running pid")?;
        if pid <= 1 || i32::try_from(pid).is_err() || pid.to_string() != *text {
            return Err("invalid running pid");
        }
        Ok(pid)
    }

    pub fn username(&self) -> Option<&str> {
        self.fields.get("username").map(String::as_str)
    }
    pub fn verify(
        &self,
        plan: &Plan,
        path: &Path,
        resolved: Option<(&str, u32)>,
    ) -> Result<(), &'static str> {
        let field = |key| self.fields.get(key).map(String::as_str);
        if self.target != plan.service_target()
            || field("path") != path.to_str()
            || field("program") != plan.executable().to_str()
            || self.arguments != plan.arguments()
        {
            return Err("loaded identity mismatch");
        }
        let domain = plan.service_target().rsplit_once('/').unwrap().0.to_owned();
        let observed_domain = field("domain").ok_or("missing domain")?;
        if domain.starts_with("gui/") {
            let asid = observed_domain
                .strip_prefix(&format!("{domain} ["))
                .and_then(|s| s.strip_suffix(']'))
                .ok_or("loaded domain mismatch")?;
            let number = asid.parse::<u32>().map_err(|_| "loaded domain mismatch")?;
            if number == 0 || number.to_string() != asid || field("asid") != Some(asid) {
                return Err("loaded domain mismatch");
            }
        } else if observed_domain != domain {
            return Err("loaded domain mismatch");
        }
        match plan.mode() {
            Mode::User { uid } => {
                if let Some(name) = self.username() {
                    if resolved != Some((name, *uid)) {
                        return Err("loaded user mismatch");
                    }
                }
            }
            Mode::SystemUser { uid, username } => {
                if self.username() != Some(username) || resolved != Some((username.as_str(), *uid))
                {
                    return Err("loaded user mismatch");
                }
            }
        }
        if let Some(cwd) = field("working directory") {
            if Some(cwd) != plan.working_directory().to_str() {
                return Err("loaded directory mismatch");
            }
        }
        // Provenance matters: a job-defined hook is not an OS-inherited key.
        // These exact additions were present in the observed Apple snapshots.
        for (scope, entries) in [
            ("environment", &self.environment),
            ("default", &self.defaults),
            ("inherited", &self.inherited),
        ] {
            for (key, val) in entries {
                if plan.environment().contains_key(key) {
                    continue;
                }
                let approved = match (scope, key.as_str(), val.as_str()) {
                    ("environment", "XPC_SERVICE_NAME", name) => name == plan.label(),
                    ("environment", "OSLogRateLimit", "64")
                    | ("environment", "MallocSpaceEfficient", "1")
                    | ("environment", "MallocMaxMagazines", "1")
                    | ("default", "PATH", "/usr/bin:/bin:/usr/sbin:/sbin") => true,
                    ("inherited", "SSH_AUTH_SOCK", socket) => {
                        matches!(plan.mode(), Mode::User { .. })
                            && socket
                                .strip_prefix("/var/run/com.apple.launchd.")
                                .and_then(|s| s.strip_suffix("/Listeners"))
                                .is_some_and(|token| {
                                    !token.is_empty()
                                        && token.bytes().all(|b| b.is_ascii_alphanumeric())
                                })
                    }
                    _ => false,
                };
                if !approved {
                    return Err("unapproved loaded environment");
                }
            }
        }
        for (key, expected) in plan.environment() {
            if self
                .environment
                .get(key)
                .or_else(|| self.inherited.get(key))
                .or_else(|| self.defaults.get(key))
                != Some(expected)
            {
                return Err("loaded environment mismatch");
            }
        }
        Ok(())
    }
}
pub fn parse(bytes: &[u8]) -> Result<Observed, &'static str> {
    if bytes.len() > LIMIT {
        return Err("output limit");
    }
    let text = std::str::from_utf8(bytes).map_err(|_| "invalid encoding")?;
    if !text.ends_with('\n') {
        return Err("truncated output");
    }
    let mut lines = text.split_terminator('\n');
    let target = value(
        lines
            .next()
            .ok_or("missing header")?
            .strip_suffix(" = {")
            .ok_or("header format")?,
    )?
    .to_owned();
    let mut result = Observed {
        target,
        fields: BTreeMap::new(),
        arguments: Vec::new(),
        environment: BTreeMap::new(),
        defaults: BTreeMap::new(),
        inherited: BTreeMap::new(),
    };
    let mut seen = BTreeSet::new();
    let mut ended = false;
    while let Some(line) = lines.next() {
        if line == "}" {
            ended = true;
            break;
        }
        if line.is_empty() {
            continue;
        }
        let (key, val) = line
            .strip_prefix('\t')
            .ok_or("field indent")?
            .split_once(" = ")
            .ok_or("field format")?;
        if !seen.insert(key.to_owned()) {
            return Err("duplicate field");
        }
        if val != "{" {
            if !scalar(key) {
                return Err("unknown field");
            }
            result.fields.insert(key.to_owned(), value(val)?.to_owned());
            continue;
        }
        if matches!(
            key,
            "endpoints"
                | "pid-local endpoints"
                | "resource coalition"
                | "jetsam coalition"
                | "resource limits"
                | "semaphores"
        ) {
            diagnostic(&mut lines, key)?;
            continue;
        }
        if !matches!(
            key,
            "arguments" | "environment" | "default environment" | "inherited environment"
        ) {
            return Err("unknown block");
        }
        let mut closed = false;
        let mut entries = BTreeSet::new();
        for item in lines.by_ref() {
            if item == "\t}" {
                closed = true;
                break;
            }
            let item = item.strip_prefix("\t\t").ok_or("block indent")?;
            match key {
                "environment" | "default environment" | "inherited environment" => {
                    let (name, val) = item.split_once(" => ").ok_or("environment format")?;
                    value(name)?;
                    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                        return Err("environment name");
                    }
                    if !entries.insert(name.to_owned()) {
                        return Err("duplicate entry");
                    }
                    let map = match key {
                        "environment" => &mut result.environment,
                        "default environment" => &mut result.defaults,
                        _ => &mut result.inherited,
                    };
                    map.insert(name.to_owned(), value(val)?.to_owned());
                }
                _ => result.arguments.push(value(item)?.to_owned()),
            }
        }
        if !closed {
            return Err("truncated block");
        }
    }
    if !ended || lines.next().is_some() || !seen.contains("arguments") {
        return Err("envelope format");
    }
    for required in ["path", "program", "domain"] {
        if !result.fields.contains_key(required) {
            return Err("missing identity field");
        }
    }
    Ok(result)
}

fn diagnostic<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    section: &str,
) -> Result<(), &'static str> {
    let mut names = BTreeSet::new();
    while let Some(line) = lines.next() {
        if line == "\t}" {
            return Ok(());
        }
        let line = line.strip_prefix("\t\t").ok_or("diagnostic indent")?;
        if matches!(section, "endpoints" | "pid-local endpoints") {
            let name = line
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix("\" = {"))
                .ok_or("endpoint format")?;
            value(name)?;
            if !names.insert(name.to_owned()) {
                return Err("duplicate endpoint");
            }
            let mut fields = BTreeSet::new();
            let mut closed = false;
            for field in lines.by_ref() {
                if field == "\t\t}" {
                    closed = true;
                    break;
                }
                let (key, val) = field
                    .strip_prefix("\t\t\t")
                    .and_then(|s| s.split_once(" = "))
                    .ok_or("endpoint field")?;
                if !matches!(
                    key,
                    "port" | "active" | "managed" | "reset" | "hide" | "watching"
                ) || !fields.insert(key.to_owned())
                {
                    return Err("unknown/duplicate endpoint field");
                }
                value(val)?;
            }
            if !closed {
                return Err("truncated endpoint");
            }
        } else {
            let separator = if matches!(section, "semaphores" | "resource limits") {
                " => "
            } else {
                " = "
            };
            let (key, val) = line.split_once(separator).ok_or("diagnostic format")?;
            let known = match section {
                "semaphores" => matches!(key, "provides events" | "successful exit"),
                "resource limits" => matches!(key, "maxfiles (soft)" | "maxfiles (hard)"),
                _ => matches!(key, "ID" | "type" | "state" | "active count" | "name"),
            };
            if !known || !names.insert(key.to_owned()) {
                return Err("unknown/duplicate diagnostic field");
            }
            value(val)?;
        }
    }
    Err("truncated diagnostic")
}

#[cfg(test)]
mod tests;
