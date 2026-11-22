//! Readers over immutable documents. Diagnostics expose locations/codes only,
//! never parser excerpts, environment values, or unrelated manifest content.
use super::versions::*;
use serde_json::Value;
use std::collections::BTreeMap;
pub const MAX_DOCUMENT_BYTES: usize = 256 * 1024;
pub const MAX_DOCUMENTS: usize = 256;
pub const VERSION_FILES: &[&str] = &[
    ".node-version",
    ".nvmrc",
    "package.json",
    ".tool-versions",
    "mise.toml",
    "rust-toolchain",
    "rust-toolchain.toml",
    ".fvmrc",
    ".fvm/fvm_config.json",
    "pubspec.yaml",
    ".xcode-version",
];
#[derive(Debug, Clone)]
pub struct Document {
    pub path: String,
    pub text: String,
}
impl Document {
    pub fn new(path: &str, bytes: &[u8]) -> Result<Self, Diagnostic> {
        let location = Location {
            path: path.chars().take(1024).collect(),
            first_line: 1,
            last_line: 1,
        };
        if !safe_path(path) || bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(Diagnostic::at("invalid_document", location));
        }
        let text = std::str::from_utf8(bytes)
            .map_err(|_| Diagnostic::at("invalid_utf8", location))?
            .to_string();
        Ok(Self {
            path: path.into(),
            text,
        })
    }
    fn location(&self, key: &str) -> Location {
        let line = self
            .text
            .lines()
            .position(|l| l.contains(key))
            .map_or(1, |i| i + 1);
        Location {
            path: self.path.clone(),
            first_line: line,
            last_line: line,
        }
    }
    fn error(&self, code: &str, key: &str) -> Diagnostic {
        Diagnostic::at(code, self.location(key))
    }
}
pub fn safe_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 1024
        && path.split('/').all(|s| {
            !s.is_empty()
                && s != "."
                && s != ".."
                && !s.chars().any(|c| c.is_control() || c == '\\')
        })
}
fn add(
    out: &mut Vec<Declaration>,
    doc: &Document,
    scope: &Scope,
    tool: &str,
    raw: &str,
    key: &str,
) -> Result<(), Diagnostic> {
    let tool = ToolId::parse(tool).map_err(|code| doc.error(code, key))?;
    let constraint = Constraint::parse(raw).map_err(|code| doc.error(code, key))?;
    out.push(Declaration {
        tool,
        constraint,
        scope: scope.clone(),
        location: doc.location(key),
    });
    Ok(())
}
fn text<'a>(doc: &Document, value: &'a Value, key: &str) -> Result<&'a str, Diagnostic> {
    value
        .as_str()
        .ok_or_else(|| doc.error("expected_version_string", key))
}
fn object(doc: &Document, format: &str) -> Result<Value, Diagnostic> {
    let value: Value = match format {
        "json" => serde_json::from_str(&doc.text).map_err(|_| doc.error("invalid_json", ""))?,
        "toml" => toml::from_str::<toml::Value>(&doc.text)
            .ok()
            .and_then(|v| serde_json::to_value(v).ok())
            .ok_or_else(|| doc.error("invalid_toml", ""))?,
        _ => serde_yaml::from_str(&doc.text).map_err(|_| doc.error("invalid_yaml", ""))?,
    };
    if !value.is_object() {
        return Err(doc.error("invalid_document_root", ""));
    }
    Ok(value)
}

fn too_many_documents() -> Diagnostic {
    Diagnostic {
        code: "too_many_documents".into(),
        locations: vec![],
    }
}
/// A version file in a format of its own (`go.mod`, `pyproject.toml`): it is
/// never read as a plain version.
pub const STRUCTURED_VERSION_FILE: &str = "structured_version_file";
const STRUCTURED: &[&str] = &[
    ".mod", ".work", ".toml", ".json", ".yaml", ".yml", ".lock", ".xml", ".gradle", ".kts",
];

/// Read one explicit project scope. Call separately for monorepo roots; only
/// workflow steps in `workflows` are included, never a live workflow reread.
pub fn read_project(
    project: &str,
    documents: &BTreeMap<String, Document>,
    workflows: &[String],
) -> Result<Vec<Declaration>, Diagnostic> {
    Ok(read_setup(project, documents, workflows)?.declarations)
}
/// [`read_project`] with what was left out. An input of a setup step that
/// cannot be read is an error, except a version file in a structured format,
/// which is only skipped. A step under an `if:` condition is read as any
/// other.
pub fn read_setup(
    project: &str,
    documents: &BTreeMap<String, Document>,
    workflows: &[String],
) -> Result<SetupRead, Diagnostic> {
    if workflows.len() > MAX_DOCUMENTS {
        return Err(too_many_documents());
    }
    let scope = Scope::project(project);
    let mut read = SetupRead {
        declarations: read_files(project, documents)?,
        ..Default::default()
    };
    for path in workflows {
        let doc = documents.get(path).ok_or_else(|| {
            Diagnostic::at(
                "missing_workflow",
                Location {
                    path: path.clone(),
                    first_line: 1,
                    last_line: 1,
                },
            )
        })?;
        read_workflow(doc, &scope, documents, &mut read)?;
        let unread = |s: &&Skipped| s.code != STRUCTURED_VERSION_FILE;
        if let Some(skipped) = read.skipped.iter().find(unread) {
            return Err(Diagnostic::at(&skipped.code, skipped.location.clone()));
        }
    }
    // Validate each individual effective scope, not all workflows together.
    effective(&read.declarations, &scope)?;
    for d in &read.declarations {
        effective(&read.declarations, &d.scope)?;
    }
    Ok(read)
}
/// The declarations of the version files of one project scope. No workflow is
/// read and nothing is compared.
pub fn read_files(
    project: &str,
    documents: &BTreeMap<String, Document>,
) -> Result<Vec<Declaration>, Diagnostic> {
    if !project.is_empty() && !safe_path(project) {
        return Err(Diagnostic::at(
            "invalid_project_path",
            Location {
                path: project.chars().take(1024).collect(),
                first_line: 1,
                last_line: 1,
            },
        ));
    }
    if documents.len() > MAX_DOCUMENTS {
        return Err(too_many_documents());
    }
    for (path, doc) in documents {
        if path != &doc.path {
            return Err(doc.error("document_path_mismatch", ""));
        }
        Document::new(path, doc.text.as_bytes())?;
    }
    let scope = Scope::project(project);
    let mut out = vec![];
    for name in VERSION_FILES {
        let path = if project.is_empty() {
            name.to_string()
        } else {
            format!("{project}/{name}")
        };
        let Some(doc) = documents.get(&path) else {
            continue;
        };
        match *name {
            ".node-version" | ".nvmrc" | ".xcode-version" => {
                let tool = if *name == ".xcode-version" {
                    "xcode"
                } else {
                    "node"
                };
                let value = doc
                    .text
                    .lines()
                    .filter_map(|l| l.split('#').next())
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ");
                add(&mut out, doc, &scope, tool, &value, &value)?;
            }
            ".tool-versions" => {
                for (i, line) in doc.text.lines().enumerate() {
                    let words: Vec<_> = line
                        .split('#')
                        .next()
                        .unwrap_or("")
                        .split_whitespace()
                        .collect();
                    if words.is_empty() {
                        continue;
                    }
                    if words.len() < 2 {
                        return Err(doc.error("missing_version", words[0]));
                    }
                    add(
                        &mut out,
                        doc,
                        &scope,
                        words[0],
                        &words[1..].join(" || "),
                        words[0],
                    )?;
                    out.last_mut().expect("added").location.first_line = i + 1;
                    out.last_mut().expect("added").location.last_line = i + 1;
                }
            }
            "package.json" => {
                let v = object(doc, "json")?;
                if let Some(engines) = v.get("engines") {
                    for (tool, value) in engines
                        .as_object()
                        .ok_or_else(|| doc.error("expected_engines_object", "engines"))?
                    {
                        add(&mut out, doc, &scope, tool, text(doc, value, tool)?, tool)?;
                    }
                }
                if let Some(manager) = v.get("packageManager") {
                    let value = text(doc, manager, "packageManager")?;
                    let (tool, version) = value
                        .split_once('@')
                        .ok_or_else(|| doc.error("invalid_package_manager", "packageManager"))?;
                    if !["npm", "pnpm", "yarn"].contains(&tool) {
                        return Err(doc.error("unsupported_package_manager", "packageManager"));
                    }
                    add(&mut out, doc, &scope, tool, version, "packageManager")?;
                }
            }
            "mise.toml" => {
                let v = object(doc, "toml")?;
                if let Some(tools) = v.get("tools") {
                    for (tool, value) in tools
                        .as_object()
                        .ok_or_else(|| doc.error("expected_tools_object", "tools"))?
                    {
                        let value = value.get("version").unwrap_or(value);
                        let raw = if let Some(values) = value.as_array() {
                            values
                                .iter()
                                .map(|v| text(doc, v, tool))
                                .collect::<Result<Vec<_>, _>>()?
                                .join(" || ")
                        } else {
                            text(doc, value, tool)?.into()
                        };
                        add(&mut out, doc, &scope, tool, &raw, tool)?;
                    }
                }
            }
            "rust-toolchain" | "rust-toolchain.toml" => {
                let raw = doc.text.trim();
                if *name == "rust-toolchain" && !raw.starts_with('[') {
                    add(&mut out, doc, &scope, "rust", raw, raw)?;
                } else {
                    let v = object(doc, "toml")?;
                    let value = v
                        .pointer("/toolchain/channel")
                        .ok_or_else(|| doc.error("missing_rust_channel", "toolchain"))?;
                    add(
                        &mut out,
                        doc,
                        &scope,
                        "rust",
                        text(doc, value, "channel")?,
                        "channel",
                    )?;
                }
            }
            ".fvmrc" | ".fvm/fvm_config.json" => {
                let v = object(doc, "json")?;
                let key = if *name == ".fvmrc" {
                    "flutter"
                } else {
                    "flutterSdkVersion"
                };
                let value = v
                    .get(key)
                    .ok_or_else(|| doc.error("missing_flutter_version", key))?;
                add(
                    &mut out,
                    doc,
                    &scope,
                    "flutter",
                    text(doc, value, key)?,
                    key,
                )?;
            }
            // The only remaining registered format is pubspec.yaml.
            _ => {
                let v = object(doc, "yaml")?;
                if let Some(environment) = v.get("environment") {
                    let env = environment
                        .as_object()
                        .ok_or_else(|| doc.error("expected_environment_object", "environment"))?;
                    for (key, tool) in [("sdk", "dart"), ("flutter", "flutter")] {
                        if let Some(value) = env.get(key) {
                            let raw = text(doc, value, key)?;
                            out.push(Declaration {
                                tool: ToolId::parse(tool).expect("registered pub tool"),
                                constraint: Constraint::Pub(
                                    PubRange::parse(raw).map_err(|code| doc.error(code, key))?,
                                ),
                                scope: scope.clone(),
                                location: doc.location(key),
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}
/// The setup steps of one workflow, added to `read`: each version input is a
/// declaration or, when it names no version that can be read, a skipped
/// input with the code of the reason. Only a workflow that does not parse is
/// an error.
pub fn read_workflow(
    doc: &Document,
    project: &Scope,
    documents: &BTreeMap<String, Document>,
    read: &mut SetupRead,
) -> Result<(), Diagnostic> {
    let workflow = local_ci::workflow::parse(&doc.path, &doc.text)
        .map_err(|_| doc.error("invalid_workflow", ""))?;
    let lines: Vec<_> = doc.text.lines().collect();
    let mut cursor = 0;
    for job in workflow.jobs {
        for (index, step) in job.steps.into_iter().enumerate() {
            let Some(uses) = step.uses else { continue };
            let (action, reference) = uses.split_once('@').unwrap_or((&uses, ""));
            let start = lines
                .iter()
                .enumerate()
                .skip(cursor)
                .find(|(_, line)| line.contains("uses:") && line.contains(action))
                .map_or(cursor, |(i, _)| i);
            cursor = start.saturating_add(1);
            let end = lines
                .iter()
                .enumerate()
                .skip(cursor)
                .find(|(_, line)| line.trim_start().starts_with("- "))
                .map_or(lines.len(), |(i, _)| i);
            let Some((tool, key)) = action_tool(action) else {
                continue;
            };
            let tool = ToolId::parse(tool).expect("registered action tool");
            let scope = Scope {
                project: project.project.clone(),
                workflow: Some(doc.path.clone()),
                job: Some(job.id.clone()),
                step: Some(index),
            };
            // The line of the step's version input, or of the step itself.
            let line = lines
                .iter()
                .enumerate()
                .take(end)
                .skip(start)
                .find(|(_, line)| line.contains(key))
                .map_or(start, |(i, _)| i)
                + 1;
            let here = Location {
                path: doc.path.clone(),
                first_line: line,
                last_line: line,
            };
            // The step itself: its `uses:` line.
            let step_line = Location {
                path: doc.path.clone(),
                first_line: start + 1,
                last_line: start + 1,
            };
            read.steps.push((scope.clone(), step_line.clone()));
            if step.cond.is_some() {
                read.conditional.push(scope.clone());
            }
            let mut found: Vec<(Result<Constraint, String>, &str, Location)> = Vec::new();
            let (direct, file) = version_inputs(action, key, &step.with);
            if let Some(raw) = direct {
                let constraint = Constraint::parse(raw).map_err(String::from);
                found.push((constraint, raw, here.clone()));
            }
            if let Some(file) = file {
                let (constraint, location) = version_file(action, &tool, file, documents, &here);
                found.push((constraint, file, location));
            }
            if action == "dtolnay/rust-toolchain"
                && !step.with.iter().any(|(name, _)| name == key)
                && rust_reference(reference)
            {
                if let Ok(constraint) = Constraint::parse(reference) {
                    found.push((Ok(constraint), reference, here.clone()));
                }
            }
            for (constraint, value, location) in found {
                match constraint {
                    Ok(constraint) => read.declarations.push(Declaration {
                        tool: tool.clone(),
                        constraint,
                        scope: scope.clone(),
                        location,
                    }),
                    Err(code) => read.skipped.push(Skipped {
                        tool: tool.clone(),
                        scope: scope.clone(),
                        code,
                        value: value.chars().take(80).collect(),
                        location,
                        step: step_line.clone(),
                    }),
                }
            }
        }
    }
    Ok(())
}
/// The version a `*-version-file` input names, and where it is written: in
/// the file, or at the step (`here`) when no file was read.
fn version_file(
    action: &str,
    tool: &ToolId,
    file: &str,
    documents: &BTreeMap<String, Document>,
    here: &Location,
) -> (Result<Constraint, String>, Location) {
    let at_step = |read: Result<Constraint, &str>| (read.map_err(String::from), here.clone());
    if file.contains("${{") {
        let path = Constraint::VersionFileExpression(file.into());
        return at_step(Constraint::parse(file).map(|_| path));
    }
    if !safe_path(file) {
        return at_step(Err("invalid_version_file"));
    }
    let Some(input) = documents.get(file) else {
        return at_step(Err("missing_version_file"));
    };
    if action == "actions/setup-node" {
        return match node_file_constraint(input) {
            Ok((constraint, key)) => (Ok(constraint), input.location(key)),
            Err(diagnostic) => (Err(diagnostic.code), diagnostic.locations[0].clone()),
        };
    }
    // A file in a format the project's own reader knows is read by it, for
    // the tool of the step.
    let registered = VERSION_FILES.iter().find_map(|name| {
        let dir = file.strip_suffix(name)?;
        (dir.is_empty() || dir.ends_with('/')).then(|| dir.trim_end_matches('/'))
    });
    if let Some(dir) = registered {
        let one = BTreeMap::from([(file.to_string(), input.clone())]);
        return match read_files(dir, &one) {
            Ok(found) => match found.into_iter().find(|d| &d.tool == tool) {
                Some(d) => (Ok(d.constraint), d.location),
                None => at_step(Err("version_file_without_tool")),
            },
            Err(unread) => {
                let at = unread.locations.first().unwrap_or(here).clone();
                (Err(unread.code), at)
            }
        };
    }
    if STRUCTURED.iter().any(|suffix| file.ends_with(suffix)) {
        return at_step(Err(STRUCTURED_VERSION_FILE));
    }
    let version = Constraint::parse(&plain_version(&input.text)).map_err(String::from);
    (version, input.location(""))
}
/// `dtolnay/rust-toolchain@<reference>` names the toolchain when the
/// reference is a channel or a version (`1.89`, `1.89.0`); a branch, a tag
/// of the action or a commit does not.
fn rust_reference(reference: &str) -> bool {
    ["stable", "beta", "nightly"].contains(&reference)
        || (reference.contains('.') && reference.bytes().all(|b| b.is_ascii_digit() || b == b'.'))
}

fn action_tool(action: &str) -> Option<(&'static str, &'static str)> {
    Some(match action {
        "actions/setup-node" => ("node", "node-version"),
        "actions/setup-python" => ("python", "python-version"),
        "actions/setup-go" => ("go", "go-version"),
        "actions/setup-java" => ("java", "java-version"),
        "ruby/setup-ruby" => ("ruby", "ruby-version"),
        "subosito/flutter-action" => ("flutter", "flutter-version"),
        "dart-lang/setup-dart" => ("dart", "sdk"),
        "maxim-lobanov/setup-xcode" => ("xcode", "xcode-version"),
        "actions-rust-lang/setup-rust-toolchain" | "dtolnay/rust-toolchain" => {
            ("rust", "toolchain")
        }
        "pnpm/action-setup" => ("pnpm", "version"),
        _ => return None,
    })
}
/// The `*-version-file` inputs of the recognized setup steps of `doc`: the
/// input's name and the path as written.
fn version_file_inputs(doc: &Document) -> Result<Vec<(String, String)>, Diagnostic> {
    let workflow = local_ci::workflow::parse(&doc.path, &doc.text)
        .map_err(|_| doc.error("invalid_workflow", ""))?;
    let mut inputs = vec![];
    for step in workflow.jobs.into_iter().flat_map(|j| j.steps) {
        let Some(uses) = step.uses else { continue };
        let action = uses.split('@').next().unwrap_or("");
        let Some((_, key)) = action_tool(action) else {
            continue;
        };
        if let Some(path) = version_inputs(action, key, &step.with).1 {
            inputs.push((format!("{key}-file"), path.to_string()));
        }
    }
    Ok(inputs)
}
/// Only recognized setup actions may request captured version-file bytes.
pub fn workflow_version_files(doc: &Document) -> Result<Vec<String>, Diagnostic> {
    let mut paths = vec![];
    for (file_key, path) in version_file_inputs(doc)? {
        if path.contains("${{") {
            Constraint::parse(&path).map_err(|code| doc.error(code, &file_key))?;
            continue;
        }
        if !safe_path(&path) {
            return Err(doc.error("invalid_version_file", &file_key));
        }
        paths.push(path);
    }
    Ok(paths)
}
/// [`workflow_version_files`] for the freeze of a CI task: the paths that can
/// name a file of the source. Anything else names no file to read, and a
/// workflow that does not parse names none.
pub fn setup_version_files(doc: &Document) -> Vec<String> {
    let inputs = version_file_inputs(doc).unwrap_or_default();
    let paths = inputs.into_iter().map(|(_, path)| path);
    let named = |path: &String| !path.contains("${{") && safe_path(path);
    paths.filter(named).collect()
}

// setup-node returns its nonempty direct input before it consults a file.
// Share this decision with snapshot discovery, including unresolved expressions.
fn version_inputs<'a>(
    action: &str,
    key: &str,
    inputs: &'a [(String, String)],
) -> (Option<&'a str>, Option<&'a str>) {
    let get = |key: &str| {
        inputs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    let mut direct = get(key);
    if action == "actions/setup-node" {
        direct = direct.filter(|value| !value.trim().is_empty());
    }
    let file = if action == "actions/setup-node" && direct.is_some() {
        None
    } else {
        get(&format!("{key}-file"))
    };
    (direct, file)
}
fn plain_version(text: &str) -> String {
    text.lines()
        .filter_map(|line| line.split('#').next())
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}
// Format/selector precedence follows actions/setup-node src/util.ts:
// Volta, npm devEngines runtime, engines; mise tools.node; node(js) prefix.
// Only the supplied immutable document is read. Unsupported structured choices
// require explicit resolution, never fallback to another installed version.
fn node_file_constraint(doc: &Document) -> Result<(Constraint, &'static str), Diagnostic> {
    let raw = doc.text.trim();
    let mut selected = None;
    if let Ok(value) = serde_json::from_str::<Value>(raw) {
        if value.is_object() {
            if let Some(version) = value.pointer("/volta/node").filter(|v| json_truthy(v)) {
                selected = Some((version.clone(), "node"));
            }
            if selected.is_none() {
                if let Some(runtime) = value.pointer("/devEngines/runtime") {
                    let candidates = runtime
                        .as_array()
                        .map(Vec::as_slice)
                        .unwrap_or(std::slice::from_ref(runtime));
                    if let Some(entry) = candidates.iter().find(|entry| {
                        entry
                            .get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|name| name.eq_ignore_ascii_case("node"))
                            && entry.get("version").is_some_and(json_truthy)
                    }) {
                        selected = entry.get("version").map(|v| (v.clone(), "version"));
                    }
                }
            }
            if selected.is_none() {
                selected = value
                    .pointer("/engines/node")
                    .filter(|v| json_truthy(v))
                    .map(|v| (v.clone(), "node"));
            }
            if selected.is_none() {
                return Err(doc.error("node_version_file_requires_resolution", ""));
            }
        }
    } else if raw.starts_with('{') {
        return Err(doc.error("invalid_json", ""));
    }
    if selected.is_none() {
        if let Ok(value) = toml::from_str::<toml::Value>(raw) {
            if let Some(node) = value.get("tools").and_then(|v| v.get("node")) {
                let version = node.get("version").unwrap_or(node);
                selected = Some((
                    serde_json::to_value(version).expect("TOML value serializes"),
                    "node",
                ));
            }
        }
    }
    if let Some((value, key)) = selected {
        return Ok((
            Constraint::parse(text(doc, &value, key)?).map_err(|code| doc.error(code, key))?,
            key,
        ));
    }
    for line in raw.lines() {
        let words: Vec<_> = line
            .split('#')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect();
        if words.len() == 2 && matches!(words[0], "node" | "nodejs") {
            return Ok((
                Constraint::parse(words[1]).map_err(|code| doc.error(code, words[0]))?,
                "node",
            ));
        }
    }
    Ok((
        Constraint::parse(&plain_version(raw)).map_err(|code| doc.error(code, ""))?,
        "",
    ))
}

fn json_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64() != Some(0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}
