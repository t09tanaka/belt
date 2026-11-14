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

/// Read one explicit project scope. Call separately for monorepo roots; only
/// workflow steps in `workflows` are included, never a live workflow reread.
pub fn read_project(
    project: &str,
    documents: &BTreeMap<String, Document>,
    workflows: &[String],
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
    if documents.len() > MAX_DOCUMENTS || workflows.len() > MAX_DOCUMENTS {
        return Err(Diagnostic {
            code: "too_many_documents".into(),
            locations: vec![],
        });
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
        read_workflow(doc, &scope, documents, &mut out)?;
    }
    // Validate each individual effective scope, not all workflows together.
    effective(&out, &scope)?;
    for d in &out {
        effective(&out, &d.scope)?;
    }
    Ok(out)
}
fn read_workflow(
    doc: &Document,
    project: &Scope,
    documents: &BTreeMap<String, Document>,
    out: &mut Vec<Declaration>,
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
            let first_added = out.len();
            let Some((tool, key)) = action_tool(action) else {
                continue;
            };
            let scope = Scope {
                project: project.project.clone(),
                workflow: Some(doc.path.clone()),
                job: Some(job.id.clone()),
                step: Some(index),
            };
            let get = |key: &str| {
                step.with
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, v)| v.as_str())
            };
            if let Some(raw) = get(key) {
                add(out, doc, &scope, tool, raw, key)?;
            }
            let file_key = format!("{key}-file");
            if let Some(file) = get(&file_key) {
                if file.contains("${{") {
                    Constraint::parse(file).map_err(|code| doc.error(code, &file_key))?;
                    out.push(Declaration {
                        tool: ToolId::parse(tool).expect("registered action tool"),
                        constraint: Constraint::VersionFileExpression(file.into()),
                        scope: scope.clone(),
                        location: doc.location(&file_key),
                    });
                } else {
                    if !safe_path(file) {
                        return Err(doc.error("invalid_version_file", &file_key));
                    }
                    let path = if project.project.is_empty() {
                        file.into()
                    } else {
                        format!("{}/{file}", project.project)
                    };
                    let input = documents
                        .get(&path)
                        .ok_or_else(|| doc.error("missing_version_file", &file_key))?;
                    let value = input
                        .text
                        .lines()
                        .filter_map(|line| line.split('#').next())
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .collect::<Vec<_>>()
                        .join(" ");
                    add(out, input, &scope, tool, &value, "")?;
                }
            }
            if action == "dtolnay/rust-toolchain"
                && get(key).is_none()
                && ["stable", "beta", "nightly"].contains(&reference)
            {
                add(out, doc, &scope, tool, reference, "uses")?;
            }
            for declaration in &mut out[first_added..] {
                if declaration.location.path == doc.path {
                    let line = lines
                        .iter()
                        .enumerate()
                        .take(end)
                        .skip(start)
                        .find(|(_, line)| line.contains(key))
                        .map_or(start, |(i, _)| i)
                        + 1;
                    declaration.location.first_line = line;
                    declaration.location.last_line = line;
                }
            }
        }
    }
    Ok(())
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
/// Only recognized setup actions may request captured version-file bytes.
pub fn workflow_version_files(doc: &Document) -> Result<Vec<String>, Diagnostic> {
    let workflow = local_ci::workflow::parse(&doc.path, &doc.text)
        .map_err(|_| doc.error("invalid_workflow", ""))?;
    let mut paths = vec![];
    for step in workflow.jobs.into_iter().flat_map(|j| j.steps) {
        let Some(uses) = step.uses else { continue };
        let action = uses.split('@').next().unwrap_or("");
        let Some((_, key)) = action_tool(action) else {
            continue;
        };
        let file_key = format!("{key}-file");
        for (_, path) in step.with.into_iter().filter(|(k, _)| k == &file_key) {
            if path.contains("${{") {
                Constraint::parse(&path).map_err(|code| doc.error(code, &file_key))?;
                continue;
            }
            if !safe_path(&path) {
                return Err(doc.error("invalid_version_file", &file_key));
            }
            paths.push(path);
        }
    }
    Ok(paths)
}
