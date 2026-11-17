//! Reports that a sharded invocation would override or publish multiple times.
use super::*;
use std::io::Read;

fn reported(text: &str) -> Option<NotSplit> {
    let lower = text.to_ascii_lowercase();
    if [
        "collectcoverage",
        "coveragethreshold",
        "@cypress/code-coverage",
        "llvm_profile_file",
    ]
    .iter()
    .any(|word| lower.contains(word))
        || (lower.contains("coverage") && lower.contains("enabled"))
    {
        return Some(NotSplit::CoverageNotMerged);
    }
    [
        "reporter",
        "junit",
        "outputfile",
        "testresultsprocessor",
        "assertioncount",
    ]
    .iter()
    .any(|word| lower.contains(word))
    .then_some(NotSplit::InexactAggregation)
}

const MAX_CONFIG_BYTES: u64 = 1 << 20;

fn read_config(workspace: &Path, file: &Path) -> Option<String> {
    let rel = file.strip_prefix(workspace).ok()?;
    let input = crate::platform::open_beneath(workspace, rel).ok()?;
    if !input.metadata().ok()?.is_file() {
        return None;
    }
    let mut text = String::new();
    input
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .ok()?;
    (text.len() as u64 <= MAX_CONFIG_BYTES).then_some(text)
}

pub(super) fn job_requirement(plan: &FrozenPlan, step: &DiscoveryStep) -> Option<NotSplit> {
    let workflow = plan.workflows.iter().find(|w| w.file == step.workflow)?;
    let job = workflow.jobs.iter().find(|j| j.id == step.job)?;
    if job.steps.len() > step.step + 1 {
        // A post-test command or upload observes the whole job's workspace.
        return Some(NotSplit::InexactAggregation);
    }
    if let Some(reason) = workflow
        .env
        .iter()
        .chain(&job.env)
        .chain(&job.steps[step.step].env)
        .find_map(|(key, value)| reported(key).or_else(|| reported(value)))
    {
        return Some(reason);
    }
    None
}

pub(super) fn reason(
    plan: &FrozenPlan, step: &DiscoveryStep, workspace: &Path,root: &Path,info: &local_ci::shard::RunnerInfo,
) -> Option<NotSplit> {
    if let Some(reason) = job_requirement(plan,step) {return Some(reason);}
    if info.runner == "cargo-test" {
        return None;
    }
    let names: &[&str] = match info.runner.as_str() {
        "jest" => &["jest.config."],
        "vitest" => &["vitest.config.", "vitest.workspace.", "vite.config."],
        "playwright" => &["playwright.config."],
        "cypress" => &["cypress.config."],
        _ => &[],
    };
    let mut files = std::collections::BTreeSet::new();
    for dir in root.ancestors().take_while(|d| d.starts_with(workspace)) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Some(NotSplit::InexactAggregation);
        };
        for (index, entry) in entries.enumerate() {
            let Ok(entry) = entry else {
                return Some(NotSplit::InexactAggregation);
            };
            if index >= 4096 {
                return Some(NotSplit::InexactAggregation);
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if names.iter().any(|prefix| name.starts_with(prefix)) {
                files.insert(entry.path());
            } else if name == "package.json" {
                let Some(text) = read_config(workspace, &entry.path()) else {
                    return Some(NotSplit::InexactAggregation);
                };
                let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
                    return Some(NotSplit::InexactAggregation);
                };
                if let Some(why) = json
                    .get(info.runner.as_str())
                    .and_then(|v| reported(&v.to_string()))
                {
                    return Some(why);
                }
            }
        }
    }
    for (i, arg) in info.config.iter().enumerate() {
        let path = arg
            .strip_prefix("--config=")
            .or_else(|| arg.strip_prefix("--config-file="))
            .or_else(|| {
                matches!(arg.as_str(), "--config" | "--config-file" | "-c")
                    .then(|| info.config.get(i + 1).map(String::as_str))
                    .flatten()
            });
        if let Some(path) = path {
            files.insert(root.join(path));
        }
    }
    files
        .iter()
        .find_map(|path| match read_config(workspace, path) {
            Some(text) => reported(&text).or_else(|| {
                // An imported or inherited project configuration can install a
                // reporter whose output cannot be seen from this file.
                ["from './", "from "./", "from '../", "from "../", "require(", "import(", "...", "extends:"].iter()
                    .any(|token| text.contains(token)).then_some(NotSplit::InexactAggregation)
            }),
            None => Some(NotSplit::InexactAggregation),
        })
}
