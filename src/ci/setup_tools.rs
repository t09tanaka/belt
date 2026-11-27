//! The tools the setup steps of a task's jobs install on GitHub Actions
//! (spec 22.1): `taiki-e/install-action` installs executables, and
//! `components:` of `dtolnay/rust-toolchain` adds rust components. local-ci
//! skips both steps, so the job finds these only when the worker already
//! has them: each is a requirement of the task, never silently ignored.
use crate::toolchain::capability::{component_name, path_tool_name, Requirement};
use local_ci::{dag::ExecutionTask, wire::StepSpec, FrozenPlan};

/// What the setup steps of one task require, and one line for each input
/// that is no requirement.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SetupTools {
    pub requires: Vec<Requirement>,
    pub notices: Vec<String>,
}

/// The setup tools of `task`: the steps of its own jobs in `plan`.
pub fn of_task(plan: &FrozenPlan, task: &ExecutionTask) -> SetupTools {
    let matrix = |key: &str| -> Option<String> {
        match task.variant.as_ref()?.values.get(key)? {
            serde_json::Value::String(s) => Some(s.clone()),
            _ => None,
        }
    };
    let mut out = SetupTools::default();
    let jobs = plan
        .workflows
        .iter()
        .filter(|w| w.file == task.workflow)
        .flat_map(|w| &w.jobs)
        .filter(|j| task.jobs.contains(&j.id));
    for job in jobs {
        for step in &job.steps {
            let at = format!("{} job {}", task.workflow, job.id);
            of_step(step, &matrix, &at, &mut out);
        }
    }
    out
}

/// The tools the setup steps of `task`'s own jobs set up (`node` for
/// `actions/setup-node`, `flutter`, `ruby`, ...), whatever their version
/// inputs say: on GitHub Actions the step sets the tool up even when belt
/// cannot read the version it asks for. These decide which presets the
/// task's workspace preparation runs (spec 22.1).
pub fn set_up_tools(plan: &FrozenPlan, task: &ExecutionTask) -> std::collections::BTreeSet<String> {
    let matrix = |key: &str| -> Option<String> {
        match task.variant.as_ref()?.values.get(key)? {
            serde_json::Value::String(s) => Some(s.clone()),
            _ => None,
        }
    };
    plan.workflows
        .iter()
        .filter(|w| w.file == task.workflow)
        .flat_map(|w| &w.jobs)
        .filter(|j| task.jobs.contains(&j.id))
        .flat_map(|j| &j.steps)
        .flat_map(|s| {
            let action = s
                .uses
                .as_deref()
                .map(|uses| uses.split_once('@').map_or(uses, |(a, _)| a))
                .and_then(crate::toolchain::declarations::setup_action_tool);
            let run = s
                .run
                .as_deref()
                .map(|script| run_programs(&expand_matrix(script, &matrix)))
                .unwrap_or_default();
            action.into_iter().chain(run)
        })
        .map(str::to_string)
        .collect()
}

/// `script` with each `${{ matrix.<key> }}` replaced by the task's matrix
/// value, as [`of_task`] reads step inputs; any other expression stays.
fn expand_matrix(script: &str, matrix: &dyn Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(script.len());
    let mut rest = script;
    while let Some(start) = rest.find("${{") {
        let Some(len) = rest[start..].find("}}") else {
            break;
        };
        let expression = &rest[start..start + len + 2];
        out.push_str(&rest[..start]);
        match literal(expression, matrix) {
            Some(value) => out.push_str(&value),
            None => out.push_str(expression),
        }
        rest = &rest[start + len + 2..];
    }
    out.push_str(rest);
    out
}

/// The preset programs (`npm`, `npx`, `node`, `dart`, `flutter`, `ruby`,
/// `bundle`) a `run:` script runs as a command: a job whose only step is
/// `run: npm run lint` uses npm, so its preparation runs the npm preset
/// (spec 22.1). A simple tokenizer, not a shell parser: a command starts the
/// script and follows each `;`, `|`, `&`, `(`, `)` and line break, after
/// reserved words (`if`, `then`, `do`, `!`, `{`, ...), `NAME=value`
/// assignments and the prefix commands `time`, `env`, `exec`, `command`
/// (and their `-` options). Programs a script, a `make` target or a
/// composite action runs are not seen.
fn run_programs(script: &str) -> Vec<&'static str> {
    const RESERVED: &[&str] = &[
        "if", "then", "else", "elif", "fi", "do", "done", "while", "until", "!", "{", "}", "time",
        "env", "exec", "command", "nohup",
    ];
    let spaced: String = script
        .chars()
        .flat_map(|c| match c {
            '\n' | ';' | '|' | '&' | '(' | ')' => [' ', ';', ' '],
            c => [c, '\0', '\0'],
        })
        .filter(|c| *c != '\0')
        .collect();
    let mut out = Vec::new();
    let mut start = true;
    let mut prefixed = false;
    for word in spaced.split_whitespace() {
        if word == ";" {
            start = true;
            prefixed = false;
            continue;
        }
        if !start {
            continue;
        }
        if RESERVED.contains(&word) {
            prefixed = true;
            continue;
        }
        if (prefixed && word.starts_with('-'))
            || word.split_once('=').is_some_and(|(n, _)| {
                !n.is_empty() && n.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
            })
        {
            continue;
        }
        start = false;
        if let Some(tool) = crate::workspace::prepare_tools::program_of(word)
            .and_then(crate::workspace::prepare_tools::preset_program)
        {
            out.push(tool);
        }
    }
    out
}

/// The workspace preparation of `task` (spec 22.1): the presets its setup
/// steps set up, or the run's `--requires` tools name, are prepared, and
/// every other preset is skipped. Its tools are the task's
/// [`Requirement::Prepare`] and its skip set the worker's `--skip-preset`:
/// the parent's checks, the owner's, placement and the worker's preparation
/// all read this one result.
pub fn task_preparation(
    prepare: &crate::workspace::prepare_tools::PrepareTools,
    plan: &FrozenPlan,
    task: &ExecutionTask,
    configured: &crate::toolchain::capability::ExecutionRequirements,
) -> crate::workspace::prepare_tools::TaskPreparation {
    let mut set_up = set_up_tools(plan, task);
    set_up.extend(configured.requires.iter().filter_map(|r| match r {
        Requirement::Tool(name) => Some(name.clone()),
        _ => None,
    }));
    prepare.for_task(&set_up)
}

/// A reference of `taiki-e/install-action` that names no tool: a major
/// version tag (`v2`), a branch, or a commit.
fn names_no_tool(reference: &str) -> bool {
    let tag = reference.strip_prefix('v').is_some_and(|rest| {
        !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit() || b == b'.')
    });
    let sha = reference.len() == 40 && reference.bytes().all(|b| b.is_ascii_hexdigit());
    tag || sha || reference.is_empty() || matches!(reference, "main" | "master")
}

/// A value as written, or the matrix value of `${{ matrix.<key> }}`; `None`
/// for any other expression.
fn literal(value: &str, matrix: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let trimmed = value.trim();
    if let Some(inner) = trimmed
        .strip_prefix("${{")
        .and_then(|rest| rest.strip_suffix("}}"))
    {
        return matrix(inner.trim().strip_prefix("matrix.")?.trim());
    }
    (!trimmed.contains("${{")).then(|| trimmed.to_string())
}

fn push(out: &mut SetupTools, requirement: Requirement) {
    if !out.requires.contains(&requirement) {
        out.requires.push(requirement);
    }
}

fn notice(out: &mut SetupTools, text: String) {
    if !out.notices.contains(&text) {
        out.notices.push(text);
    }
}

fn of_step(
    step: &StepSpec,
    matrix: &dyn Fn(&str) -> Option<String>,
    at: &str,
    out: &mut SetupTools,
) {
    let Some(uses) = step.uses.as_deref() else {
        return;
    };
    let (action, reference) = uses.split_once('@').unwrap_or((uses, ""));
    let with = |key: &str| {
        step.with
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    match action {
        "taiki-e/install-action" => {
            let written = match with("tool") {
                Some(tool) => tool.to_string(),
                None if names_no_tool(reference) => {
                    return notice(
                        out,
                        format!("{at}: taiki-e/install-action@{reference} names no tool; nothing is required from it"),
                    )
                }
                None => reference.to_string(),
            };
            let Some(list) = literal(&written, matrix) else {
                return notice(
                    out,
                    format!("{at}: taiki-e/install-action tool `{}` is an expression belt does not read; nothing is required from it", short(&written)),
                );
            };
            for item in list.split(',').map(str::trim).filter(|i| !i.is_empty()) {
                let (name, version) = item.split_once('@').unwrap_or((item, ""));
                let name = if name == "nextest" {
                    "cargo-nextest"
                } else {
                    name
                };
                if !path_tool_name(name) {
                    notice(
                        out,
                        format!("{at}: taiki-e/install-action tool `{}` is not one a worker reports; nothing is required from it", short(item)),
                    );
                    continue;
                }
                if !version.is_empty() && version != "latest" {
                    notice(
                        out,
                        format!(
                            "{at}: {name}@{} is required by name only; its version is not checked",
                            short(version)
                        ),
                    );
                }
                push(out, Requirement::Setup(name.to_string()));
            }
        }
        "dtolnay/rust-toolchain" | "actions-rust-lang/setup-rust-toolchain" => {
            let Some(written) = with("components") else {
                return;
            };
            let Some(list) = literal(written, matrix) else {
                return notice(
                    out,
                    format!("{at}: {action} components `{}` is an expression belt does not read; nothing is required from it", short(written)),
                );
            };
            for item in list
                .split(|c: char| c == ',' || c.is_whitespace())
                .filter(|i| !i.is_empty())
            {
                let name = component_name(item, None);
                let requirement = Requirement::Component(name);
                if requirement.validate() {
                    push(out, requirement);
                } else {
                    notice(
                        out,
                        format!("{at}: {action} component `{}` is not a component name; nothing is required from it", short(item)),
                    );
                }
            }
        }
        _ => {}
    }
}

fn short(value: &str) -> String {
    value.chars().take(80).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_run_script_names_the_preset_programs_its_commands_start_with() {
        assert_eq!(run_programs("npm run lint"), ["npm"]);
        assert_eq!(
            run_programs("cd web && CI=1 npx tsc\nflutter test | tee log; echo npm"),
            ["npm", "flutter"]
        );
        assert!(run_programs("cargo test").is_empty());
        assert!(run_programs("echo npm; cargo test --features node").is_empty());
        for script in [
            "if true; then npm ci; fi",
            "for d in a b; do npm test; done",
            "while false; do :; done; until npm test; do :; done",
            "! npm test",
            "{ npm test; }",
            "(cd web && npm test)",
            "time npm test",
            "env -i CI=1 NODE_ENV=test npm test",
            "exec npm test",
            "command -v npm && npm test",
            "CI=1 npm test",
            "cd web\nnpm test",
            "make lint || npx eslint .",
        ] {
            assert!(run_programs(script).contains(&"npm"), "{script}");
        }
        assert!(run_programs("make lint && ./scripts/test.sh").is_empty());
    }

    #[test]
    fn a_run_script_is_read_with_its_matrix_values() {
        let matrix = |key: &str| (key == "tool").then(|| "npm".to_string());
        let script = expand_matrix("${{ matrix.tool }} test ${{ github.sha }}", &matrix);
        assert_eq!(script, "npm test ${{ github.sha }}");
        assert_eq!(run_programs(&script), ["npm"]);
    }

    fn step(uses: &str, with: &[(&str, &str)]) -> StepSpec {
        StepSpec {
            id: None,
            name: None,
            uses: Some(uses.into()),
            run: None,
            with: with
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            env: Vec::new(),
            cond: None,
            working_directory: None,
            shell: None,
            continue_on_error: false,
            timeout_minutes: None,
        }
    }

    fn read(steps: &[StepSpec]) -> SetupTools {
        let matrix = |key: &str| (key == "tool").then(|| "cargo-hack".to_string());
        let mut out = SetupTools::default();
        for s in steps {
            of_step(s, &matrix, "ci.yml job coverage", &mut out);
        }
        out
    }

    fn setup(names: &[&str]) -> Vec<Requirement> {
        names
            .iter()
            .map(|n| Requirement::Setup(n.to_string()))
            .collect()
    }

    /// The run5 coverage job: the reference names the tool, and the
    /// component of the toolchain step is required without `-preview`.
    #[test]
    fn the_coverage_job_requires_cargo_llvm_cov_and_llvm_tools() {
        let got = read(&[
            step(
                "dtolnay/rust-toolchain@stable",
                &[("components", "llvm-tools-preview")],
            ),
            step("taiki-e/install-action@cargo-llvm-cov", &[]),
            step("actions/checkout@v4", &[]),
            // Said twice, required and told once; a run step adds nothing.
            step("taiki-e/install-action@cargo-llvm-cov", &[]),
            StepSpec {
                uses: None,
                run: Some("cargo llvm-cov".into()),
                ..step("", &[])
            },
        ]);
        let twice = read(&[
            step("taiki-e/install-action@v2", &[]),
            step("taiki-e/install-action@v2", &[]),
        ]);
        assert_eq!(twice.notices.len(), 1, "{:?}", twice.notices);
        assert_eq!(
            got.requires,
            vec![
                Requirement::Component("llvm-tools".into()),
                Requirement::Setup("cargo-llvm-cov".into()),
            ]
        );
        assert!(got.notices.is_empty(), "{:?}", got.notices);
    }

    #[test]
    fn the_tool_input_lists_tools_and_wins_over_the_reference() {
        let got = read(&[
            step(
                "taiki-e/install-action@v2",
                &[("tool", "nextest, cargo-hack@0.6.30,just")],
            ),
            step(
                "taiki-e/install-action@cargo-deny",
                &[("tool", "cargo-deny@latest")],
            ),
        ]);
        assert_eq!(
            got.requires,
            setup(&["cargo-nextest", "cargo-hack", "just", "cargo-deny"])
        );
        assert_eq!(got.notices.len(), 1, "{:?}", got.notices);
        assert!(got.notices[0].contains("cargo-hack@0.6.30"));
        assert!(got.notices[0].contains("version is not checked"));
    }

    #[test]
    fn a_reference_that_names_no_tool_and_an_unread_input_are_told() {
        let sha = "0123456789abcdef0123456789abcdef01234567";
        for reference in ["v2", "v2.1", "main", "master", sha, ""] {
            let uses = format!("taiki-e/install-action@{reference}");
            let got = read(&[step(&uses, &[])]);
            assert!(got.requires.is_empty(), "{reference}");
            assert!(got.notices[0].contains("names no tool"), "{reference}");
        }
        let got = read(&[
            step("taiki-e/install-action@v2", &[("tool", "${{ env.TOOLS }}")]),
            step(
                "taiki-e/install-action@v2",
                &[("tool", "${{ matrix.tool }}")],
            ),
            step(
                "taiki-e/install-action@v2",
                &[("tool", "protoc,some-other")],
            ),
            step(
                "dtolnay/rust-toolchain@stable",
                &[("components", "${{ env.C }}")],
            ),
            step(
                "dtolnay/rust-toolchain@stable",
                &[("components", "clippy rustfmt, bad/name")],
            ),
        ]);
        assert_eq!(
            got.requires,
            vec![
                Requirement::Setup("cargo-hack".into()),
                Requirement::Setup("protoc".into()),
                Requirement::Component("clippy".into()),
                Requirement::Component("rustfmt".into()),
            ]
        );
        let text = got.notices.join("\n");
        assert_eq!(got.notices.len(), 4, "{text}");
        assert!(
            text.contains("`${{ env.TOOLS }}` is an expression"),
            "{text}"
        );
        assert!(
            text.contains("`some-other` is not one a worker reports"),
            "{text}"
        );
        assert!(text.contains("components `${{ env.C }}`"), "{text}");
        assert!(text.contains("`bad/name` is not a component"), "{text}");
    }

    /// Each leg reads its own matrix value; a value that is no string is an
    /// expression; the other job and workflow add nothing.
    #[test]
    fn a_task_reads_the_steps_of_its_own_jobs_with_its_matrix_values() {
        const WORKFLOW: &str = "on: push\njobs:\n  lint:\n    runs-on: ubuntu-latest\n    strategy:\n      matrix:\n        tool: [cargo-hack, 7]\n    steps:\n      - uses: taiki-e/install-action@v2\n        with:\n          tool: ${{ matrix.tool }}\n  other:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: taiki-e/install-action@just\n";
        let f = crate::ci::execute::tests::fixture(WORKFLOW);
        let mut seen = Vec::new();
        for task in f
            .plan
            .tasks
            .iter()
            .filter(|t| t.jobs.contains(&"lint".to_string()))
        {
            seen.push(of_task(&f.plan, task));
        }
        assert_eq!(seen.len(), 2, "{:?}", f.plan.tasks);
        let hack = SetupTools {
            requires: vec![Requirement::Setup("cargo-hack".into())],
            notices: Vec::new(),
        };
        assert!(seen.contains(&hack), "{seen:?}");
        let number = seen.iter().find(|s| *s != &hack).unwrap();
        assert!(number.requires.is_empty());
        assert!(number.notices[0].contains("job lint"), "{number:?}");
        assert!(number.notices[0].contains("is an expression"), "{number:?}");
        let other = f
            .plan
            .tasks
            .iter()
            .find(|t| t.jobs.contains(&"other".to_string()));
        let other = of_task(&f.plan, other.unwrap());
        assert_eq!(other.requires, vec![Requirement::Setup("just".into())]);
    }

    #[test]
    fn a_tag_with_letters_after_v_is_a_tool_name_and_other_actions_require_nothing() {
        assert!(!names_no_tool("v2x"));
        assert!(!names_no_tool("v"));
        assert!(!names_no_tool("cargo-llvm-cov"));
        assert!(!names_no_tool("0123456789abcdef0123456789abcdef0123456"));
        let got = read(&[
            step("actions/setup-node@v4", &[("tool", "cargo-x")]),
            step("dtolnay/rust-toolchain@stable", &[]),
            step(
                "actions-rust-lang/setup-rust-toolchain@v1",
                &[("components", "miri")],
            ),
        ]);
        assert_eq!(got.requires, vec![Requirement::Component("miri".into())]);
        assert!(got.notices.is_empty());
    }
}
