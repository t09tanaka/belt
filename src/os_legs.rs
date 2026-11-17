//! Which operating system a `runs-on` names, and which legs of a matrix job
//! this host can stand in for.
//!
//! local-ci runs every job on the host. A matrix over `os: [ubuntu-latest,
//! macos-latest]` therefore runs the same steps twice on one machine. When the
//! legs differ only in the OS label, the leg of the host's own OS family is run
//! and the others are reported as not covered: the run does not claim that
//! Linux was checked on a Mac.

use crate::workflow::Job;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Linux,
    MacOs,
    Windows,
}

impl Family {
    /// The OS family local-ci is running on.
    pub fn host() -> Family {
        if cfg!(target_os = "macos") {
            Family::MacOs
        } else if cfg!(windows) {
            Family::Windows
        } else {
            Family::Linux
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Family::Linux => "Linux",
            Family::MacOs => "macOS",
            Family::Windows => "Windows",
        }
    }
}

/// The family a single `runs-on` label stands for. `None` for anything else
/// (a self-hosted runner's own labels, an unknown name).
pub fn family_of_label(label: &str) -> Option<Family> {
    let l = label.trim().to_ascii_lowercase();
    if l == "linux" || l.starts_with("ubuntu-") {
        Some(Family::Linux)
    } else if l == "macos" || l.starts_with("macos-") {
        Some(Family::MacOs)
    } else if l == "windows" || l.starts_with("windows-") {
        Some(Family::Windows)
    } else {
        None
    }
}

/// The labels of an evaluated `runs-on` value: a string, a list of strings, or
/// a mapping with `labels`.
fn labels(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => vec![s.clone()],
        Value::Array(a) => a.iter().flat_map(labels).collect(),
        Value::Object(m) => m.get("labels").map(labels).unwrap_or_default(),
        _ => vec![],
    }
}

/// The family an evaluated `runs-on` names, with the label that says so.
/// A self-hosted runner is `None` (it is run as before).
pub fn family_of_runs_on(v: &Value) -> Option<(Family, String)> {
    let ls = labels(v);
    if ls.iter().any(|l| l.eq_ignore_ascii_case("self-hosted")) {
        return None;
    }
    ls.iter()
        .find_map(|l| family_of_label(l).map(|f| (f, l.clone())))
}

/// The family the `runs-on` of `job` names with the contexts `contexts` of a
/// leg (at least `matrix`), with the label that says so. `None` when it names
/// none, or cannot be evaluated.
pub fn family_of_leg(job: &Job, contexts: &Map<String, Value>) -> Option<(Family, String)> {
    let y = job.runs_on.as_ref()?;
    let ctx = crate::expr::Ctx::new(contexts, crate::expr::Status::Success);
    let v = crate::job_evaluate::eval_strings(&crate::matrix::yaml_to_json(y), &ctx).ok()?;
    family_of_runs_on(&v)
}

/// How many of the legs `combos` of a matrix job run on this host: the legs
/// another leg of the host's OS stands in for do not (see [`twins`]).
pub fn running_legs(host: Family, job: &Job, combos: &[Map<String, Value>]) -> usize {
    let legs: Vec<Leg> = combos
        .iter()
        .map(|c| {
            let mut cx = Map::new();
            cx.insert("matrix".into(), Value::Object(c.clone()));
            Leg {
                family: family_of_leg(job, &cx).map(|(f, _)| f),
                matrix: c.clone(),
            }
        })
        .collect();
    twins(host, job, &legs)
        .iter()
        .filter(|t| t.is_none())
        .count()
}

/// One leg of a matrix job.
#[derive(Debug, Clone)]
pub struct Leg {
    pub family: Option<Family>,
    pub matrix: Map<String, Value>,
}

/// The matrix keys whose values differ between two legs, when that is all that
/// differs and every such value is an OS label. `None` when the legs differ in
/// anything else (a key one lacks, another kind of value).
fn os_keys_differing(a: &Map<String, Value>, b: &Map<String, Value>) -> Option<Vec<String>> {
    if a.len() != b.len() || a.keys().any(|k| !b.contains_key(k)) {
        return None;
    }
    let mut keys = Vec::new();
    for (k, av) in a {
        let bv = &b[k];
        if av == bv {
            continue;
        }
        let is_os = |v: &Value| v.as_str().is_some_and(|s| family_of_label(s).is_some());
        if !(is_os(av) && is_os(bv)) {
            return None;
        }
        keys.push(k.clone());
    }
    (!keys.is_empty()).then_some(keys)
}

fn ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Whether the expression `e` reads one of `keys` of the matrix, or the matrix
/// as a whole (`matrix[...]`, `toJSON(matrix)`), or the index of the leg.
fn expr_reads(e: &str, keys: &[String]) -> bool {
    let e = e.to_ascii_lowercase();
    if e.contains("strategy.job-") {
        return true;
    }
    // `runner.os`, `runner.arch`, `runner.name` and the rest are the OS label
    // the leg runs on, which the host does not reproduce
    let mut at = 0;
    while let Some(p) = e[at..].find("runner") {
        let i = at + p;
        at = i + "runner".len();
        let before = e[..i].chars().next_back();
        if !before.is_some_and(|c| ident(c) || c == '.') && e[at..].starts_with('.') {
            return true;
        }
    }
    let mut from = 0;
    while let Some(p) = e[from..].find("matrix") {
        let at = from + p;
        from = at + "matrix".len();
        let before = e[..at].chars().next_back();
        if before.is_some_and(|c| ident(c) || c == '.') {
            continue;
        }
        let after = &e[from..];
        if after.chars().next().is_some_and(ident) {
            continue;
        }
        match after.strip_prefix('.') {
            Some(rest) => {
                let name: String = rest.chars().take_while(|c| ident(*c)).collect();
                if name.is_empty() || keys.iter().any(|k| k.to_ascii_lowercase() == name) {
                    return true;
                }
            }
            None => return true,
        }
    }
    false
}

/// Whether anything in `v` that is evaluated reads one of `keys`. Names are
/// labels only and left out. `cond` strings are expressions as they are.
fn reads(v: &Value, key: &str, keys: &[String]) -> bool {
    match v {
        Value::String(s) => {
            if key == "name" {
                return false;
            }
            let mut rest = s.as_str();
            let mut any = false;
            while let Some(i) = rest.find("${{") {
                let after = &rest[i + 3..];
                let end = after.find("}}").unwrap_or(after.len());
                any |= expr_reads(&after[..end], keys);
                rest = &after[end..];
            }
            any || (key == "cond" && !s.contains("${{") && expr_reads(s, keys))
        }
        Value::Array(a) => a.iter().any(|x| reads(x, key, keys)),
        Value::Object(m) => m.iter().any(|(k, x)| reads(x, k, keys)),
        _ => false,
    }
}

/// What a leg's steps, environment and outputs are made of, without `runs-on`
/// and the names.
fn evaluated_parts(job: &Job) -> Value {
    json!({
        "cond": job.cond,
        "env": job.env,
        "working_directory": job.working_directory,
        "shell": job.shell,
        "services": job.services,
        "steps": job.steps,
        "outputs": job.outputs,
        "timeout_minutes": job.timeout_minutes,
        "continue_on_error": job.continue_on_error,
        "container": job.container,
        "job_exprs": job.job_exprs,
    })
}

/// For each leg, the index of the leg of the host's family that runs the same
/// steps, when this host stands in for it: it has another OS family than the
/// host, and a leg of the host's family differs from it only in OS-label
/// values that nothing in the job reads.
pub fn twins(host: Family, job: &Job, legs: &[Leg]) -> Vec<Option<usize>> {
    let parts = evaluated_parts(job);
    legs.iter()
        .map(|leg| {
            if !leg.family.is_some_and(|f| f != host) {
                return None;
            }
            legs.iter().enumerate().find_map(|(j, other)| {
                if other.family != Some(host) {
                    return None;
                }
                let keys = os_keys_differing(&leg.matrix, &other.matrix)?;
                (!reads(&parts, "", &keys)).then_some(j)
            })
        })
        .collect()
}

/// Why a leg was not run: shown under `not covered`.
pub fn not_run_reason(label: &str, family: Family, host: Family, twin: &str) -> String {
    format!(
        "runs-on {label}: {} is not reproduced on this {} host; the same steps ran as {twin}",
        family.name(),
        host.name()
    )
}

/// Whether a job's skip reason is a leg that was left out for this.
pub(crate) fn is_not_run_reason(reason: &str) -> bool {
    reason.starts_with("runs-on ") && reason.contains(" is not reproduced on this ")
}

/// The note on a leg that ran on the host although its label names another OS.
pub fn ran_on_note(label: &str, family: Family, host: Family) -> String {
    format!(
        "ran on {}, not {} (runs-on {label})",
        host.name(),
        family.name()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn job(yaml: &str) -> Job {
        let w = crate::workflow::parse(
            "w.yml",
            &format!("on: push\njobs:\n  t:\n    runs-on: x\n{yaml}"),
        )
        .unwrap();
        w.jobs.into_iter().next().unwrap()
    }

    fn leg(label: &str, matrix: Value) -> Leg {
        Leg {
            family: family_of_label(label),
            matrix: m(matrix),
        }
    }

    #[test]
    fn labels_map_to_families() {
        for (l, f) in [
            ("ubuntu-latest", Some(Family::Linux)),
            ("ubuntu-22.04", Some(Family::Linux)),
            ("ubuntu-24.04-arm", Some(Family::Linux)),
            ("linux", Some(Family::Linux)),
            ("macos-latest", Some(Family::MacOs)),
            ("macos-14-xlarge", Some(Family::MacOs)),
            ("windows-latest", Some(Family::Windows)),
            ("Windows-2022", Some(Family::Windows)),
            ("self-hosted", None),
            ("my-runner", None),
            ("ubuntu", None),
        ] {
            assert_eq!(family_of_label(l), f, "{l}");
        }
    }

    #[test]
    fn runs_on_shapes() {
        let fam = |v: Value| family_of_runs_on(&v);
        assert_eq!(
            fam(json!("ubuntu-latest")),
            Some((Family::Linux, "ubuntu-latest".into()))
        );
        assert_eq!(
            fam(json!(["macos-14", "big"])),
            Some((Family::MacOs, "macos-14".into()))
        );
        assert_eq!(
            fam(json!({"group": "g", "labels": ["windows-latest"]})),
            Some((Family::Windows, "windows-latest".into()))
        );
        assert_eq!(fam(json!(["self-hosted", "linux"])), None);
        assert_eq!(fam(json!({"group": "g"})), None);
        assert_eq!(fam(json!("custom")), None);
        assert_eq!(fam(json!(null)), None);
    }

    #[test]
    fn legs_that_differ_only_in_the_os_are_twins() {
        let j = job("    steps:\n      - run: echo ${{ matrix.node }}\n");
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest", "node": 20})),
            leg("macos-latest", json!({"os": "macos-latest", "node": 20})),
        ];
        assert_eq!(twins(Family::MacOs, &j, &legs), vec![Some(1), None]);
        assert_eq!(twins(Family::Linux, &j, &legs), vec![None, Some(0)]);
        // a host with no leg of its family has no twin to stand in
        assert_eq!(twins(Family::Windows, &j, &legs), vec![None, None]);
    }

    #[test]
    fn another_matrix_value_means_no_twin() {
        let j = job("    steps:\n      - run: echo hi\n");
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest", "node": 18})),
            leg("macos-latest", json!({"os": "macos-latest", "node": 20})),
        ];
        assert_eq!(twins(Family::MacOs, &j, &legs), vec![None, None]);
        // a key one leg lacks (an `include` entry)
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest", "extra": 1})),
            leg("macos-latest", json!({"os": "macos-latest"})),
        ];
        assert_eq!(twins(Family::MacOs, &j, &legs), vec![None, None]);
    }

    #[test]
    fn steps_that_read_the_os_mean_no_twin() {
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest"})),
            leg("macos-latest", json!({"os": "macos-latest"})),
        ];
        for steps in [
            "    steps:\n      - run: echo ${{ matrix.os }}\n",
            "    steps:\n      - run: echo ${{ MATRIX.OS }}\n",
            "    steps:\n      - run: echo hi\n        if: matrix.os == 'macos-latest'\n",
            "    steps:\n      - run: echo hi\n        with:\n          a: ${{ toJSON(matrix) }}\n",
            "    steps:\n      - run: echo hi\n        env:\n          A: ${{ matrix['os'] }}\n",
            "    steps:\n      - run: echo hi\n    env:\n      A: ${{ strategy.job-index }}\n",
            "    if: matrix.os != 'x'\n    steps:\n      - run: echo hi\n",
        ] {
            assert_eq!(
                twins(Family::MacOs, &job(steps), &legs),
                vec![None, None],
                "{steps}"
            );
        }
        // names are labels only; another matrix key or a longer name is not the os
        let ok = "    steps:\n      - name: On ${{ matrix.os }}\n        run: echo ${{ matrix.os_version }} matrix.os\n";
        let legs2 = [
            leg(
                "ubuntu-latest",
                json!({"os": "ubuntu-latest", "os_version": 1}),
            ),
            leg(
                "macos-latest",
                json!({"os": "macos-latest", "os_version": 1}),
            ),
        ];
        assert_eq!(twins(Family::MacOs, &job(ok), &legs2), vec![Some(1), None]);
    }

    #[test]
    fn steps_that_read_the_runner_mean_no_twin() {
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest"})),
            leg("macos-latest", json!({"os": "macos-latest"})),
        ];
        for steps in [
            "    steps:\n      - run: echo hi\n        if: runner.os == 'Linux'\n",
            "    steps:\n      - run: echo ${{ runner.arch }}\n",
            "    steps:\n      - run: echo hi\n        env:\n          A: ${{ RUNNER.NAME }}\n",
        ] {
            assert_eq!(
                twins(Family::MacOs, &job(steps), &legs),
                vec![None, None],
                "{steps}"
            );
        }
    }

    #[test]
    fn job_settings_that_read_the_os_mean_no_twin() {
        let legs = [
            leg("ubuntu-latest", json!({"os": "ubuntu-latest"})),
            leg("macos-latest", json!({"os": "macos-latest"})),
        ];
        for head in [
            "    continue-on-error: ${{ matrix.os == 'macos-latest' }}\n",
            "    timeout-minutes: ${{ matrix.os == 'macos-latest' && 5 || 10 }}\n",
            "    container: ${{ matrix.os }}\n",
        ] {
            let y = format!("{head}    steps:\n      - run: echo hi\n");
            assert_eq!(
                twins(Family::MacOs, &job(&y), &legs),
                vec![None, None],
                "{y}"
            );
        }
        let y = "    continue-on-error: true\n    timeout-minutes: 5\n    steps:\n      - run: echo hi\n";
        assert_eq!(twins(Family::MacOs, &job(y), &legs), vec![Some(1), None]);
    }

    #[test]
    fn reading_the_matrix_is_told_from_names_that_only_look_like_it() {
        let keys = ["os".to_string()];
        for (e, reads) in [
            ("matrix.os", true),
            (" matrix.os == 'a' ", true),
            ("matrix.OS", true),
            ("matrix", true),
            ("matrix[0]", true),
            ("matrix.", true),
            ("toJSON(matrix)", true),
            ("matrix.node == 1 && matrix.os", true),
            ("strategy.job-index", true),
            ("matrix.node", false),
            ("matrix.os_version", false),
            ("matrix.os-version", false),
            ("env.matrix.os", false),
            ("mymatrix.os", false),
            ("matrixes.os", false),
            ("github.sha", false),
            ("", false),
        ] {
            assert_eq!(expr_reads(e, &keys), reads, "{e}");
        }
        // every expression of a string counts, and a cond is one as it is
        let v = json!({"run": "a ${{ matrix.node }} b ${{ matrix.os }}", "cond": "x"});
        assert!(reads(&v, "", &keys));
        let v = json!({"run": "a ${{ matrix.node }} b ${{ github.sha }} c ${{ env.A", "cond": "success()"});
        assert!(!reads(&v, "", &keys));
        assert!(reads(&json!({"cond": "matrix.os"}), "", &keys));
        // a bare string is not an expression unless it is a cond
        assert!(!reads(&json!({"run": "matrix.os"}), "", &keys));
        assert!(!reads(&json!({"name": "${{ matrix.os }}"}), "", &keys));
    }

    #[test]
    fn running_legs_count_what_runs_here() {
        let j = crate::workflow::parse(
            "w.yml",
            "on: push\njobs:\n  t:\n    runs-on: ${{ matrix.os }}\n    steps:\n      - run: echo hi\n",
        )
        .unwrap()
        .jobs
        .remove(0);
        let combos = [
            m(json!({"os": "ubuntu-latest"})),
            m(json!({"os": "macos-latest"})),
            m(json!({"os": "windows-latest"})),
        ];
        assert_eq!(running_legs(Family::MacOs, &j, &combos), 1);
        assert_eq!(running_legs(Family::Linux, &j, &combos), 1);
        // no leg of the host's family: all of them run
        assert_eq!(running_legs(Family::MacOs, &j, &combos[..1]), 1);
        assert_eq!(running_legs(Family::Windows, &j, &combos[..2]), 2);
        // a leg that cannot be told is a leg that runs
        let mut j2 = j.clone();
        j2.runs_on = Some(serde_yaml::Value::String("${{ nope( }}".into()));
        assert_eq!(running_legs(Family::MacOs, &j2, &combos), 3);
        j2.runs_on = None;
        assert_eq!(running_legs(Family::MacOs, &j2, &combos), 3);
    }

    #[test]
    fn self_hosted_and_unknown_legs_are_never_skipped() {
        let j = job("    steps:\n      - run: echo hi\n");
        let legs = [
            Leg {
                family: None,
                matrix: m(json!({"os": "mine"})),
            },
            leg("macos-latest", json!({"os": "macos-latest"})),
        ];
        assert_eq!(twins(Family::MacOs, &j, &legs), vec![None, None]);
    }

    #[test]
    fn not_run_reasons_are_recognized() {
        let r = not_run_reason(
            "ubuntu-latest",
            Family::Linux,
            Family::MacOs,
            "test (macos-latest)",
        );
        assert_eq!(
            r,
            "runs-on ubuntu-latest: Linux is not reproduced on this macOS host; the same steps ran as test (macos-latest)"
        );
        assert!(is_not_run_reason(&r));
        assert!(!is_not_run_reason("if: false"));
        assert_eq!(
            ran_on_note("ubuntu-latest", Family::Linux, Family::MacOs),
            "ran on macOS, not Linux (runs-on ubuntu-latest)"
        );
    }
}
