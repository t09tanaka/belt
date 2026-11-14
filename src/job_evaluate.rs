//! Shared pure job evaluation extracted from Runner::run_job.
//!
//! Legacy worker evaluation preserves expansion before per-variant conditions.
//! Portable evaluation opts into early condition/context validation and bounded
//! expansion, while reusing the same expression/matrix/display implementation.
use crate::dag::TaskShard;
use crate::expr::{self, Ctx, Status};
use crate::matrix;
use crate::workflow::Job;
use anyhow::Result;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
pub(crate) const PORTABLE_OUTPUT_BYTES: usize = 1024 * 1024;
const PORTABLE_EVALUATION_BYTES: usize = 8 * 1024 * 1024;
fn bounded(ctx: Ctx<'_>, portable: bool) -> Ctx<'_> {
    if portable {
        ctx.with_output_limit(PORTABLE_OUTPUT_BYTES)
    } else {
        ctx
    }
}

#[derive(Debug)]
pub(crate) enum Decision {
    Run(Map<String, Value>),
    Skip(String),
    Error(String),
}
#[derive(Debug)]
pub(crate) struct EvaluatedEntry {
    pub display: String,
    pub decision: Decision,
}
#[derive(Debug)]
pub(crate) struct JobEvaluation {
    pub entries: Vec<EvaluatedEntry>,
}
impl JobEvaluation {
    fn terminal(display: String, decision: Decision) -> Self {
        Self {
            entries: vec![EvaluatedEntry { display, decision }],
        }
    }
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_job(
    job: &Job,
    github: Value,
    runner: Value,
    needs: &Map<String, Value>,
    ancestor_failed: bool,
    run_cancelled: bool,
    only: Option<usize>,
    shard: Option<&TaskShard>,
) -> JobEvaluation {
    evaluate_inner(
        job,
        github,
        runner,
        needs,
        ancestor_failed,
        run_cancelled,
        only,
        shard,
        None,
    )
}

pub(crate) fn evaluate_portable(
    job: &Job,
    contexts: &Map<String, Value>,
    needs: &Map<String, Value>,
    ancestor_failed: bool,
    run_cancelled: bool,
    limit: usize,
) -> JobEvaluation {
    evaluate_inner(
        job,
        contexts.get("github").cloned().unwrap_or(json!({})),
        json!({}),
        needs,
        ancestor_failed,
        run_cancelled,
        None,
        None,
        Some((contexts, limit)),
    )
}

#[allow(clippy::too_many_arguments)]
fn evaluate_inner(
    job: &Job,
    github: Value,
    runner: Value,
    needs: &Map<String, Value>,
    ancestor_failed: bool,
    run_cancelled: bool,
    only: Option<usize>,
    shard: Option<&TaskShard>,
    portable: Option<(&Map<String, Value>, usize)>,
) -> JobEvaluation {
    let base_name = job.name.clone().unwrap_or_else(|| job.id.clone());
    let needs_of_job: Map<String, Value> = job
        .needs
        .iter()
        .filter_map(|n| needs.get(n).map(|v| (n.clone(), v.clone())))
        .collect();
    // The status functions of a job, as on GitHub: `success()` when every
    // job it needs succeeded (a skipped one does not count), `failure()`
    // when one failed, or any job further up did, `cancelled()` when the
    // run was cancelled.
    let results: Vec<String> = needs_of_job
        .values()
        .map(|v| v["result"].as_str().unwrap_or("success").to_string())
        .collect();
    let any_result = |r: &str| results.iter().any(|x| x == r);
    let needs_fns = expr::StatusFns {
        success: results.iter().all(|r| r == "success"),
        failure: ancestor_failed || any_result("failure"),
        cancelled: run_cancelled,
    };
    // why a job that is not run by its `if:` is not run
    let why_not = || {
        if needs_fns.success || !expr::success_gated(job.cond.as_deref()) {
            "if: false"
        } else if any_result("failure") {
            "needs failed"
        } else if any_result("cancelled") {
            "needs cancelled"
        } else if any_result("skipped") {
            "needs skipped"
        } else {
            "if: false"
        }
    };
    let mut ctx_map = Map::new();
    ctx_map.insert("github".into(), github);
    ctx_map.insert("needs".into(), Value::Object(needs_of_job.clone()));
    ctx_map.insert("runner".into(), runner);
    ctx_map.insert("inputs".into(), json!({}));
    ctx_map.insert("vars".into(), json!({}));
    ctx_map.insert("env".into(), json!({}));
    ctx_map.insert("matrix".into(), json!({}));
    ctx_map.insert("strategy".into(), json!({}));

    if let Some((contexts, _)) = portable {
        for root in ["vars", "inputs"] {
            if let Some(value) = contexts.get(root) {
                ctx_map.insert(root.into(), value.clone());
            }
        }
        let check = || -> Result<bool> {
            if let Some(cond) = &job.cond {
                expr::validate_portable_context(
                    cond,
                    true,
                    expr::PortableStage::JobIf,
                    Some(&ctx_map),
                )?;
            }
            expr::condition(
                job.cond.as_deref(),
                &bounded(Ctx::with_fns(&ctx_map, needs_fns), true),
            )
        };
        match check() {
            Ok(false) => {
                return JobEvaluation::terminal(base_name, Decision::Skip(why_not().into()))
            }
            Err(e) => {
                return JobEvaluation::terminal(base_name, Decision::Error(format!("if: {e:#}")))
            }
            Ok(true) => {}
        }
        if job.max_parallel == Some(0) {
            return JobEvaluation::terminal(
                base_name,
                Decision::Error("strategy.max-parallel: must be positive".into()),
            );
        }
    }
    // The matrix may be an expression over `needs`.
    let combos = match &job.matrix {
        None => vec![Map::new()],
        Some(m) => {
            let ctx = bounded(Ctx::new(&ctx_map, Status::Success), portable.is_some());
            let raw = matrix::yaml_to_json(m);
            let value = if portable.is_some() {
                validate_strings(&raw, &ctx_map, expr::PortableStage::Matrix)
                    .and_then(|()| eval_strings(&raw, &ctx))
            } else {
                eval_strings(&raw, &ctx)
            };
            match value.and_then(|v| match portable {
                Some((_, 0)) => anyhow::bail!("matrix expansion data budget exhausted"),
                Some((_, limit)) => matrix::expand_portable(&v, limit, PORTABLE_OUTPUT_BYTES),
                None => matrix::expand(&v),
            }) {
                Ok(c) => c,
                Err(e) => {
                    return JobEvaluation::terminal(
                        base_name,
                        Decision::Error(format!("matrix: {e:#}")),
                    )
                }
            }
        }
    };
    if job.container {
        return JobEvaluation::terminal(
            base_name,
            Decision::Skip("container: is not supported".into()),
        );
    }
    if let Some(u) = &job.reusable {
        return JobEvaluation::terminal(
            base_name,
            Decision::Skip(format!("reusable workflow {u} is not supported")),
        );
    }

    if portable.is_some() {
        if let Some(name) = &job.name {
            // Matrix/strategy keys are validated for availability by stage;
            // actual values are supplied for each member below.
            if let Err(e) = expr::validate_portable_context(
                name,
                false,
                expr::PortableStage::Name,
                Some(&ctx_map),
            ) {
                return JobEvaluation::terminal(base_name, Decision::Error(format!("name: {e:#}")));
            }
        }
    }
    if portable.is_some() && expr::serialized_size(&job.name, PORTABLE_OUTPUT_BYTES).is_err() {
        return JobEvaluation::terminal(
            base_name,
            Decision::Error("name: byte budget exceeded".into()),
        );
    }
    // Combinations that pass `if:` run after this loop.
    let mut evaluated_bytes = 0usize;
    let mut entries = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let total = combos.len();
    for (idx, combo) in combos.into_iter().enumerate() {
        if portable.is_some() {
            let next =
                expr::serialized_size(&(&ctx_map, &combo, &job.name), PORTABLE_EVALUATION_BYTES)
                    .ok()
                    .and_then(|bytes| evaluated_bytes.checked_add(bytes));
            if next.is_none_or(|bytes| bytes > PORTABLE_EVALUATION_BYTES) {
                return JobEvaluation::terminal(
                    base_name,
                    Decision::Error("evaluation byte budget exceeded".into()),
                );
            }
        }
        let mut cm = ctx_map.clone();
        cm.insert("matrix".into(), Value::Object(combo.clone()));
        cm.insert(
            "strategy".into(),
            json!({"job-index": idx, "job-total": total}),
        );
        let mut display = {
            let ctx = bounded(Ctx::new(&cm, Status::Success), portable.is_some());
            match &job.name {
                Some(n) if n.contains("${{") => match expr::interpolate(n, &ctx) {
                    Ok(value) => value,
                    Err(e) if portable.is_some() => {
                        return JobEvaluation::terminal(
                            base_name,
                            Decision::Error(format!("name: {e:#}")),
                        )
                    }
                    Err(_) => n.clone(),
                },
                Some(n) if !combo.is_empty() => format!("{n} ({})", matrix::describe(&combo)),
                Some(n) => n.clone(),
                None if !combo.is_empty() => {
                    format!("{} ({})", job.id, matrix::describe(&combo))
                }
                None => job.id.clone(),
            }
        };
        if portable.is_some() && expr::serialized_size(&display, PORTABLE_OUTPUT_BYTES).is_err() {
            return JobEvaluation::terminal(
                base_name,
                Decision::Error("name: byte budget exceeded".into()),
            );
        }
        // each shard of a job has a name (and so a log) of its own
        if let Some(t) = shard {
            display = format!("{display} (shard {}/{})", t.index + 1, t.count);
        }
        // a name over some of the matrix only: keep the entries (and their logs) apart
        if !seen.insert(display.clone()) {
            display = format!("{display} #{}", idx + 1);
            seen.insert(display.clone());
        }
        if only.is_some_and(|o| o != idx) {
            continue;
        }
        if portable.is_some() {
            let next = expr::serialized_size(&(&cm, &display), PORTABLE_EVALUATION_BYTES)
                .ok()
                .and_then(|bytes| evaluated_bytes.checked_add(bytes));
            match next {
                Some(bytes) if bytes <= PORTABLE_EVALUATION_BYTES => evaluated_bytes = bytes,
                _ => {
                    return JobEvaluation::terminal(
                        base_name,
                        Decision::Error("evaluation byte budget exceeded".into()),
                    )
                }
            }
        }
        let run_it = {
            let ctx = bounded(Ctx::with_fns(&cm, needs_fns), portable.is_some());
            expr::condition(job.cond.as_deref(), &ctx)
        };
        let decision = match run_it {
            Ok(true) => Decision::Run(cm),
            Ok(false) => Decision::Skip(why_not().into()),
            Err(e) => Decision::Error(format!("if: {e:#}")),
        };
        entries.push(EvaluatedEntry { display, decision });
    }

    JobEvaluation { entries }
}

pub(crate) fn validate_strings(
    value: &Value,
    contexts: &Map<String, Value>,
    stage: expr::PortableStage,
) -> Result<()> {
    match value {
        Value::String(s) => expr::validate_portable_context(s, false, stage, Some(contexts)),
        Value::Array(a) => a
            .iter()
            .try_for_each(|v| validate_strings(v, contexts, stage)),
        Value::Object(m) => m
            .values()
            .try_for_each(|v| validate_strings(v, contexts, stage)),
        _ => Ok(()),
    }
}

/// Evaluate every `${{ }}` string inside a JSON value, keeping the result's type.
pub(crate) fn eval_strings(v: &Value, ctx: &Ctx) -> Result<Value> {
    ctx.check_value(v)?;
    let result = match v {
        Value::String(s) if s.contains("${{") => expr::interpolate_value(s, ctx)?,
        Value::Array(a) => Value::Array(
            a.iter()
                .map(|x| eval_strings(x, ctx))
                .collect::<Result<_>>()?,
        ),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| Ok((k.clone(), eval_strings(x, ctx)?)))
                .collect::<Result<_>>()?,
        ),
        other => other.clone(),
    };
    ctx.check_value(&result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn job(text: &str) -> Job {
        crate::workflow::parse("ci.yml", &format!("on: push\njobs:\n  test:\n{text}"))
            .unwrap()
            .jobs
            .remove(0)
    }
    fn evaluate(job: &Job, needs: &Map<String, Value>, only: Option<usize>) -> JobEvaluation {
        evaluate_job(
            job,
            json!({"workspace": "/actual-worker/root"}),
            json!({"os": "actual-worker-os", "temp": "/actual-worker/tmp"}),
            needs,
            false,
            false,
            only,
            None,
        )
    }
    #[test]
    fn selected_variant_preserves_order_display_dedup_and_worker_context() {
        let j = job("    name: '${{ matrix.name }}'\n    runs-on: x\n    strategy:\n      matrix:\n        n: [0, 1]\n        name: [same]\n    steps: []\n");
        let all = evaluate(&j, &Map::new(), None);
        assert_eq!(all.entries.len(), 2);
        assert_eq!(all.entries[0].display, "same");
        assert_eq!(all.entries[1].display, "same #2");
        for (i, expected_entry) in all.entries.iter().enumerate() {
            let selected = evaluate(&j, &Map::new(), Some(i));
            assert_eq!(selected.entries.len(), 1);
            assert_eq!(selected.entries[0].display, expected_entry.display);
            let (Decision::Run(cm), Decision::Run(expected)) =
                (&selected.entries[0].decision, &expected_entry.decision)
            else {
                panic!()
            };
            assert_eq!(cm, expected);
            assert_eq!(cm["strategy"], json!({"job-index": i, "job-total": 2}));
            assert_eq!(cm["github"]["workspace"], "/actual-worker/root");
            assert_eq!(cm["runner"]["os"], "actual-worker-os");
        }
    }
    #[test]
    fn legacy_per_combo_if_keeps_matrix_and_strategy_contexts() {
        let j = job("    if: matrix.n == 1 && strategy.job-index == 1 && strategy.job-total == 2\n    strategy:\n      matrix:\n        n: [0, 1]\n    steps: []\n");
        let all = evaluate(&j, &Map::new(), None);
        assert_eq!(all.entries.len(), 2);
        assert!(
            matches!(&all.entries[0].decision, Decision::Skip(reason) if reason == "if: false")
        );
        assert!(matches!(&all.entries[1].decision, Decision::Run(cm) if cm["matrix"]["n"] == 1));
        let selected = evaluate(&j, &Map::new(), Some(1));
        assert!(
            matches!(&selected.entries[0].decision, Decision::Run(cm) if cm["strategy"]["job-index"] == 1)
        );
    }
    #[test]
    fn direct_needs_status_and_if_errors_keep_legacy_reasons() {
        let mut j = job("    needs: seed\n    runs-on: x\n    steps: []\n");
        for (status, reason) in [
            ("failure", "needs failed"),
            ("skipped", "needs skipped"),
            ("cancelled", "needs cancelled"),
        ] {
            let needs = json!({"seed": {"result": status, "outputs": {}}, "unrelated": {"result": "failure"}}).as_object().unwrap().clone();
            let evaluated = evaluate(&j, &needs, None);
            assert!(matches!(&evaluated.entries[0].decision, Decision::Skip(why) if why == reason));
            j.cond = Some("always()".into());
            let evaluated = evaluate(&j, &needs, None);
            let Decision::Run(cm) = &evaluated.entries[0].decision else {
                panic!()
            };
            assert!(cm["needs"].get("unrelated").is_none());
            j.cond = None;
        }
        j.cond = Some("unknownFunction()".into());
        assert!(
            matches!(&evaluate(&j, &Map::new(), None).entries[0].decision, Decision::Error(why) if why.starts_with("if: "))
        );
    }
    #[test]
    fn matrix_evaluation_precedes_if_and_unsupported_constructs() {
        let mut j = job("    if: false\n    runs-on: x\n    container: unsupported\n    strategy:\n      matrix: '${{ fromJSON(needs.seed.outputs.matrix) }}'\n    steps: []\n");
        let error = evaluate(&j, &Map::new(), None);
        assert!(
            matches!(&error.entries[0].decision, Decision::Error(why) if why.starts_with("matrix: "))
        );
        let needs = json!({"seed": {"result": "success", "outputs": {"matrix": "{\"n\":[0,1]}"}}})
            .as_object()
            .unwrap()
            .clone();
        j.needs = vec!["seed".into()];
        let unsupported = evaluate(&j, &needs, None);
        assert_eq!(unsupported.entries.len(), 1);
        assert!(
            matches!(&unsupported.entries[0].decision, Decision::Skip(why) if why == "container: is not supported")
        );
        j.container = false;
        j.reusable = Some("other.yml".into());
        assert!(
            matches!(&evaluate(&j, &needs, None).entries[0].decision, Decision::Skip(why) if why == "reusable workflow other.yml is not supported")
        );
        j.reusable = None;
        assert_eq!(evaluate(&j, &needs, None).entries.len(), 2);
        assert!(evaluate(&j, &needs, None)
            .entries
            .iter()
            .all(|e| matches!(&e.decision, Decision::Skip(why) if why == "if: false")));
    }
    #[test]
    fn ancestor_failure_and_run_cancellation_are_explicit_facts() {
        let mut j = job("    runs-on: x\n    if: failure()\n    steps: []\n");
        let result = evaluate_job(
            &j,
            json!({}),
            json!({}),
            &Map::new(),
            true,
            false,
            None,
            None,
        );
        assert!(matches!(result.entries[0].decision, Decision::Run(_)));
        j.cond = Some("cancelled()".into());
        let result = evaluate_job(
            &j,
            json!({}),
            json!({}),
            &Map::new(),
            false,
            true,
            None,
            None,
        );
        assert!(matches!(result.entries[0].decision, Decision::Run(_)));
    }
}

#[cfg(test)]
mod runtime_tests;
