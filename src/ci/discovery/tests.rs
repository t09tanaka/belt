use super::*;
use crate::ci::execute::tests::fixture;
use local_ci::shard::Basis;

const WORKFLOW: &str =
    "on: push\njobs:\n  js:\n    runs-on: ubuntu-latest\n    steps:\n      - run: npx jest\n";

#[test]
fn an_exact_worker_listing_is_split_with_history_and_bound_to_its_plan() {
    let fixture = fixture(WORKFLOW);
    let plan = fixture.plan;
    let candidate = candidates(&plan).pop().unwrap();
    let probe = candidate.execution_plan(&plan).unwrap();
    assert!(probe.workflows[0].jobs[0].steps[1]
        .run
        .as_ref()
        .unwrap()
        .contains("--listTests"));
    let stdout = ["a.js", "b.js", "c.js", "d.js"]
        .iter()
        .map(|n| fixture.area.join(n).display().to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let listing = candidate
        .parse_listing(
            &plan,
            &fixture.area,
            "30.2".into(),
            CommandOutput {
                stdout,
                stderr: String::new(),
                success: true,
                exit: Some(0),
            },
        )
        .unwrap();
    assert_eq!(listing.discovered.tests, ["a.js", "b.js", "c.js", "d.js"]);
    let costs = PartitionOpts::default();
    let timing = History {
        millis: [("a.js", 90), ("b.js", 30), ("c.js", 30), ("d.js", 30)]
            .into_iter()
            .map(|(s, n)| (s.to_owned(), n))
            .collect(),
    };
    let request = listing
        .partition(&plan, &timing, 2, &costs, Isolation::None)
        .unwrap();
    assert_eq!(request.plan.basis, Basis::History);
    assert!(request.plan.shards.iter().all(|s| s.load == 90));
    assert_eq!(
        listing
            .partition(&plan, &History::default(), 2, &costs, Isolation::None)
            .unwrap()
            .plan
            .basis,
        Basis::Fallback
    );
    let mut expensive = listing.clone();
    expensive.setup_ms = Some(120);
    assert_eq!(
        expensive
            .partition(&plan, &timing, 2, &costs, Isolation::None)
            .unwrap()
            .plan
            .shards
            .len(),
        1
    );
    let split = apply(
        &plan,
        std::slice::from_ref(&listing),
        2,
        |_| Ok((timing.clone(), costs.clone())),
        |_| Isolation::None,
    )
    .unwrap();
    split.verify().unwrap();
    assert!(split.shard_groups[0].split);
    assert_eq!(split.shard_groups[0].manifest, listing.discovered.tests);
    assert!(listing
        .partition(&split, &timing, 2, &costs, Isolation::None)
        .is_err());
    assert!(apply(
        &plan,
        &[],
        2,
        |_| Ok((timing.clone(), costs.clone())),
        |_| Isolation::None
    )
    .is_err());
}

#[test]
fn browser_shards_require_a_fixture_isolation_declaration() {
    let f = fixture(&WORKFLOW.replace("npx jest", "npx cypress run"));
    let step = candidates(&f.plan).pop().unwrap();
    let listing = WorkerListing {
        setup_ms: None,
        step,
        runner_version: "13.0.0".into(),
        discovered: Discovered {
            tests: vec!["cypress/e2e/a.cy.js".into(), "cypress/e2e/b.cy.js".into()],
            complete: true,
            blocked: vec![],
        },
    };
    let request = |isolation| {
        listing
            .partition(
                &f.plan,
                &History::default(),
                2,
                &PartitionOpts::default(),
                isolation,
            )
            .unwrap()
    };
    assert!(request(Isolation::None)
        .plan
        .not_split
        .contains(&NotSplit::NoIsolation));
    let asked = request(Isolation::Declared {
        evidence: "database per portool project".into(),
    });
    assert_eq!(asked.plan.shards.len(), 2);
    assert!(f.plan.with_shards(&[asked]).unwrap().shard_groups[0].split);
}
