//! Report the exact helper versions resolved for this binary.
fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lock: toml::Value = std::fs::read_to_string("Cargo.lock")
        .unwrap()
        .parse()
        .unwrap();
    let packages = lock["package"].as_array().unwrap();
    let own = packages
        .iter()
        .find(|p| p["name"].as_str() == Some(env!("CARGO_PKG_NAME")))
        .expect("belt package in Cargo.lock");
    let direct = own["dependencies"].as_array().expect("belt dependencies");
    for (name, key) in [
        ("fast-worktree", "FAST_WORKTREE"),
        ("portool", "PORTOOL"),
        ("local-ci", "LOCAL_CI"),
        ("task-traffic-controller", "TTC"),
    ] {
        // Cargo appends a version to the direct edge when more than one
        // release of a crate is present in the dependency graph.
        let wanted = direct
            .iter()
            .filter_map(toml::Value::as_str)
            .find_map(|d| {
                let mut parts = d.split_whitespace();
                (parts.next() == Some(name)).then(|| parts.next())
            })
            .expect("direct helper dependency");
        let package = packages
            .iter()
            .find(|p| {
                p["name"].as_str() == Some(name)
                    && wanted.is_none_or(|v| p["version"].as_str() == Some(v))
            })
            .unwrap();
        println!(
            "cargo:rustc-env=BELT_{key}_VERSION={}",
            package["version"].as_str().unwrap()
        );
    }
}
