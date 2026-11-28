use task_traffic_controller::config::Config;
use task_traffic_controller::error::Result;
use task_traffic_controller::{retention, Context};

/// The retention sweep, now. The same one `exec`, `submit` and the library
/// run by themselves at most every six hours.
pub fn run(ctx: &Context, cfg: &Config) -> Result<i32> {
    let report = retention::sweep(ctx, cfg);
    println!("{report}");
    for e in &report.errors {
        eprintln!("task-traffic-controller: gc: {e}");
    }
    Ok(if report.errors.is_empty() { 0 } else { 2 })
}
