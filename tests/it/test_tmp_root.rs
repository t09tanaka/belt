//! The per-process root of the tests' temporary directories: gone when the
//! test process exits, and removed by the next test process when its owner
//! was killed.
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const RECORD: &str = "BELT_TEST_TMP_RECORD";
const HANG: &str = "BELT_TEST_TMP_HANG";

#[test]
#[ignore = "invoked as a dedicated subprocess by the root regressions"]
fn temp_root_process() {
    let record = PathBuf::from(std::env::var_os(RECORD).unwrap());
    let dir = crate::test_tmp::tempdir().unwrap();
    let sealed = dir.path().join("sealed");
    std::fs::create_dir(&sealed).unwrap();
    std::fs::write(sealed.join("file"), b"x").unwrap();
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o555)).unwrap();
    // A guard that never runs, with content its removal could not empty.
    std::mem::forget(dir);
    let pending = record.with_extension("pending");
    std::fs::write(&pending, crate::test_tmp::root().to_str().unwrap()).unwrap();
    std::fs::rename(pending, &record).unwrap();
    if std::env::var_os(HANG).is_some() {
        // Until the parent kills this process; bounded in case it cannot.
        std::thread::sleep(Duration::from_secs(120));
    }
}

fn helper(record: &Path, hang: bool) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "test_tmp_root::temp_root_process", "--ignored"])
        .env(RECORD, record)
        .env_remove(HANG)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if hang {
        command.env(HANG, "1");
    }
    command.spawn().unwrap()
}

/// The root the helper recorded (`<outer>/d`), once it has.
fn recorded_root(record: &Path) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(text) = std::fs::read_to_string(record) {
            return PathBuf::from(text);
        }
        assert!(Instant::now() < deadline, "the helper recorded no root");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn exiting_test_process_removes_its_root_and_what_guards_left() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let record = dir.path().join("root");
    assert!(helper(&record, false).wait().unwrap().success());
    let root = recorded_root(&record);
    assert_ne!(root, crate::test_tmp::root());
    let outer = root.parent().unwrap();
    let name = outer.file_name().unwrap().to_str().unwrap();
    assert!(name.starts_with("belt-t-"), "{name}");
    assert!(!outer.exists(), "{outer:?} outlived its process");
}

#[test]
fn killed_test_process_root_is_removed_by_the_next_test_process() {
    let dir = crate::test_tmp::tempdir().unwrap();
    let record = dir.path().join("killed");
    let mut killed = helper(&record, true);
    let root = recorded_root(&record);
    let outer = root.parent().unwrap().to_path_buf();
    // While its owner lives, another test process leaves the root alone.
    let bystander = dir.path().join("bystander");
    assert!(helper(&bystander, false).wait().unwrap().success());
    assert!(root.is_dir() && outer.join("owner").is_file());
    // The child is ours and not reaped yet, so the PID is still its.
    assert!(belt::platform::signal_pid(killed.id(), libc::SIGKILL));
    assert!(!killed.wait().unwrap().success());
    // Any test process that starts from here on may remove the root: no
    // assertion that it still exists.
    let next = dir.path().join("next");
    assert!(helper(&next, false).wait().unwrap().success());
    assert!(!outer.exists(), "{outer:?} survived the next test process");
    // The root of this process was never for another process to remove.
    assert!(crate::test_tmp::root().is_dir());
}
