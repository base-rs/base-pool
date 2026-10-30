// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::process::Command;

#[test]
fn executable_lists_discovered_tasks() {
    let output = Command::new(env!("CARGO_BIN_EXE_base-pool-bake"))
        .arg("--list")
        .output()
        .expect("run Bake task executable");

    assert!(output.status.success());
    let output = String::from_utf8(output.stdout).expect("task listing is UTF-8");
    assert!(output.contains("cargo:after_version_bump"));
}

#[test]
fn executable_reports_unknown_tasks_as_failure() {
    let output = Command::new(env!("CARGO_BIN_EXE_base-pool-bake"))
        .arg("unknown-task")
        .output()
        .expect("run Bake task executable");

    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).expect("error output is UTF-8");
    assert!(error.contains("unknown task"), "{error}");
}
