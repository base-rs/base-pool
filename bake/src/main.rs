// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use std::process::ExitCode;

fn main() -> ExitCode {
    match bake::Registry::discover().and_then(|registry| registry.run().map(|_| ())) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("bake: {error}");
            ExitCode::FAILURE
        }
    }
}

#[path = "bake_generated_tasks/mod.rs"]
mod bake_generated_tasks;
