# Changelog

All notable changes to belt are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: a breaking change bumps
the minor version).

## [Unreleased]

## [0.1.0] - 2026-11-28

First release. The package is `belt-ci` on crates.io; the command is `belt`.

### Added

- `belt ci` plans and runs a repository's GitHub Actions workflows on your own machines and can post the result as a GitHub commit status. The tools a workflow sets up are detected and installed by version, as `setup-*` actions do on GitHub.
- A group of machines shares the work with no extra setup per run. `belt init` starts a group and `belt group join <host>` adds a machine. Every member can submit, and jobs are placed on the members that have room, longest first.
- `belt exec` and `belt submit` run one command on the group, with `ls`, `show`, `logs`, `wait`, `cancel`, `retry`, `outputs` and `top` to follow it.
- Resource admission (task-traffic-controller), fast worktrees with ready dependencies (fast-worktree) and per-worktree ports and local resources (portool) are linked in, so one install covers all of them.
- `belt upgrade` and `belt workers upgrade` replace a resident with a health check and roll back if the new version does not answer. `belt doctor` checks the setup, including PATH folders that macOS privacy protection guards.
- On macOS the resident runs as a launchd agent with `ProcessType` Interactive, so its jobs are not throttled as background work.
