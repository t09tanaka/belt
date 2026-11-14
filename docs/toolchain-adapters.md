# Private toolchain adapter API (S20b)

`toolchain::adapters` supplies closed installation plans, durable installation
receipts, and per-attempt activation contracts. Public command submission freezes
source declarations before persistence and routes configured recipes through the
shared setup builder. Operator profiles remain base worker identities; each task
receives an activated environment. Native observations remain distinct from
eligibility to install a selected version.

The trusted parent supplies an authenticated catalog digest and explicit alias
or expression resolutions to `resolve`. This API checks those resolutions
against the existing captured `FrozenToolchains` constraints and binds the
selection authority to the unselected contract. It does not fetch catalogs,
read the live checkout, trust an installed manager's default, or authenticate a
caller-supplied digest. Catalog authentication is a required integration duty.
`plan` requires that exact evidence and concrete selection; retry never floats.

The trusted operator supplies absolute, captured regular manager/runtime
identities and a fixed search path. Symlinked operator executables must first
be resolved by the operator configuration reader. Projects cannot select these
paths. The recipes are fixed argv, with a cleared environment required by the
executor, a private working directory/HOME/config/cache/tmp, and these mappings:

| Adapter | Supported selection | Explicit preparation |
| --- | --- | --- |
| nodenv | node | Trusted node-build dependency: exact version and private `NODENV_ROOT/versions/<version>` prefix; no plugin/default discovery |
| rustup | rust | `toolchain install <exact>-<explicit-host> --profile minimal --no-self-update`, private RUSTUP_HOME/CARGO_HOME |
| mise | node, python, go, ruby | `install-into <tool>@<exact> <private-payload>`, no config/env/hooks and private mise directories |
| fvm | flutter | `install <exact> --skip-pub-get`, private FVM_CACHE_PATH, explicit SDK setup before finalization |
| corepack | npm, pnpm, yarn | `install --global <manager>@<exact>[+sha…]` within unique private COREPACK_HOME, then `enable --install-directory <private-bin> <manager>` |

Go probes use `go version`. Go probe and consumer environments force
`GOTOOLCHAIN=local`, overriding base environment values so module/workspace
requirements and persisted Go settings cannot switch or download another
compiler. A project requiring a newer compiler fails with the selected local
compiler rather than changing the frozen receipt. See [Go toolchain
selection](https://go.dev/doc/toolchain#select) and [Go version
command](https://pkg.go.dev/cmd/go#hdr-Print_Go_version).

Corepack's private LastKnownGood is allowed; host defaults are untouched.
Corepack activation disables project-spec/default-to-latest/network/auto-pin and
`.corepack.env` loading. Distribution integrity is passed as the frozen Corepack
descriptor. Other adapters reject distribution-hash contracts they cannot
honestly enforce. Unsupported tool mappings (including standalone Dart and
non-semver channel identities without concrete resolution) fail explicitly.
Xcode/license acceptance, Docker Desktop and system package installation stay
manual, with action reasons; this API never accepts legal terms or runs sudo.

`AdmittedExecutor` is a trusted host interface, not a token or proof that TTC
admission occurred. There is deliberately no production process executor here.
Its implementation must clear inherited environment, use the exact supplied
argv/env/cwd, retain at most bounded stdout, enforce deadlines and cancellation,
and own process groups through complete drain. `install_admitted` must only be
called by the existing admitted setup builder. The nonblocking per-install file
lock prevents simultaneous publication; it is not a second scheduler or a
replacement for setup singleflight, builder reference counting or TTC permits.

Preparation publishes only after fixed concrete-version probes succeed. Flutter
initialization probes occur only here, never during detection/activation.
Receipt publication uses an owned nofollow 0600 file, fsync, rename and directory
fsync. Receipts bind the recipe/version, external manager/runtime bytes, probe
output digests and a bounded installed-file manifest. The manifest rejects
special files, hard links, writable/shared ownership and escaping links.
Internal file links are recorded; Corepack external script links are accepted
only when the exact target is a separately declared external identity. Manager,
Node, target script and link identity are all retained. Installed payloads are
bounded to 131072 entries, 8 GiB read and a 32 MiB encoded receipt. The API is
synchronous: the caller must place this bounded blocking I/O under its owned
blocking-work/drain mechanism, not spawn detached work.

`detect` reads validated receipts and actual bytes only. A missing root is
Missing; an incomplete, corrupt, changed or unreceipted root is Unknown, never
Installed. No manager command runs. Receipt validation repeats before activation
and reuse. Failed/crashed installs never produce success; their exact partial
root must be explicitly cleaned by its owner before retry. There is no recursive
rollback or cleanup of other worktrees. Detect is plan-directed; discovering and
reconstructing the inventory of plans after restart is an integration duty.
Exact artifact keys omit source contracts, allowing private reuse across runs;
activation binding still records each frozen source contract separately; actual
environment fingerprints remain stable across source commits with equal tools/env.

`activate` handles a complete single-tool contract without baselines.
`activate_set` requires every frozen selection exactly once, validates all
receipts and declared baseline observations, and returns one immutable env for
both setup and consumers. It preserves the base profile fingerprint separately,
binds shell identity/setup policy/actual installations/environment into the actual
execution fingerprint, independent of captured source commits. A separate
binding digest retains the frozen source/profile/scope/contract. Consumer HOME,
XDG directories and TMPDIR remain those of the operator base Profile; private
installer HOME is used only during explicit preparation/finalization.
Conflicting manager env fails closed: separate Corepack homes for two different
package-manager selections are currently unsupported. No selection is silently
dropped. Tasks cannot supply these activation overrides through the wire.

## Public command routing

`TaskSpec` and `AttemptPayload` optionally carry a versioned `FrozenBundle`:
the source-bound frozen contract and every original resolution evidence. The
absent field preserves legacy serialization and hashes. Exact typed source pins
can be sealed offline. Ranges and aliases require a coherent trusted local
inventory or one authenticated read-only worker proposal, tried by configured
priority and worker ID. Versions from different workers are never combined.
Unknown required baselines block capture. Project commands do not guess CI matrix
or secret-derived values.

`frozen_toolchains_v1` must be negotiated. Both durable database schema fences
prevent older owners from silently dropping constraints. Parent proposal IO runs
outside admission and database ownership with a source lifetime guard. It never
uploads source or installs. Workers derive manager paths and environment locally
and verify the captured source declarations before installation.

The complete resolved set is rebuilt into trusted requests before polling the
shared `Setups` lifecycle. Project setup builders and consumers receive the same
activation environment, shell and execution fingerprint. The accepted base
profile stays separate; effective fingerprints feed template, history and
artifact identity. Submitted environments cannot change protected activation
variables, including `GOTOOLCHAIN`. Private durable bindings are checked before
new launches; recovery inspects identities without repairing drift. Running
supervisors retain their existing adoption path. Forecast and inspection never
install.

Actual manager/Docker acceptance remains outside private fake-manager tests.
Bundled Dart matching, installed-version enumeration, reusable installation timing
telemetry and `workers diff`/`sync` remain separate work.

Primary recipe references: [node-build usage](https://github.com/nodenv/node-build#usage),
[rustup environment](https://rust-lang.github.io/rustup/environment-variables.html),
[mise install-into](https://mise.jdx.dev/cli/install-into.html),
[mise environment](https://mise.jdx.dev/configuration/environment-variables.html),
[FVM commands](https://fvm.app/documentation/guides/basic-commands),
[Corepack commands and environment](https://github.com/nodejs/corepack#utility-commands).
The operator executable byte identities pin the actual installed manager release;
unsupported CLI versions fail preparation rather than fall back to other flags.

## Worker-local registry (S20c infrastructure)

`toolchain::registry::Registry` retains a private `state/toolchains.toml` snapshot
for one resident lifetime. Changes require operator drain/restart; request building
and detection do not reread configuration, run discovery commands, create directories,
install, or modify manager defaults. The checked worker factory supplies its existing
trusted `Profile` and measured execution fingerprint to `Registry::load`; it must first
obtain the canonical private tool root from `Setups::private_tool_root`. No task or wire
input may supply this configuration or the measured fingerprint.

The schema is version 1, with `active_profile`, local `profile_definition`, and a
`recipes` array. Each recipe has `tool`, closed `kind` (`Nodenv`, `Rustup`, `Mise`,
`Fvm`, `Corepack`), absolute canonical `manager`, `dependencies`, `search_path`,
optional `rust_host` (required for Rustup), explicit concrete `inventory`, and optional
`resolved_inputs` mapping aliases to inventory versions. Nodenv requires a trusted executable `node-build` dependency; Corepack requires
a trusted executable `node` dependency. Unknown fields, duplicate tool recipes,
unsupported kind/tool pairs and invalid paths are rejected. There are no shell,
installer argv, task environment or project path fields. The configuration is read
through one no-follow descriptor and must be an owned private singly linked regular
file, at most 64 KiB; recipes are limited to 32 and each inventory to 256 versions.
A missing file yields an unconfigured registry; malformed supplied configuration fails
closed. Manager/dependency identities are captured and revalidated when planning.

Inventory is operator authorization to prepare a concrete version, not evidence that
it is installed or available from a remote server. Local resolution chooses the highest
matching authorized version and binds evidence to the recipe's inventory authority digest;
aliases require explicit `resolved_inputs`. Expressions and version-file expressions always
require original parent evidence attesting the captured CI context; a static local
expression mapping cannot select a matrix leg. An existing frozen selection
requires its original `ResolutionEvidence`: the bridge validates the full source contract
and selection, requires the selected version in the local inventory, and preserves the
parent catalog, alias evidence and authority without substituting local mappings.
`PreparationRequest` derives its local definition and base execution identity from the
retained trusted worker inputs; the frozen parent definition remains capture provenance.
The exact plan key and private root come from the closed adapter plan. Source-independent
artifact reuse retains separate source-bound request evidence.

`Registry::detect` validates the exact request binding and only checks that plan's private
adapter receipt/tree. It does not recursively discover manager catalogs. Unconfigured,
unsupported, resolution-required, unauthorized versions, manual requirements, missing
receipts and unknown/drift states remain distinct; the registry never advertises
`auto_installable` native observations. Public routing uses explicit preparation
eligibility and baseline checks. Installed-version enumeration, `workers diff`/`sync`
and installation timing telemetry remain separate. Host installation and network
acceptance are outside this slice.

A valid baseline-only tool returns `BaselineCheckRequired`, not an invalid-capture
error or an install request. Worker routing first compares frozen baselines
with actual base observations under the frozen match policy. A match needs no installer;
mismatch or unknown can exclude the candidate or block with `environment_mismatch`.
The registry does not synthesize explicit requirements from baselines.

Multi-tool callers must finish a coherent resolution phase before polling or installing
any tool. A request used to resolve the first tool may contain an intermediate frozen
contract with later tools still unselected. Retain each tool's original evidence, finish
all selections, then rebuild every request from the complete final frozen contract using
`request(final_frozen, tool, Some(original_evidence))`. All plans passed to activation must
contain the same complete frozen contract. Evidence contract digests clear all selections,
so this rebuild preserves the original authority. Do not begin installation with an
intermediate request and later change that consumer's binding to a completed contract.
