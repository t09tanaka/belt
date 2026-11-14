# Private toolchain adapter API (S20b)

`toolchain::adapters` supplies closed installation plans, durable installation
receipts, and per-attempt activation contracts. It does not complete S20 and
currently does not change worker placement, TaskSpec, active Profile, setup
builders, forecast, or consumer process environments. No worker advertises
`auto_installable` on the strength of this module alone.

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

## Mandatory S20c integration

Connect catalog acquisition, baseline observation and plan reconstruction to
captured submit requirements; connect admitted execution to `Setups` rather than
inventing admission grants. Reuse its existing per-key singleflight/cancellation
and forecast preparation accounting. Carry the same activation receipt/env/shell
to setup builders and consumers, and use its effective fingerprint for
cache/template/history/artifact identity. Revalidate changed/missing tools before
new launch/relaunch without rewriting already accepted attempts; supervisor
adoption remains bound to its previous actual identity. Add bundled Dart matching
and broader manager combinations before advertising those capabilities. Actual
manager/Docker acceptance is still required in owned isolated environments; the
current tests execute fake fixture interfaces only and never install host tools.

Primary recipe references: [node-build usage](https://github.com/nodenv/node-build#usage),
[rustup environment](https://rust-lang.github.io/rustup/environment-variables.html),
[mise install-into](https://mise.jdx.dev/cli/install-into.html),
[mise environment](https://mise.jdx.dev/configuration/environment-variables.html),
[FVM commands](https://fvm.app/documentation/guides/basic-commands),
[Corepack commands and environment](https://github.com/nodejs/corepack#utility-commands).
The operator executable byte identities pin the actual installed manager release;
unsupported CLI versions fail preparation rather than fall back to other flags.
