# fast-worktree

Create a git worktree with its dependencies in seconds, and remove it in
one.

```sh
fast-worktree add ../app-feature -b feature/x   # worktree + node_modules, ready to work
fast-worktree install                           # dependencies for an existing worktree
fast-worktree check --quiet                     # what is missing
fast-worktree remove ../app-feature             # gone at once, deleted in the background
fast-worktree pool status                       # worktrees made ahead of time, if you want them
fast-worktree hook print-config                 # Claude Code's worktrees, made by fast-worktree
fast-worktree plan                              # where each project's files would come from
fast-worktree config check [--staged]           # every lockfile listed in .fast-worktree.toml?
```

## What & why

A worktree per task keeps parallel work apart, but each new worktree starts
without `node_modules`. In a monorepo with several `package-lock.json`
files that means several `npm ci` runs before anything else can happen,
and removing the worktree again means deleting tens of thousands of files.

`fast-worktree` makes both ends cheap:

- **Clone, don't install.** On macOS (APFS), a whole `node_modules`
  directory is cloned copy-on-write with one `clonefile(2)` call. Cloning
  file by file (`cp -Rc`) is about as slow as `npm ci`; one call on the
  directory is what makes it fast.
- **A cache keyed by what matters.** Every install result whose tree
  matches the lockfile is kept under the lockfile's hash and the `node -v`
  the project resolves (native addons are built for one Node ABI). The
  next worktree with the same lockfile clones it.
- **Other worktrees as a source.** Without a cache entry, a registered
  worktree (the main checkout included) is cloned from when its lockfile is
  byte-identical, its installed tree matches that lockfile (compared with
  `node_modules/.package-lock.json`: versions, integrity, resolved sources
  and links, so a checkout that pulled but did not reinstall is not used,
  nor one whose git dependency moved to another commit), and its Node
  version is the same.
- **Only the difference when the lockfile changed.** Without either, the
  project's most recently used cache entry (an older lockfile, same Node
  version) is cloned and `npm install` brings it up to date. The result is
  kept only if `package-lock.json` is byte-identical afterwards and the
  tree matches it; otherwise it is thrown away and `npm ci` runs.
- **`npm ci` only when nothing matches.** Projects are handled in
  parallel; their install scripts, which may write the repository's
  `.git/config`, [one at a time](#install-scripts).
- **Flutter and Dart too.** A tracked `pubspec.lock` is a project: `flutter
  pub get --offline` (then online), or `dart pub get` without Flutter.
- **Ruby too.** A tracked `Gemfile.lock` is a project: the directory
  Bundler installs into is cloned from the cache or another worktree with
  the same `Gemfile.lock`, Ruby version and platform, and checked with
  `bundle check --dry-run`; else `bundle install` runs.
- **Any other directory, by key.** A [`[[clone]]`](#what-each-worktree-gets-clone)
  names a directory such as CocoaPods' `Pods` or a build cache, the files
  that decide whether a copy can be reused, and the command that makes it;
  it is cloned the same way, and `remove` hands it to the next worktree.
- **Ignored files come along.** `.env` and other gitignored files listed
  in `.worktreeinclude` are copied into each new worktree.
- **Removal by rename.** `remove` renames the worktree into a trash
  directory on the same volume, removes its own entry from git (and no
  other), and deletes the files in a detached background process.
- **Worktrees made ahead of time.** With a [pool](#the-pool), `add` moves
  a ready worktree into place and switches its branch, instead of creating
  one. Off unless you set a size.
- **Claude Code's worktrees.** `hook create` and `hook remove` are
  [WorktreeCreate / WorktreeRemove hooks](#claude-code-hooks), so
  `claude --worktree` and subagent worktrees get their dependencies the
  same way.

## Install

```sh
cargo install fast-worktree
```

Or download a binary from the
[releases](https://github.com/t09tanaka/fast-worktree/releases).

## Usage

```sh
fast-worktree add <path> -b <branch> [--fetch-timeout <secs>] [<start-point>]
```

Native Git worktree metadata operations and branch deletion are serialized
per repository, including their Git hooks, to avoid reading another
creation's incomplete metadata. Long Git hooks therefore delay other
metadata operations in that repository. A synchronous metadata operation
from a Git hook in the same repository fails immediately with a recursive
operation error instead of waiting for its own parent. The normal
`FAST_WORKTREE_SKIP=1` post-checkout integration still returns immediately.
Dependency installation and `.fast-worktree.toml` hooks remain outside this
lock and can run concurrently. Raw Git commands and other tools do not
participate in this cooperative lock.

Runs `git worktree add <path> -b <branch> <start-point>`, copies the files
`.worktreeinclude` selects, then runs `install` in the new worktree, then
the `post_add` hooks. With the [pool](#the-pool) on, a ready worktree is
moved to `<path>` instead of `git worktree add`. Without `<start-point>`, the new branch starts from
origin's default branch, freshly fetched: it is read from
`refs/remotes/origin/HEAD` (set with `git remote set-head origin -a` when
missing), never assumed to be `main`. The fetch never waits for a
prompt (as in [`hook create`](#claude-code-hooks)) and is stopped after
`--fetch-timeout` seconds (default 30). When it fails or is stopped, the
branch starts from origin's default branch as last fetched, with a note on
stderr; only when there is no such ref does `add` fail. `git worktree add`
runs with `FAST_WORKTREE_SKIP=1`, so a [post-checkout hook](#post-checkout-hook)
stays out of the way, and with `--quiet`, so that git's own messages and
its `Updating files` progress stay out of the output (its errors do not).

```sh
fast-worktree install [--root <dir>] [--changed] [<project-dir>...]
```

For every tracked `package-lock.json`, `pubspec.lock` and `Gemfile.lock`, or for each
[`[[clone]]`](#what-each-worktree-gets-clone) when there are any (or only
the given project directories), makes the dependencies present. A project is
named by its directory (relative to the current directory or to the
worktree root); `root` names the one at the root (`.`), unless a project
directory is called `root`, and the last part of a directory alone
(`lambda` for `provision/lambda`) names it when no other project's
directory ends the same way (otherwise the error lists them). npm projects:
from the cache, else from a matching worktree, else incrementally from the
latest cache entry, else with `npm ci --prefer-offline --no-audit
--no-fund` (without its scripts first, see [install
scripts](#install-scripts)). Dart projects: `pub get` as above; with neither `flutter` nor
`dart` installed, a note and nothing else. Bundler projects: see
[the `bundler` preset](#what-each-worktree-gets-clone). A project whose dependencies
are present is skipped; for npm that means `node_modules` has its
`node_modules/.package-lock.json`, and a `node_modules` without one (an
install that did not finish) is moved to the trash and installed again.
A `node_modules` that another package manager made is left as it is, with
a note, and nothing is installed there: one with `.yarn-integrity` or
`.yarn-state.yml` (yarn), `.modules.yaml` (pnpm) or `.bun` (bun), or one
without `.package-lock.json` next to a `bun.lock` or `bun.lockb`.
With `--changed`, a project is installed again when its lockfile changed
since fast-worktree last installed it (for npm: unless the installed tree
already matches the new lockfile) or, for npm, when the project now
resolves another Node version. Then the `post_add` hooks run.

```sh
fast-worktree check [--quiet]
```

Lists the projects without `node_modules/.package-lock.json` (npm) or
`.dart_tool/package_config.json` (pub) or the directory Bundler installs
into (bundler). It always exits 0 and runs nothing
but two quick `git` calls. `--quiet` prints only when something is missing.
While the background install of [`hook create`](#claude-code-hooks) runs,
it says so.

```sh
fast-worktree check --post-checkout <prev> <new> <flag>
```

For a `post-checkout` hook; see [below](#post-checkout-hook).

```sh
fast-worktree remove <path> [--delete-branch]
```

Refuses (exit 1) the main checkout, a path that is not a worktree root, a
locked worktree (a spare of the pool is one; `pool clear` removes those),
a worktree with another registered worktree inside it
(even in an ignored directory), a worktree with uncommitted or untracked
changes, and a worktree with submodules. Changes are what `git status
--porcelain --untracked-files=all --ignore-submodules=none` prints, so
`status.showUntrackedFiles=no` hides nothing; ignored files do not count,
and a `git status` that fails counts as changes. With `--delete-branch`, it
also refuses a branch whose commit is not reachable from any of its
upstream, `origin/<default>` and the local `<default>` branch, where
`<default>` is origin's default branch as `refs/remotes/origin/HEAD`
records it (nothing is fetched; without that record, the main checkout's
`HEAD` is used, as `git branch -d` does). So a branch merged on origin can
be deleted while the main checkout is behind or on another branch. And it refuses a worktree
that cannot be renamed (its parent directory is not writable, or it is a
mount point). All of this is decided before any hook runs and before
anything is removed.

Otherwise stops the background install of [`hook create`](#claude-code-hooks)
if one is still running in it (as `hook remove` does), runs the
`pre_remove` hooks, renames the worktree away, deletes
its entry under `.git/worktrees/`, deletes the files in the background
and, with `--delete-branch`, deletes the branch (`git branch -D`, if it is
still at the commit that was checked). It does not run `git
worktree prune`, which would also unregister other worktrees that are only
out of sight, such as one on a volume that is not mounted. If the rename
still fails after the hooks ran, it says so and exits 1; the worktree stays
where it was, registered.

```sh
fast-worktree cache ls
fast-worktree cache prune [--older-than <days> | --legacy]
```

`ls` lists the cache entries with their repository, project directory
(`.` for the root), key and last use. Directories that versions before
0.2.0 made (a repository's name without the hash), which nothing reads any
more, are listed apart under `LEGACY`. `prune` moves the entries not used
for `<days>` (default 7) to the trash, and prints each as `ls` names it;
`prune --legacy` moves the `LEGACY` directories to the trash instead.

```sh
fast-worktree kept ls
fast-worktree kept prune [--older-than <days>]
```

`ls` lists the refs under `refs/fast-worktree/kept/` of the repository
you are in, where `hook remove` keeps commits that would be lost (see
[Claude Code hooks](#claude-code-hooks)), with their commit and when they
were kept. `prune` deletes those kept `<days>` (default 30) ago or more,
going by the date in the ref's name, and those kept more than 25 hours ago
whose commit a local branch or a tag has (a remote-tracking branch does not
count: somebody else can delete it); refs whose name has no date are left
alone. The same is done by itself (see [What is
deleted by itself](#what-is-deleted-by-itself)).

```sh
fast-worktree pool status | fill | refresh | clear
fast-worktree hook create | remove | print-config
fast-worktree wait [<path>]
```

See [the pool](#the-pool) and [Claude Code hooks](#claude-code-hooks).

### Example

[`examples/monorepo`](examples/monorepo) has two npm projects with one
small dependency each and a Dart package without dependencies. `web` has
a `postinstall` that writes a file outside `node_modules`; `api` has one
that generates code into `node_modules`, which `.fast-worktree.toml` skips
when its input is unchanged. `.worktreeinclude` lists `.env`. Committed as
its own repository and cloned to `~/monorepo`, with a `.env` in the main
checkout, on an Apple Silicon Mac:

```console
$ fast-worktree add ../monorepo-a -b feature/a
worktree added in 0.0s
copy   .env
npm ci api (0.5s)
pub get mobile (flutter, 0.5s)
npm ci web (0.5s)
deps done in 0.5s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-a
```

The second worktree clones both npm projects from the cache. `web`'s own
`postinstall` runs again in the clone; `api`'s is skipped, because the
schema it generates from is the one the cloned `node_modules` was
generated from:

```console
$ fast-worktree add ../monorepo-b -b feature/b
worktree added in 0.0s
copy   .env
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/api/263f7cb8e0876026-v20.13.1 (0.1s)
pub get mobile (flutter, 0.3s)
clone  web from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/web/02ed81ddfaf462f9-v20.13.1 (0.3s)
deps done in 0.4s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-b
$ cat ../monorepo-b/.env
API_TOKEN=dev-only
$ cat ../monorepo-b/web/generated.txt
one day is 86400000 ms
$ cat ../monorepo-b/api/node_modules/.codegen/index.js
module.exports = ["id","name"];
```

After a dependency of `web` changes, its new lockfile has no cache entry
and no worktree has it installed, so the old entry is cloned and updated
with `npm install`:

```console
$ (cd web && npm install --package-lock-only --no-audit --no-fund ms@2.1.2)
$ git commit -qam "web: ms 2.1.2"
$ fast-worktree add ../monorepo-c -b feature/c HEAD
worktree added in 0.0s
copy   .env
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/api/263f7cb8e0876026-v20.13.1 (0.1s)
pub get mobile (flutter, 0.3s)
delta  web from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/web/02ed81ddfaf462f9-v20.13.1 (0.5s)
deps done in 0.5s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-c
```

With the [post-checkout hook](#post-checkout-hook) installed, a plain `git
worktree add` gets what can be cloned, and a `git switch` lists what is
missing:

```console
$ git worktree add -q ../monorepo-d -b feature/d HEAD
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/api/263f7cb8e0876026-v20.13.1 (0.1s)
clone  web from ~/Library/Caches/fast-worktree/monorepo-fc34b8e7/web/a1fd1fb4cf620116-v20.13.1 (0.3s)
missing deps: mobile (pub)
run `fast-worktree install` to install them
$ git -C ../monorepo-d switch -q -c feature/e
missing deps: mobile (pub)
run `fast-worktree install` to install them
```

The cache, and `remove`:

```console
$ fast-worktree cache ls
REPO               DIR  KEY                        LAST USED
monorepo-fc34b8e7  api  263f7cb8e0876026-v20.13.1  2026-10-26 (0d ago)
monorepo-fc34b8e7  web  02ed81ddfaf462f9-v20.13.1  2026-10-26 (0d ago)
monorepo-fc34b8e7  web  a1fd1fb4cf620116-v20.13.1  2026-10-26 (0d ago)
3 entries in ~/Library/Caches/fast-worktree
$ git -C ../monorepo-a commit -q --allow-empty -m "work in progress"
$ fast-worktree remove ../monorepo-a --delete-branch
fast-worktree: branch feature/a is not merged into origin/main or main; nothing was removed. Merge it first, or run remove without --delete-branch
$ fast-worktree remove ../monorepo-b --delete-branch
hook   pre_remove: echo "bye: $(basename "$FAST_WORKTREE_PATH")"
bye: monorepo-b
removed ~/monorepo-b (moved to ~/Library/Caches/fast-worktree/trash/1793012885-22119-0303526000-monorepo-b, deleting in background)
deleted branch feature/b
```

The dependencies here are tiny; the clone stays one syscall per project
however large `node_modules` is, while `npm ci` grows with it.

## Settings: `.fast-worktree.toml`

fast-worktree knows nothing about the tools a repository uses. Anything
repository-specific goes in an optional `.fast-worktree.toml` at the
worktree root:

```toml
[[post_add]]            # after add / install, in the worktree root, in order
run = "make setup"
requires = "make"       # optional: skipped silently when not on PATH or in ~/.cargo/bin

[[pre_remove]]          # after remove's safety checks, before the worktree is moved
run = 'docker compose -p "$(basename "$FAST_WORKTREE_PATH")" down -v --remove-orphans'
requires = "docker"

[[skip_lifecycle]]      # after a clone, do not run this script of this project...
dir = "api"             # (relative to the worktree root; default ".")
script = "postinstall"
# ...when every pair of files (relative to the project) is byte-identical
if_equal = [["schema.json", "node_modules/.codegen/schema.json"]]

[pool]                  # keep this many ready worktrees (see "The pool")
size = 2
```

- `run` is executed with `sh -c` in the worktree root, with
  `FAST_WORKTREE_PATH` (the worktree's absolute path) and
  `FAST_WORKTREE_MAIN` (the main checkout's absolute path) set.
- A failing `post_add` hook makes the command exit non-zero. A failing
  `pre_remove` hook is reported and the removal goes on.
- `skip_lifecycle` is for a project's own script that generates into
  `node_modules` and records its input there, as code generators commonly
  do: when the input is unchanged, the cloned `node_modules` already holds
  the output. A file that does not exist is never equal, so the script
  runs. It applies only where fast-worktree runs the script itself, after a
  clone; `npm ci` and `npm install` run scripts on their own.
- `[[clone]]` lists what each worktree gets; see
  [below](#what-each-worktree-gets-clone). Without it, the projects are
  found from the tracked lockfiles. `[[ignore]]` leaves a project out on
  purpose, with the reason ([below](#leaving-a-project-out-ignore)).
- No file, no hooks. Unknown keys are warned about and ignored (`config
  check` reports them).

## What each worktree gets: `[[clone]]`

Without any `[[clone]]` in `.fast-worktree.toml`, every tracked
`package-lock.json`, `pubspec.lock` and `Gemfile.lock` is a project, found by itself
(above). That is all a repository needs to start. Once the file has one
`[[clone]]`, only what the `[[clone]]` entries list is handled: a project
they leave out gets nothing from `add`, `install`, `check` or the hooks, and
`config check` (below) reports its lockfile.

```toml
[[clone]]
preset = "npm"            # path = "api/node_modules", key = ["api/package-lock.json"],
dir = "api"               # key_commands = ["node -v"], npm's own install and checks

[[clone]]
preset = "dart"
dir = "mobile"

[[clone]]                 # no preset: everything written out
path = "mobile/ios/Pods"                # relative to the worktree root
key = ["mobile/ios/Podfile.lock"]       # what decides whether a copy can be reused
key_commands = ["pod --version"]        # their output is part of the key
install = "pod install"                 # makes it when nothing can be cloned
verify = "test -f Manifest.lock"        # optional: must pass after a clone
rewrite_paths = true                    # optional: see below
after = ["echo pods ready"]             # optional: after a clone or an install
```

- **A preset** fills in every field. `npm` is what a detected npm project
  gets: `path = "<dir>/node_modules"`, `key = ["<dir>/package-lock.json"]`,
  `key_commands = ["node -v"]`, and `npm ci` with everything described in
  this README (the cache, other worktrees, the incremental install, the
  tree check against `node_modules/.package-lock.json`, the project's own
  install scripts after a clone, `skip_lifecycle`). `dart` is a `pub get`
  as above, with `path = "<dir>/.dart_tool"` and `key =
  ["<dir>/pubspec.lock"]`; it never clones `.dart_tool`: the packages live
  in pub's shared cache, so `pub get --offline` is quick, and most of
  `.dart_tool` is build output that holds the path of the worktree it was
  made in. `bundler` is below. `dir` defaults to `.`, the
  root.
- **`bundler`** gets `key = ["<dir>/Gemfile.lock"]` and `key_commands =
  ["ruby -v", "ruby -e 'print RUBY_PLATFORM'"]`: native extensions are
  built for one Ruby and one platform. Its directory is the one Bundler
  installs into, so that `bundle exec` finds what is there: `BUNDLE_PATH`
  from the project's `.bundle/config`, else from the environment, else
  from `~/.bundle/config` (Bundler's own order), relative to `dir`; else
  `<dir>/vendor/bundle`. It is made present as a `[[clone]]` without a
  preset is (below): from the cache, else from a worktree, else with
  `BUNDLE_PATH=<path> bundle install`. After a clone, `bundle check
  --dry-run` runs with `BUNDLE_PATH` set to the clone, and a clone it turns
  down is thrown away. (Every `bundle check` here has `--dry-run`: without
  it, Bundler writes `Gemfile.lock` when it lacks the current platform.) Nothing is rewritten after a clone: the worktree's path is only in
  the build logs (`gem_make.out`) and in the debug information of the
  compiled extensions, which load from the clone all the same. A
  `BUNDLE_PATH` outside the worktree holds gems every checkout shares:
  nothing is cloned there, `bundle check` decides and `bundle install`
  runs when it fails. The same goes for a `BUNDLE_PATH` that holds tracked
  files (`BUNDLE_PATH=vendor` next to a tracked `vendor/javascript`), and
  one that leaves the worktree through a symlink: fast-worktree never
  clones over them or moves them away. `.bundle/config` is usually ignored by git, and
  without it `bundle exec` looks for the system's gems: list it in
  [`.worktreeinclude`](#ignored-files-worktreeinclude), or set
  `BUNDLE_PATH`.
- **Any field of a preset can be written** to replace its default: `key`
  with more files (their contents are hashed with the lockfile's),
  `key_commands` (npm puts their output in the key in place of `node -v`'s,
  joined with `-`), `install` (run instead of `npm ci` or `pub get`, in
  `dir`), `verify`, `rewrite_paths`, `after`. With a preset, `path` must be
  `<dir>/node_modules` (or `<dir>/.dart_tool`); it can stand in for `dir`.
- **Without a preset**, `path` and `key` are required. `path` is made
  present by the first of:
  1. a cache entry with the same key (the contents of the `key` files and
     the output of the `key_commands`);
  2. a registered worktree whose `path` was made from the same key (its
     `.fast-worktree-key`, below, says so), or, without that record, whose
     `key` files are byte-identical, whose `key_commands` print the same
     there and where `verify` passes;
  3. `install`. Its result is stored in the cache. Without `install`,
     nothing is made, and the directory only ever comes from the cache or
     another worktree (a build cache, say).

  A clone is one `clonefile(2)` call for the whole directory (macOS only;
  elsewhere only `install` runs). After it, `verify` must exit 0 in `path`,
  or the clone is thrown away and the next source is tried. With
  `rewrite_paths`, the path of the worktree the directory was made in is
  replaced with this worktree's path in its text files (files with a NUL
  byte, and symlinks, are left alone). `key_commands`, `install` and
  `after` run in `dir`, by default the parent of `path`; `verify` runs in
  `path`. All of them run with `sh -c`, with `FAST_WORKTREE_PATH` and
  `FAST_WORKTREE_MAIN` set. Unlike `node_modules`, the directory is not
  checked for symlinks with an absolute target.

  Once the clone or the install, `verify` and `after` have all succeeded,
  the key is recorded in `<path>/.fast-worktree-key`. Only a `path` with
  that record, made from the key as it is now, counts as installed (for
  `check`, `install` and as a source for another worktree); one without it
  counts when `verify` passes in it (and is recorded then). When the
  install, `verify` or an `after` fails, `path` is moved away. `verify`
  runs in this worktree's `path` and in another worktree's without a
  record, never in a cache entry.

  A `path` that is there without that record, and that `verify` does not
  vouch for (or that has no `verify`), was not made by fast-worktree: a
  build's output or a hand-made directory, say. `install` leaves it as it
  is and says so, it is not lent to another worktree, and `check` lists it
  apart (`not made by fast-worktree, left as it is: <target>`) instead of
  asking for an install. Only `install --changed` makes it again. A `path`
  that is made again (that one, or one recorded from other key files) is
  moved away only once its replacement is complete: when nothing can
  replace it (no cache entry, no worktree to clone from, no `install`) or
  the replacement fails, it is put back.
- **What fast-worktree touches.** `path` is moved away, cloned into and
  handed to the cache whole, so it must be fast-worktree's own: a `dir`,
  `path` or `key` that leads out of the worktree through a symlink, a
  `path` that holds tracked files, and a `path` that holds another
  target's `path` or `dir` are refused (`install`, `add` and the hooks
  stop with the reason, and `config check` reports them). A project found
  by its lockfile (without any `[[clone]]`) that hits this is left out
  with a warning, and the others are handled; an
  [`[[ignore]]`](#leaving-a-project-out-ignore) leaves it out without the
  warning. `path` is looked at again right before it is moved,
  cloned into or rewritten.
- **Order.** The targets with a preset are installed first, in parallel,
  then those without one: `pod install` needs what `flutter pub get`
  generates.
- **Removal gives to the next worktree.** When `remove` (or `hook remove`)
  deletes a worktree after its checks, it first moves the `path` of each
  `[[clone]]` without a preset into the cache, by rename (the cache is
  usually on the same volume, so this is instant): when it was made from
  the key files it has now (its `.fast-worktree-key`) or passes `verify`,
  and when the cache has no entry with that key yet. This happens once the
  worktree has been moved into the trash, so a removal that stops before
  that leaves the worktree whole. The next worktree with that key clones it. A
  worktree that `hook remove` keeps in the trash, because it had changes,
  keeps its directories.

In the [example](#example), `config init` writes the presets for the
three projects, and [`pods.toml`](examples/monorepo/pods.toml) adds a
`[[clone]]` for `mobile/ios/Pods`, made by a stand-in for `pod install`:

```console
$ fast-worktree config init
add    [[clone]] preset = "npm", dir = "api"
add    [[clone]] preset = "dart", dir = "mobile"
add    [[clone]] preset = "npm", dir = "web"
note   with [[clone]] in .fast-worktree.toml, only what it lists is installed; `fast-worktree config check` finds lockfiles it leaves out
$ cat pods.toml >> .fast-worktree.toml
$ git add .fast-worktree.toml && git commit -qm "[[clone]] entries"
$ fast-worktree add ../monorepo-a -b feature/a HEAD
worktree added in 0.0s
copy   .env
npm ci api (0.8s, waited 0.2s for lifecycle lock)
pub get mobile (flutter, 0.5s)
npm ci web (0.8s)
install mobile/ios/Pods (sh fake-pod.sh install, 0.0s)
deps done in 1.0s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-a
$ fast-worktree add ../monorepo-b -b feature/b HEAD
worktree added in 0.1s
copy   .env
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-e8406f05/api/263f7cb8e0876026-v20.13.1 (0.1s)
pub get mobile (flutter, 0.3s)
clone  web from ~/Library/Caches/fast-worktree/monorepo-e8406f05/web/02ed81ddfaf462f9-v20.13.1 (0.3s)
clone  mobile/ios/Pods from ~/Library/Caches/fast-worktree/monorepo-e8406f05/mobile__ios__Pods/d6937bd63decbc2e-365864a4 (0.0s)
deps done in 0.3s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-b
$ sed "s|$HOME|~|" ../monorepo-b/mobile/ios/Pods/Target/Target.xcconfig
PODS_ROOT = ~/monorepo-b/mobile/ios/Pods
$ fast-worktree plan ../monorepo-b
plan for ~/monorepo-b
api (npm)
  from  present (node_modules)
mobile (pub)
  from  present (.dart_tool)
web (npm)
  from  present (node_modules)
mobile/ios/Pods (clone)
  from  present
```

With the cache emptied (`cache prune --older-than 0`), `remove` keeps the
`Pods` of the worktree it deletes, and the next worktree clones it:

```console
$ fast-worktree remove ../monorepo-a
hook   pre_remove: echo "bye: $(basename "$FAST_WORKTREE_PATH")"
bye: monorepo-a
cache  mobile/ios/Pods kept as ~/Library/Caches/fast-worktree/monorepo-e8406f05/mobile__ios__Pods/d6937bd63decbc2e-365864a4
removed ~/monorepo-a (moved to ~/Library/Caches/fast-worktree/trash/1793049431-73174-0735852000-monorepo-a, deleting in background)
$ fast-worktree add ../monorepo-c -b feature/c HEAD
worktree added in 0.1s
copy   .env
note   api: postinstall skipped, inputs unchanged
clone  api from ~/monorepo-b/api/node_modules (0.2s)
pub get mobile (flutter, 0.3s)
clone  web from ~/monorepo-b/web/node_modules (0.4s)
clone  mobile/ios/Pods from ~/Library/Caches/fast-worktree/monorepo-e8406f05/mobile__ios__Pods/d6937bd63decbc2e-365864a4 (0.0s)
deps done in 0.4s
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-c
```

### Tools without a preset

A tool fast-worktree has no preset for gets the same from a `[[clone]]`
without one: the directory it installs into, the files and the tool
version that decide whether a copy can be reused, and the command that
brings a copy up to date. These have been tried:

```toml
[[clone]]                 # yarn classic (1.x)
path = "node_modules"
key = ["yarn.lock"]
key_commands = ["node -v"]
install = "yarn install --frozen-lockfile"
after = ["yarn install --frozen-lockfile"]

[[clone]]                 # Composer
path = "vendor"
key = ["composer.lock"]
key_commands = ["php -v"]
install = "composer install"
after = ["composer install"]

[[clone]]                 # Rust (Cargo)
path = "target"
key = ["Cargo.lock"]
key_commands = ["rustc -V"]
```

- **yarn classic** checks the tree against `yarn.lock` itself, so the
  `yarn install --frozen-lockfile` after a clone finds nothing to do and
  returns at once.
- **Composer** writes the PHP version into
  `vendor/composer/platform_check.php`, hence `php -v` in the key; the
  `composer install` after a clone writes the autoloader again for the
  worktree it is in.
- **Cargo** needs nothing after a clone: its fingerprints decide what is
  current and rebuild the rest. Without an `install`, `target` comes only
  from the cache or another worktree, and `remove` hands it to the next
  worktree.
- For yarn berry, bun and pnpm, cloning `node_modules` gained little.

### Leaving a project out: `[[ignore]]`

A project that no worktree needs (a site built only in CI, a sample) is
left out with an `[[ignore]]`, which says why:

```toml
[[ignore]]
dir = "docs-site"
reason = "built in CI only"

[[ignore]]                # only the Bundler side of a project that has npm too
dir = "web"
preset = "bundler"
reason = "the Ruby tools are not used in worktrees"
```

`reason` is required: a file without one is an error, so the reason is
always written next to what it leaves out. Without `preset`, every
lockfile of `dir` is left out. Without any `[[clone]]`, the projects an
`[[ignore]]` names are not detected; with `[[clone]]` entries, a project
they do not list gets nothing anyway, and the `[[ignore]]` keeps `config
check` from reporting it and `config init` from adding it.

### `config init` and `config check`

```sh
fast-worktree config init [--dry-run]
fast-worktree config check [--staged]
```

`config init` adds a `[[clone]]` with its preset for each tracked lockfile
that no `[[clone]]` or `[[ignore]]` names yet, at the end of `.fast-worktree.toml` (made if
missing). What the file already says, comments included, is kept. With
`--dry-run`, it prints the entries and writes nothing.

`config check` exits 1 and lists what it finds:

- `unlisted`: a tracked lockfile of a preset (`package-lock.json`,
  `pubspec.lock`, `Gemfile.lock`) that no `[[clone]]` or `[[ignore]]` names;
- `missing`: a `[[clone]]` whose lockfile or `key` file does not exist, or
  an `[[ignore]]` whose directory has no tracked lockfile (of its
  `preset`, when it names one);
- `untracked`: a `[[clone]]` whose `key` file exists but is not tracked
  (another worktree has its own, or none);
- `invalid`: a mistake in the file, such as a file that does not parse, an
  unknown key or section (`[[clones]]`), an `[[ignore]]` without a
  `reason`, an unknown preset, a `path` that does not fit its preset, a
  `[[clone]]` without a preset that lacks `path` or `key`, a project that
  both a `[[clone]]` and an `[[ignore]]` name, or a target fast-worktree
  would refuse to touch (a symlink out of the worktree, a `path` that holds
  tracked files or another target).

It runs a few `git ls-files` (the index, so a lockfile that is only
staged counts) and looks at which files exist, so it is quick enough for
a pre-commit hook. In a repository without any `[[clone]]` it does not
compare the lockfiles, but still reports the other mistakes, and says
that the projects are detected; without a `.fast-worktree.toml` it says
so and exits 0.

With `--staged`, it checks what the next commit has: `.fast-worktree.toml`
is read from the index, and a file "exists" when it is in the index. A
commit that stages a new lockfile but not the `[[clone]]` added for it,
or the other way round, fails, where the files on disk would pass. What
fast-worktree would refuse to touch (a symlink out of the worktree,
tracked files under a `path`) is still looked at on disk. After a new
project was added in the example:

```console
$ mkdir tools && cp web/package.json web/package-lock.json tools/ && git add tools
$ fast-worktree config check
unlisted   tools/package-lock.json (npm) is in no [[clone]]; `fast-worktree config init` adds it
1 problem in .fast-worktree.toml
```

As a pre-commit hook (`.git/hooks/pre-commit`, or the file your hook
manager runs), skipped where fast-worktree is not installed:

```sh
#!/bin/sh
command -v fast-worktree >/dev/null 2>&1 || exit 0
exec fast-worktree config check --staged
```

When the hook goes on after the check, take its exit code with `||`. Some
hook managers (husky among them) run the file with `sh -e`, where
`fast-worktree config check --staged; rc=$?` stops the whole hook at the
failing command, before `rc` is set. A version of fast-worktree without
`config check --staged` exits 2 (a usage error), which such a hook can
let through:

```sh
rc=0
fast-worktree config check --staged || rc=$?
if [ "$rc" -eq 1 ]; then
  exit 1
fi
# 2: an older fast-worktree that does not know the command; go on
```

In CI (GitHub Actions):

```yaml
- run: cargo install fast-worktree --locked
- run: fast-worktree config check
```

### `plan`

```sh
fast-worktree plan [<path>]
```

For each target, where its files would come from (`cache <entry>`,
`worktree <path>`, `delta from <entry>` for npm's incremental install,
`install (<command>)`, or `present`), and below it, one `not` line for
each source passed over and why (no cache entry for the key, another key
file or tool version in a worktree, `verify` failing...). A clone that
`verify` checks once it is made says `(verify after the clone)`: `plan`
does not run it in a cache entry. `plan` moves, clones and installs
nothing, but it does run commands: the `key_commands` (`node -v` for npm),
and `verify` in this worktree and in another worktree whose `path` has no
record of its key. These, like `install` and `after`, are shell commands
the repository writes, and the [post-checkout hook](#post-checkout-hook)
runs them too: they are trusted as the `post_add` hooks are, so look at a
repository's `.fast-worktree.toml` before you run fast-worktree in it.
With `<path>`, it is for
that worktree as it is; without, for a new worktree with the current
worktree's files (a new branch from origin's default branch may have other
lockfiles), where nothing is present yet and the current worktree is one
to clone from. In the example, before the first `add`:

```console
$ fast-worktree plan
plan for a new worktree with the files of ~/monorepo
api (npm)
  from  install (npm ci)
  not   cache: no entry 263f7cb8e0876026-v20.13.1
  not   worktree ~/monorepo: no node_modules that matches package-lock.json
  not   delta: no older cache entry for v20.13.1
mobile (pub)
  from  install (flutter pub get)
  not   clone: pub keeps packages in a cache of its own
web (npm)
  from  install (npm ci)
  not   cache: no entry 02ed81ddfaf462f9-v20.13.1
  not   worktree ~/monorepo: no node_modules that matches package-lock.json
  not   delta: no older cache entry for v20.13.1
mobile/ios/Pods (clone)
  from  install (sh fake-pod.sh install)
  not   cache: no entry d6937bd63decbc2e-365864a4
  not   worktree ~/monorepo: no mobile/ios/Pods
```

and after the `remove` above, with the npm entries gone from the cache:

```console
$ fast-worktree plan
plan for a new worktree with the files of ~/monorepo
api (npm)
  from  worktree ~/monorepo-b/api/node_modules
  not   cache: no entry 263f7cb8e0876026-v20.13.1
  not   worktree ~/monorepo: no node_modules that matches package-lock.json
mobile (pub)
  from  install (flutter pub get)
  not   clone: pub keeps packages in a cache of its own
web (npm)
  from  worktree ~/monorepo-b/web/node_modules
  not   cache: no entry 02ed81ddfaf462f9-v20.13.1
  not   worktree ~/monorepo: no node_modules that matches package-lock.json
mobile/ios/Pods (clone)
  from  cache ~/Library/Caches/fast-worktree/monorepo-e8406f05/mobile__ios__Pods/d6937bd63decbc2e-365864a4 (verify after the clone)
```

## Ignored files: `.worktreeinclude`

A `.worktreeinclude` in the main checkout's root lists, in `.gitignore`
syntax, files to copy from the main checkout into each worktree `add`
creates. It is the format Claude Code reads for its own worktrees:

```text
.env
.env.local
config/secrets.json
```

Only files that match a pattern **and** are gitignored in the main
checkout are copied, so tracked files and untracked files that git does
not ignore never are. Nothing that exists in the new worktree is
overwritten. A directory that git ignores as a whole (such as
`node_modules/`) is looked into only when a pattern can reach it: the
directory itself matches, an anchored pattern starts with its path (write
`vendor/**/config.json`, not `**/config.json`), or, for a pattern starting
with `**/` (or without a slash), the first name after `**/` is one of the
directory's names. Another repository is never copied: a registered
worktree, or a directory with a `.git` of its own, is left out. A matching
directory with one inside (such as `.claude/` holding
`.claude/worktrees/<name>/`) is copied entry by entry around it. Copies are
clones on macOS.

## Post-checkout hook

`check --post-checkout` takes the three arguments git passes to a
`post-checkout` hook. Add it to the repository's hook (`.git/hooks/post-checkout`,
or the file your hook manager runs):

```sh
#!/bin/sh
command -v fast-worktree >/dev/null 2>&1 && fast-worktree check --post-checkout "$@" || true
```

Where fast-worktree is not installed, the hook does nothing.

- After `git worktree add` or `git clone` (the previous commit is all zeros
  and the flag is 1), the dependencies that the cache or another worktree
  can provide are cloned in, with the project's own lifecycle scripts as
  after any clone (so it runs `node -v` and those scripts, and the
  `key_commands`, `verify` and `after` of the `[[clone]]` entries: the
  repository's own commands, trusted as `post_add` hooks are). A
  `.fast-worktree.toml` that cannot be read makes it say so and do
  nothing. Nothing that
  downloads or resolves runs: projects that would need `npm ci`, `npm
  install` or `pub get` are listed as missing. (A fresh `git clone` has no
  cache entries or other worktrees of its own yet, so there it only
  lists.)
- On any other checkout, such as `git switch`, it lists what is missing,
  like `check --quiet`.
- It always exits 0, also when its output goes to a pipe that is already
  closed. With the environment variable `CI` set, or
  `FAST_WORKTREE_SKIP=1`, it does nothing.

## The pool

A worktree made ahead of time, with its dependencies installed, is ready
in the time it takes to rename a directory and switch a branch. The pool
keeps such spares, and `add` takes one instead of creating a worktree.

It is off by default, because spares take disk space. Turn it on with a
size, for every repository in your user config,
`$XDG_CONFIG_HOME/fast-worktree/config.toml` (default
`~/.config/fast-worktree/config.toml`, on macOS too):

```toml
[pool]
size = 1
```

or for one repository in the main checkout's `.fast-worktree.toml`, which
wins over the user config. `size = 0` turns it off.

- **Spares.** `pool fill` fetches origin's default branch (for at most 60
  seconds, else as last fetched) and, until there are `size` spares, runs
  `git worktree add --detach` of it, `install`, and `git worktree lock
  --reason "fast-worktree pool"`. A spare is only put in place once it is
  complete. The repository's own git hooks do not run for that `git
  worktree add` (it gets an empty directory as `core.hooksPath`): what a
  `post-checkout` hook sets up for a worktree belongs to the worktree the
  spare becomes, and it runs there, on the `git switch` of `add`.
- **Taking one.** `add` renames a spare to a name of its own (two `add`s
  never get the same one: the rename either succeeds or finds it gone),
  unlocks it, `git worktree move`s it to `<path>`, and `git switch -c
  <branch> <start-point>`s there. Then `install --changed` installs again
  only the projects whose lockfile (or Node version) differs, and the other
  projects run their own install steps again where the worktree is now
  (below). The `.worktreeinclude` files and the `post_add` hooks follow, as
  for any `add`.
- **Made in one place, used in another.** A spare is made in the pool and
  moved to `<path>` when it is taken. It cannot be made where it will be
  used: that path is only known when `add` runs. So whatever its install
  wrote by absolute path points into the pool. Against that, a spare keeps
  no `node_modules` with a symlink whose target is absolute (the `add`
  that takes the spare installs that project again), and after the move
  each npm project's own `preinstall` / `install` / `postinstall` /
  `prepare` run again (`skip_lifecycle` rules apply). In each Dart
  project, the path where the spare was made is replaced with the new one
  in the ignored text files under `.dart_tool/`, `ios/Flutter/`,
  `macos/Flutter/ephemeral/`, `linux/flutter/ephemeral/`,
  `windows/flutter/ephemeral/` and in `.flutter-plugins*`, where `flutter
  pub get` writes the project's path (`FLUTTER_APPLICATION_PATH` in
  `ios/Flutter/Generated.xcconfig`, for example). `pub get` itself would
  not: it leaves those files alone while they are current. Anything else
  that records its own absolute path is not rewritten.
- **Refilling.** When `add` is done, whether it took a spare or found
  none, it starts `pool fill` in the background; after `hook create`, the
  fill first waits for the background install. So the refill never
  installs at the same time as the command that started it. Only one fill
  runs per repository at a time (a lock in the pool directory); its output
  goes to `fill.log` there.
- **No spare ready**, because a fill is still busy for example: `add`
  creates the worktree as usual, counts a miss, and starts the refill as
  above. If the spare cannot be moved (`<path>` on another file system),
  it is put back and the worktree is created as usual.
- A spare made at an older commit still works: the difference is
  installed on top. One that is far behind takes as long as a fresh
  worktree; `pool refresh` replaces the spares that are behind.

```sh
fast-worktree pool status    # spares: commit, how far behind origin, when made; misses
fast-worktree pool fill      # make spares up to the size
fast-worktree pool refresh   # fetch, drop the spares behind origin, fill again
fast-worktree pool clear     # remove every spare
```

In the example repository, with the cache already filled by an earlier
`add`:

```console
$ cat ~/.config/fast-worktree/config.toml
[pool]
size = 1
$ fast-worktree pool fill
2026-10-26 13:03 fill  started
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/api/263f7cb8e0876026-v20.13.1 (0.1s)
pub get mobile (flutter, 0.3s)
clone  web from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/web/02ed81ddfaf462f9-v20.13.1 (0.3s)
deps done in 0.4s
2026-10-26 13:03 spare wt-1793019788-59566-0 at 005cb21 ready (0.4s)
2026-10-26 13:03 fill  done: 1 of 1 ready
$ fast-worktree add ../monorepo-b -b feature/b
worktree from the pool in 0.1s
copy   .env
skip   api (up to date)
skip   mobile (up to date)
skip   web (up to date)
deps done in 0.1s
note   api: postinstall skipped, inputs unchanged
rerun  web: postinstall
hook   post_add: echo "ready: $(basename "$FAST_WORKTREE_PATH")"
ready: monorepo-b
$ fast-worktree pool status
pool   1 of 1 ready (size from [pool] in ~/.config/fast-worktree/config.toml)
ID                     COMMIT   BEHIND origin/main  CREATED
wt-1793019788-59890-0  005cb21  0                   2026-10-26 13:03 UTC
misses 0 (an add found no spare ready)
$ git -C ../upstream commit -q --allow-empty -m "move on"
$ fast-worktree pool refresh
drop   wt-1793019788-59890-0 (behind origin/main)
dropped 1 spare(s) behind origin/main
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/api/263f7cb8e0876026-v20.13.1 (0.1s)
pub get mobile (flutter, 0.3s)
clone  web from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/web/02ed81ddfaf462f9-v20.13.1 (0.3s)
deps done in 0.3s
2026-10-26 13:03 spare wt-1793019789-60373-0 at 50c46ff ready (0.4s)
2026-10-26 13:03 fill  done: 1 of 1 ready
$ fast-worktree pool clear
removed 1 spare(s) (deleting in background)
```

(The refill after `add` ran in the background before `pool status`.)
`remove` refuses a spare, since it is locked, and points to `pool clear`.

## Claude Code hooks

Claude Code creates a worktree for `claude --worktree <name>`, for a
subagent with `isolation: "worktree"` and for background sessions. With a
[WorktreeCreate hook](https://code.claude.com/docs/en/hooks#worktreecreate)
it lets the hook create it instead, and with a WorktreeRemove hook remove
it. `fast-worktree hook print-config` prints the two hooks for a settings
file (`~/.claude/settings.json`, or a project's `.claude/settings.json`);
it changes no file itself:

```console
$ fast-worktree hook print-config
{
  "hooks": {
    "WorktreeCreate": [
      { "hooks": [{ "type": "command", "command": "fast-worktree hook create" }] }
    ],
    "WorktreeRemove": [
      { "hooks": [{ "type": "command", "command": "fast-worktree hook remove" }] }
    ]
  }
}
```

If the settings file has a `"hooks"` object already, add the two events to
it. Add `.claude/worktrees/` to `.gitignore`, as Claude Code suggests for
its own worktrees.

**`hook create`** reads the hook input on stdin (`name`, and `cwd`, from
which it finds the main checkout) and creates the worktree where Claude
Code would: `<main checkout>/.claude/worktrees/<name>`, on a new branch
`worktree-<name>` from origin's default branch, fetched (for at most 5
seconds, else as last fetched; from `HEAD` when there is no origin), as [Claude Code's
documentation](https://code.claude.com/docs/en/worktrees) describes its own.
If that branch exists (a previous `hook remove` kept it), it is checked
out; if the worktree exists, it is used as it is.

- It prints the worktree's absolute path as the last line of stdout;
  everything else goes to stderr. The path is a real path, so it never
  passes through a symlink inside the repository (Claude Code refuses
  those).
- It works like `add`: a spare from the pool when there is one, the
  `.worktreeinclude` files (Claude Code does not copy them when a hook
  creates the worktree), the dependencies, the `post_add` hooks.
- A hook holds up the start of the session, so only what can be cloned is
  installed while Claude Code waits. The rest (`npm ci`, incremental
  installs, `pub get`) is left to `fast-worktree install --changed`, started
  in the background, which then runs the `post_add` hooks. `fast-worktree
  wait [<path>]` waits for it and exits 1 if it failed; `check` says it is
  running.
- It exits non-zero only when git cannot create the worktree. Dependencies
  or `post_add` hooks that fail are warnings, and the path is printed.
- It always starts from origin's default branch. It does not read Claude
  Code's `worktree.baseRef` setting.
- The fetch never waits for a password, a passphrase or a host key prompt:
  it runs with `GIT_TERMINAL_PROMPT=0` and `-o BatchMode=yes` added to the
  ssh command git would use (`GIT_SSH_COMMAND`, else `core.sshCommand`,
  else `ssh`; one named by `GIT_SSH` alone is left as it is). So do the
  fetches of `add` and `pool fill`. The background install runs in a
  session of its own, without a terminal.
- Claude Code waits for the hook's output to end. A `post_add` hook that
  runs while it waits (when nothing is left to the background) and leaves
  a process behind that still holds stderr, such as `some-server &`, can
  keep Claude Code waiting. Redirect such a process's output (`some-server
  >/dev/null 2>&1 &`).

**`hook remove`** reads `worktree_path`. Claude Code takes exit 0 for "the
worktree is gone", and calls the hook only after you agreed to remove the
worktree (or with `discard_changes`), so it always deletes it:

- It refuses (exit 1, nothing deleted, no hook run) a path that is not a
  linked worktree of a repository, the main checkout, a spare of the pool,
  a worktree you locked (`git worktree lock`; a lock the pool left is
  fine), one that cannot be renamed away (as for `remove`), and one with a
  submodule git directory that cannot go with it (below).
- If its background install is still running, it stops it first, with
  everything it started (`SIGTERM`, then `SIGKILL` after 3 seconds), so
  that nothing writes into the worktree once it is gone. It signals the
  process the marker names only while that process holds the install's
  lock; when the lock cannot be checked (a file system without locks), it
  waits 3 seconds for the install to end and otherwise says it could not
  stop it.
- It runs the `pre_remove` hooks, moves the worktree into the trash and
  removes its own entry from git, and no other (it does not run `git
  worktree prune`, as for `remove`). When the path is already gone, it
  removes the one entry registered at that path, if git has exactly one.
- It keeps the worktree in the trash for at least 24 hours, and prints the
  command to get it back, when it has uncommitted or untracked changes (as
  `remove` counts them, whatever `status.showUntrackedFiles` says), when it
  has a submodule checked out (whether its git directory is in the
  worktree's entry, in the main repository or inside the submodule), or
  when another registered worktree is inside it. Submodules keep their git
  directories in the worktree's entry, which goes; each one's is moved
  into the submodule as its `.git` first, so their commits go with it (`git
  submodule absorbgitdirs` moves them back after a restore). If one of them
  cannot be moved, those already moved are put back, nothing else changes
  and it exits 1. A worktree inside is not deleted: it stays registered
  where it is now, in the trash, and the command to move it out is printed.
- Commits that would be lost are kept in refs of the main repository, so
  that gc does not take them and they outlive the trash entry. Each removal
  gets a group, `refs/fast-worktree/kept/<name>-<UTC date>-<time>`:
  - `<group>/head`: the worktree's `HEAD`, when the worktree is kept and
    `HEAD` is not on a branch that stays (a detached `HEAD`, or
    `worktree-<name>` when it is deleted), and when a detached `HEAD` is on
    no other ref. A worktree inside that is detached gets a group of its
    own.
  - `<group>/sub/<path>`: the `HEAD` of each checked-out submodule of a
    kept worktree, fetched into the main repository first (if that fails,
    it says so).

  The refs are named in the output, with the day each is deleted. One
  whose commit a local branch or a tag has is deleted after 25 hours; any
  other after 30 days, and its commits can then be dropped by `git gc`;
  either way a line on stderr says so, with the ref, the commit and why (see [What is deleted by
  itself](#what-is-deleted-by-itself)). `fast-worktree kept ls` lists them
  and `fast-worktree kept prune [--older-than <days>]` applies the same
  rules at once (default: 30 days; `git update-ref -d <ref>` deletes one). They are not pushed by `git push` of a
  branch, but `git push --mirror` pushes every ref, these included.
- Claude Code never deletes the branch of a worktree a hook created. `hook
  remove` deletes `worktree-<name>` only when its commit is on its
  upstream, a remote branch or the default branch; otherwise it keeps it
  and says so. Other branches the worktree was on are kept.

In the example repository, with the pool off. The last line `hook create`
prints is the absolute path; here the home directory in it is shortened
to `~`:

```console
$ echo '{"name":"demo","cwd":"'"$PWD"'"}' | fast-worktree hook create
worktree added in 0.0s
copy   .env
note   api: postinstall skipped, inputs unchanged
clone  api from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/api/263f7cb8e0876026-v20.13.1 (0.1s)
later  mobile (installed in the background)
clone  web from ~/Library/Caches/fast-worktree/monorepo-9da4d4fc/web/02ed81ddfaf462f9-v20.13.1 (0.3s)
deps done in 0.3s
later  started; `fast-worktree wait` waits for it
~/monorepo/.claude/worktrees/demo
$ fast-worktree wait .claude/worktrees/demo
waiting for the background install in ~/monorepo/.claude/worktrees/demo
background install done
$ (cd .claude/worktrees/demo && git branch --show-current && fast-worktree check && cat .env)
worktree-demo
deps ok (3 projects)
API_TOKEN=dev-only
$ echo wip > .claude/worktrees/demo/notes.txt
$ echo '{"worktree_path":"'"$PWD"'/.claude/worktrees/demo"}' | fast-worktree hook remove
hook   pre_remove: echo "bye: $(basename "$FAST_WORKTREE_PATH")"
bye: demo
removed ~/monorepo/.claude/worktrees/demo (moved to ~/Library/Caches/fast-worktree/trash/keep-1793106191-1793019791-61112-0116067000-demo)
deleted branch worktree-demo
fast-worktree: it had uncommitted changes; it is kept in ~/Library/Caches/fast-worktree/trash/keep-1793106191-1793019791-61112-0116067000-demo for at least 24 hours
to restore them: git -C ~/monorepo worktree add ~/monorepo/.claude/worktrees/demo refs/fast-worktree/kept/demo-20261026-130311/head && rsync -a --exclude=/.git ~/Library/Caches/fast-worktree/trash/keep-1793106191-1793019791-61112-0116067000-demo/ ~/monorepo/.claude/worktrees/demo/
fast-worktree: its HEAD 50c46ff4cb8f is kept as refs/fast-worktree/kept/demo-20261026-130311/head
fast-worktree: `fast-worktree kept ls` lists such refs. Each is deleted by itself on 2026-11-25 (30 days from now), and what it holds can then be dropped by `git gc`; sooner only when a branch or tag has its commit. Make a branch of what you want to keep: git branch <name> <commit>
```

`worktree-demo` was deleted because its commit is on `origin/main`. A
worktree with a commit of its own keeps its branch:

```console
$ echo '{"worktree_path":"'"$PWD"'/.claude/worktrees/work"}' | fast-worktree hook remove
hook   pre_remove: echo "bye: $(basename "$FAST_WORKTREE_PATH")"
bye: work
removed ~/monorepo/.claude/worktrees/work (moved to ~/Library/Caches/fast-worktree/trash/1793019792-61600-0225457000-work)
fast-worktree: kept branch worktree-work: commit 21f7c601e324 is not on its upstream, a remote branch or the default branch
```

## Where things live

- Cache: `~/Library/Caches/fast-worktree` on macOS, else
  `$XDG_CACHE_HOME/fast-worktree` (default `~/.cache/fast-worktree`).
  `FAST_WORKTREE_CACHE` overrides it. Entries are
  `<repo>-<hash>/<project>/<lockfile sha256, 16 digits>-<node -v>`; the
  repository part is the main checkout's name plus a hash of its real path,
  so two checkouts with the same name never share entries. Next to an
  entry, `<entry>.ok` records that it was checked when stored (below).
  Only a tree whose `.package-lock.json` matches the lockfile is stored.
  A `[[clone]]` without a preset, and a Bundler project, has its entries under
  `<repo>-<hash>/<path, with / as __>/<key files sha256, 16 digits>-<key
  commands' output sha256, 8 digits>`, and `<entry>.src` records the
  worktree an entry was made in, for `rewrite_paths`.
  Entries unused for 7 days are removed, in every project of every
  repository, whether or not it is installed again; `cache prune` does it
  on demand (see [What is deleted by itself](#what-is-deleted-by-itself)).
  Clones share blocks with their source, so the cache costs almost no disk
  while worktrees made from it exist.
- `node_modules/.fast-worktree-key`, `.dart_tool/fast-worktree-key` and,
  for a `[[clone]]` without a preset and a Bundler project,
  `<path>/.fast-worktree-key` record
  what an install was made from, for `install --changed` (and `remove`). A tree
  without one is compared with its lockfile.
- Trash: `<cache>/trash`. When the trash is on another volume than the
  directory being deleted, that directory is renamed to a hidden
  `.fast-worktree-trash-*` sibling instead, and a note in the trash records
  where. Every `add`, `remove`, `install`, `pool fill` and library call
  deletes again whatever an interrupted background deletion left in the
  trash or in a sibling it has a note for. Entries named
  `keep-<unix time>-...`
  (worktrees `hook remove` kept) are left alone until that time.
- `refs/fast-worktree/kept/*` in the repository: commits `hook remove`
  kept (above); `kept ls` and `kept prune` list and delete them. `git push
  --mirror` pushes them.
- Pool: `<cache>/<repo>-<hash>/pool`, with one directory per spare
  (`wt-<unix time>-<pid>-<n>`), `fill.log`, `fill.lock` and `misses` (one
  line per `add` that found no spare, the last 7 days).
- The background install of `hook create` keeps its marker
  (`fast-worktree-installing`), lock, log and result in the worktree's own git
  directory (`.git/worktrees/<name>/` of the repository), not in the
  worktree, where `git status` would show them.

## What is deleted by itself

Nothing that fast-worktree keeps after a command has ended grows without
bound, and no command has to be run to clean up. The rules run from the
commands that write to the same place (`add`, `remove`, `hook remove`,
`install`, `pool fill`, and the library calls that do the same), not from a
daemon. `cache prune`, `kept prune` and `pool clear` apply the same rules at
once.

- **Every command** starts one background deleter for what an interrupted
  one left in the trash (none while one is at work), and, in its own
  repository, notes that the repository is in use (the modification time of
  `<cache>/<repo>-<hash>/.main`) and discards the spares beyond the pool
  size.
- **At most once an hour, in its own repository** (stamped in
  `<cache>/<repo>-<hash>/.retention-stamp`), a command deletes the kept refs
  that are no longer needed, the pool's leftovers, and the registrations of
  spares that are no longer there. This part runs git and may wait for that
  repository's locks, so it is never done for another repository; and it
  says what it deletes, so `pool fill`, which mostly runs in the background
  with its output in a log file, leaves it (and its hour) to the next
  command.
- **At most once an hour, for the whole cache** (stamped in
  `<cache>/.retention-stamp`), whichever command comes first walks every
  repository's directory. This walk must not delay the command that makes
  it: it reads and renames only inside the cache, takes locks only without
  waiting (a busy one is left for the next walk), runs no git, moves at
  most 200 things, and starts one deleter for all of them. When work is
  left, the next command goes on, not the next hour's. It never looks at
  where another repository is: a network mount that is down would hold the
  command for ever.
- **The repositories that ran no command for 7 days** are handed by that
  walk to one detached, low-priority process (`fast-worktree cache retire`,
  at most 200 of them, each at most once a day while its directory stays).
  Only this process looks at their checkouts, and nobody waits for it. It
  takes every lock without waiting, and looks again once it holds them.

A sweep never fails the command that made it. A stamp that cannot be written
is said once, and the walk is then made by every command.

| What | Where | Kept | Deleted by |
|---|---|---|---|
| Cache entry, with its `.ok` and `.src` | `<cache>/<repo>/<project>/<key>` | until unused for 7 days (its modification time is its last use, set when it is cloned); the next install clones from it | the hourly walk (every project of every repository), `install` of that project, `cache prune` |
| Half-made entry `<key>.tmp-<pid>-<n>` | the project directory | until its run is gone (the pid is not alive) and it was made 10 minutes ago, or 7 days ago whatever the pid. Its age is when its inode last changed: the modification time of a clone is that of its source | the same |
| Pool spare | `<cache>/<repo>/pool/wt-*` | while the pool is on, it is within `[pool] size`, and a command ran in its repository in the last 7 days: the next `add` there takes it | `add` (it is used); the next command in the repository when the size was lowered or the pool turned off; the detached process once the repository ran no command for 7 days; `pool refresh`, `pool clear` |
| Pool `.new-*`, `.no-hooks-*` | the pool | while the fill that makes them is alive (the pid in the name) and for 10 minutes after they were last touched (a pid says nothing about a fill in a container that shares the cache); a day at most | the start of the next `pool fill`, the repository's hourly sweep, `pool clear`; the detached process once the repository ran no command for 7 days |
| Pool `.claimed-*` | the pool | while the `add` that took the spare is alive and for 10 minutes after it took it; a day at most | the same |
| `pool/misses` | the pool | the last 7 days: `pool status` shows how often an `add` found no spare | trimmed by each `add` that adds to it, by the repository's hourly sweep, and by the detached process |
| `pool/fill.log` | the pool | 7 days after the last fill wrote to it, and no longer than the repository is in use; started afresh when it passes 1 MiB | the repository's hourly sweep; the detached process |
| Registration of a spare whose directory is gone | `<git dir>/worktrees/wt-*` | until the next command in the repository | the repository's hourly sweep |
| Trash entry | `<cache>/trash` | deleted at once in the background; a leftover of a killed deleter after 10 minutes, a `keep-` entry (a worktree `hook remove` kept) for 24 hours | the next fast-worktree command after those times |
| Hidden sibling `.fast-worktree-trash-*` | next to a removed worktree on another volume than the cache | as a trash entry | the same, by the note the trash keeps of it, which goes with it; without a note, the next removal next to it and the repository's hourly sweep, by its name |
| Kept ref whose commit a local branch or a tag has | `refs/fast-worktree/kept/*` | 25 hours: it protects nothing, but the removal that made it may be running, and the restore command for a worktree kept in the trash names it | the repository's hourly sweep (never `pool fill`), `kept prune`; a line on stderr names the ref, the commit and the branch or tag |
| Any other kept ref (nothing has its commit, or only a remote-tracking branch) | the same | 30 days (the date in the ref's name) | the same; the line says that the 30 days passed and that `git gc` may drop the commit |
| Lock files: `.store.lock` (one for each project), `lifecycle.lock`, `pool/fill.lock`; the directories they are in; `.main` and `.retention-stamp` | `<cache>/<repo>` | until the repository is gone (below): a few small files and empty directories for a repository | the detached process, once the repository ran no command for 7 days and its main checkout was deleted: the whole `<cache>/<repo>` at once |
| Registration lock | `<git dir>/fast-worktree-registration.lock` | one empty file, as long as the repository | deleting the repository |
| `<cache>/trash/.lock`, `<cache>/.retention-stamp` | the cache | two empty files, for ever | nothing |

**Lock files are never unlinked while their repository is there.** Versions
0.4.0 to 0.6.0 take each of these locks on the file they opened and do not
look at the path again, and several versions can run on one machine. Had a
newer one unlinked a lock file that an older one waited on, the older one
and the next to come would both hold "the lock". So the lock files stay, and
the directories that hold them: their number is fixed by the number of
repositories and projects, and does not grow with use.

**A repository that is not in use** ran no command for 7 days: the
modification time of its `.main`, which every command renews, and of its
pool directory, which a fill or an `add` of an older version changes too.
Its spares are discarded then, with what a fill left half made, `fill.log`
and `misses`: a spare is there for the next `add` of its repository, and 7
days is also how long a cache entry stays that nothing cloned from, so past
it an `add` there starts from nothing either way (the first one makes a
worktree the slow way and starts a fill). When the checkout is there, they
are discarded as any spare is, unregistered and deleted. When it cannot be
reached, only the directories go, and the registrations are dropped by the
repository's next command.

**A repository that is gone** is one that is not in use and whose main
checkout was deleted. `.main` records where the checkout is (the
directory's name has only a hash of that path) and the device numbers of the
checkout, of the directory it is in and of the cache. All of this must hold
before `<cache>/<repo>` is deleted, and anything that cannot be read keeps
it:

1. no command ran in it for 7 days;
2. `.main` has the device numbers (a record without them, or a directory of
   the cache with no `.main` at all, that only versions up to 0.6.0 wrote
   to, stays until a command of this version runs in its repository: no
   length of time tells a deleted checkout from one on a disk that was not
   plugged in);
3. the checkout was not itself the root of a mount;
4. the directory the checkout was in is there, on the same device as then
   (the same device as the cache, when both were on one);
5. the checkout's path does not exist.

So these are not "gone":

- A disk mounted at `/mnt/data` that is not mounted, a network or FUSE
  mount that is down: the mount point is there, but on another device (4).
  On macOS a volume under `/Volumes` takes its mount point with it (4, or
  3 when the checkout is the volume's root).
- A cache shared with another machine or a container, where the path only
  exists on the other side: nothing happens while that side uses it (1),
  and afterwards the directory it was in is missing or on another device
  (4). Device numbers of two machines can be equal by chance; then only
  what can be made again is lost (entries and spares).
- A checkout whose path cannot be looked at (no permission, an I/O error).

A checkout that was **moved** cannot be told from a deleted one, and after 7
days the directory of its old path goes. Its commands use another directory
of the cache from the first one on; the spares that stayed behind are still
registered in the repository (locked, so `git worktree prune` keeps them),
and the repository's next command after they are gone drops exactly those
registrations: locked with the reason fast-worktree gives its spares
(`fast-worktree pool`, the same in every version), recorded at a spare's
place in this cache, with nothing there. A worktree that you or another
tool locked is never touched.

The detached process is `fast-worktree` itself. A program that embeds the
library starts it with its helper executable (`with_helper`); with no
helper, or one older than this version, the repositories that are not in
use stay as they are until a `fast-worktree` command of this version runs.

**What a kept ref's deletion loses.** `hook remove` keeps in
`refs/fast-worktree/kept/` what would otherwise go with the worktree: the
`HEAD` of a worktree with uncommitted changes whose branch it deleted, a
detached `HEAD` that no branch has, and the `HEAD` of each checked-out
submodule, fetched into the main repository. The worktree's reflog is gone
with the worktree.

- A ref whose commit a local branch or a tag has is deleted 25 hours after
  the removal. Nothing is lost: the branch or tag holds the commit, and
  only you delete those. The line that says so names it.
- Any other ref is deleted 30 days after the removal (the line that names
  the ref when it is made says the date). From then on nothing of
  fast-worktree refers to those commits, and unless a branch or a tag has
  them `git gc` can drop them at once. A commit that only a remote-tracking
  branch has counts as held by nothing: that branch goes when somebody
  deletes or rewrites it on the remote and you fetch with `--prune`, so the
  ref stays its 30 days.

No kept ref is deleted without a line on stderr that says which, which
commit and why, and none is deleted by the background `pool fill`, whose
output nobody reads. A program that embeds the library gets the same list
in the result of the call (`deleted_kept_refs`). To keep the commits, make
a branch before then: `git branch <name> <ref>`.

Only names that fast-worktree makes are removed from the cache directory: a
repository directory `<name>-<8 hex digits>`, in it a key `<16 hex
digits>-<...>` with its `.ok` marker, or such a key followed by
`.tmp-<pid>-<n>`; in a pool, a spare's names (`wt-<time>-<pid>-<n>`, and
that after `.new-`, `.no-hooks-` or `.claimed-<pid>-`), `fill.log` and
`misses`. Anything else there is left alone and is not listed by `cache
ls`.

## What is safe to clone

A cloned `node_modules` is the same tree an install would produce for that
lockfile and Node version. Dependencies' install scripts wrote their output
inside `node_modules`, so the clone carries it. After cloning:

- `node_modules/.cache` is deleted (tool caches often embed absolute
  paths).
- A tree with a symlink whose target is absolute, which could point into
  the worktree it came from, is not used: a clone from another worktree is
  checked and dropped, and an install result is checked before it is
  stored in the cache, and then not stored. The check walks every file, so
  for the cache it runs once per entry, when the entry is stored, not on
  each clone.
- The project's own `preinstall`, `install`, `postinstall` and `prepare`
  scripts, which may write outside `node_modules` (git hooks setup, code
  generation), are run again if its `package.json` has them, unless a
  `skip_lifecycle` rule applies. If one fails, the clone is removed, so the
  next `install` does not take a broken tree for an installed one.
- Those scripts often write the repository's shared `.git/config` (`git
  config core.hooksPath ...`), which git lets only one process write at a
  time, so they run one at a time ([install scripts](#install-scripts)).

## Install scripts

A project's own `preinstall`, `install`, `postinstall` and `prepare`, and
its dependencies' install scripts (a native addon's build, a tool that
installs git hooks), may write the repository's shared `.git/config`,
which git lets only one process write at a time. So every run of them, in
any fast-worktree command of the repository, holds a lock,
`<cache>/<repo>-<hash>/lifecycle.lock`, and they run one at a time. The
downloads do not:

- `npm ci` runs with `--ignore-scripts`, for every project at once. Then,
  holding the lock, `npm rebuild <names>` runs the install scripts of the
  dependencies that have one (`hasInstallScript` in
  `node_modules/.package-lock.json`), and `npm run <script>` the project's
  own scripts, in npm's order. The tree, `.package-lock.json` included, is
  the one `npm ci` makes. (A plain `npm rebuild` runs the project's own
  scripts in another order and leaves out `prepare`.)
- An incremental install runs `npm install --ignore-scripts`, then the
  same, for the dependencies that are new or changed only.
- Until the scripts are done, `node_modules/.fast-worktree-scripts-pending`
  is there, and the tree does not count as installed (an install that was
  stopped in between is done again).
- A lockfile with a git dependency or a link (workspaces), whose scripts
  npm runs in ways `npm rebuild` does not repeat, gets `npm ci` (or `npm
  install`) with its scripts, holding the lock. So does a project whose
  `npm rebuild` failed (an optional dependency whose build fails, which
  `npm ci` lets pass), with a note.
- With npm's `ignore-scripts` set (in an `.npmrc` or the environment), no
  script runs, as with `npm ci`.

The time a step waited for the lock is shown apart from the time it took:
`clone  . from … (0.3s, waited 4.1s for lifecycle lock)`. A run that still
fails with `could not lock config file` (something else had it) is tried
again, at most twice.

## Library usage

The crate is also a library, for programs that create and remove worktrees
themselves, such as a task runner that gives every attempt its own checkout.
It does what the command does, with the same pool, cache, hooks, trash and
kept refs in the same places, so an embedding program and the command can work
on the same repository and state directories at the same time.

```toml
[dependencies]
# `default-features = false` leaves the command-line parser (clap) out.
fast-worktree = { version = "0.6", default-features = false }
```

```rust
use fast_worktree::{Checkout, CreateOptions, Output, RemoveMode, RemoveOptions, Repository, Start};

let repo = Repository::open("/work/app")?.with_output(Output::Capture);

// A detached worktree at a commit, with its dependencies; a spare from the
// pool when one is ready.
let made = repo.create_worktree(&CreateOptions::new(
    "/work/attempts/42",
    Checkout::Detached(Start::Rev("3f2c1ab".into())),
))?;
assert!(made.setup_ok);

// Removed at once; the files are deleted in the background.
repo.remove_worktree(&made.path, &RemoveOptions::new(RemoveMode::Discard))?;

// What applies to the repository: `.fast-worktree.toml`, the pool size and
// `.worktreeinclude`.
let cfg = repo.config()?;
```

- `Repository::create_worktree` is `add`: a new branch, an existing branch or
  a detached `HEAD`, starting at a revision or at origin's default branch.
- `Repository::remove_worktree` is `remove` (`RemoveMode::Strict`, which
  refuses a dirty worktree) or `hook remove` (`RemoveMode::Discard`, which
  keeps what has changes in the trash for a day and its `HEAD` in
  `refs/fast-worktree/kept/`). `kept_refs`, `prune_kept_refs` and
  `sweep_trash` manage what removals leave. A program that creates and
  removes worktrees through these calls need call nothing else to keep the
  state bounded: they make the same sweeps as the commands (see [What is
  deleted by itself](#what-is-deleted-by-itself)); `sweep_trash` runs the
  trash and cache part alone.
- `Repository::fill_pool` fills the pool in the calling thread. After a
  worktree is created, the command refills the pool in the background with
  `fast-worktree pool fill`; the library does so with the `fast-worktree`
  executable it finds on `PATH` or in `~/.cargo/bin` (or `with_helper`),
  and does not refill without one.
- Errors are one `Error` enum (`Refused`, `BranchNotMerged`, `Config`, `Git`,
  `Io`, `Failed`). Progress lines go where `Output` says: the terminal (as the
  command), nowhere, or the `log` of the result.

A program can also embed the whole command, for example one binary that acts
as `fast-worktree` when it is started through a link of that name. It runs
the command line with `fast_worktree::cli::run_with_helper(args, helper)`
(the `cli` feature), which returns the exit code. The background work that
`add`, `hook create` and `install` start (`pool fill`, `install --changed
--deferred`) re-execs `helper`, which must behave as the command: pass the
link. With `None` it re-execs the running binary, as the command does; on
Linux that is the real binary behind the link, not the link.

```rust
use std::path::PathBuf;

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let link = PathBuf::from("/usr/local/bin/fast-worktree");
    // ... when started as `fast-worktree`:
    std::process::exit(fast_worktree::cli::run_with_helper(args, Some(link)));
}
```

The API is documented on [docs.rs](https://docs.rs/fast-worktree).

## Platforms

Cloning needs macOS `clonefile(2)`. On other platforms the clone and delta
stages are skipped and every npm project gets `npm ci`; the post-checkout
hook only lists. A `[[clone]]` without a preset gets its `install`, a
Bundler project `bundle install`, and
`remove` keeps nothing in the cache. `add`, `check`, `remove`,
`.worktreeinclude`, Dart
projects, the cache commands, the pool and the hooks work the same; there,
`hook create` leaves every npm install to the background.

## License

MIT
