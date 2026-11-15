"""The mutation and coverage target policy; no mutant exclusions."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def modules():
    paths = (ROOT / "scripts/pure-modules.txt").read_text().splitlines()
    if not paths or len(paths) != len(set(paths)):
        raise ValueError("pure modules must be nonempty and unique")
    for path in paths:
        if not path.startswith("src/") or not path.endswith(".rs") or not (ROOT / path).is_file():
            raise ValueError(f"invalid or renamed pure module: {path}")
    return paths


def selectors():
    # These parsers have shared causal tests rather than per-file test modules.
    shared = {"config/legacy": "config::", "config/parse": "config::", "config/paths": "config::",
              "toolchain/versions": "toolchain::tests::", "toolchain/declarations": "toolchain::tests::"}
    return list(dict.fromkeys(shared.get(path[4:-3], path[4:-3].replace("/", "::") + "::") for path in modules()))
