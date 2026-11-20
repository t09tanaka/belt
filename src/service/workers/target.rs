//! Pure decisions about which binary the parent may send to a worker host.
//!
//! The parent sends its own binary only to a host of its own operating
//! system and architecture. Any other host needs a build made for it, named
//! with `--binary`; nothing is downloaded. Whatever is sent must report the
//! parent's version before anything on the host is switched to it.

/// An operating system a worker can run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Os {
    Macos,
    Linux,
}

/// A processor architecture a worker can run on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    X86_64,
    Aarch64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Platform {
    pub os: Os,
    pub arch: Arch,
}

fn arch_of(name: &str) -> Option<Arch> {
    match name {
        "x86_64" | "amd64" => Some(Arch::X86_64),
        // macOS says `arm64`, Linux and Rust say `aarch64`.
        "arm64" | "aarch64" => Some(Arch::Aarch64),
        _ => None,
    }
}

impl Platform {
    /// From `uname -s` and `uname -m`, as a worker host prints them.
    pub fn from_uname(system: &str, machine: &str) -> Option<Platform> {
        let os = match system {
            "Darwin" => Os::Macos,
            "Linux" => Os::Linux,
            _ => return None,
        };
        Some(Platform {
            os,
            arch: arch_of(machine)?,
        })
    }

    /// From `std::env::consts::OS` and `ARCH`, as the parent knows itself.
    pub fn from_rust(os: &str, arch: &str) -> Option<Platform> {
        let os = match os {
            "macos" => Os::Macos,
            "linux" => Os::Linux,
            _ => return None,
        };
        Some(Platform {
            os,
            arch: arch_of(arch)?,
        })
    }

    /// The Rust target triple of a build that runs on this platform.
    pub fn triple(self) -> &'static str {
        match (self.os, self.arch) {
            (Os::Macos, Arch::Aarch64) => "aarch64-apple-darwin",
            (Os::Macos, Arch::X86_64) => "x86_64-apple-darwin",
            (Os::Linux, Arch::Aarch64) => "aarch64-unknown-linux-gnu",
            (Os::Linux, Arch::X86_64) => "x86_64-unknown-linux-gnu",
        }
    }
}

/// What a worker host answered to the staging probe: `uname -s`, `uname -m`
/// and whether the binary is already staged, one per line. A missing line
/// is empty, and only the exact word counts as staged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe<'a> {
    pub system: &'a str,
    pub machine: &'a str,
    pub staged: bool,
}

pub fn parse_probe(text: &str) -> Probe<'_> {
    let mut lines = text.lines();
    let mut next = || lines.next().unwrap_or("").trim();
    Probe {
        system: next(),
        machine: next(),
        staged: next() == "staged",
    }
}

/// Whether the parent may send a binary to the worker, and to which
/// platform. `parent` is `None` when the parent itself runs on something no
/// worker runs on. `explicit` says a build was named with `--binary`. The
/// error is the message for the user: it names the triple that is needed
/// and the flag.
pub fn decide(
    parent: Option<Platform>,
    system: &str,
    machine: &str,
    explicit: bool,
) -> Result<Platform, String> {
    let Some(worker) = Platform::from_uname(system, machine) else {
        return Err(format!(
            "the worker is {system}/{machine}, which is not a platform a worker runs on (macOS or Linux, x86_64 or arm64); nothing was sent"
        ));
    };
    if explicit || parent == Some(worker) {
        return Ok(worker);
    }
    Err(format!(
        "the worker is {system}/{machine} and this parent's own binary does not run there; build belt of this version for {} and name it with --binary <path>; nothing was sent",
        worker.triple()
    ))
}

/// Whether what the staged binary printed for `--version` is exactly what
/// this parent prints. Surrounding whitespace is not part of a version; an
/// empty answer never matches.
pub fn version_matches(expected: &str, reported: &[u8]) -> bool {
    let expected = expected.trim();
    !expected.is_empty()
        && std::str::from_utf8(reported).is_ok_and(|reported| reported.trim() == expected)
}

/// A reported version for a message: one line, no control characters, and
/// short enough that a wrong file cannot flood the terminal.
pub fn shown_version(reported: &[u8]) -> String {
    let text = String::from_utf8_lossy(reported);
    let line: String = text
        .lines()
        .next()
        .unwrap_or("")
        .chars()
        .filter(|c| !c.is_control())
        .take(80)
        .collect();
    if line.trim().is_empty() {
        "nothing".into()
    } else {
        line.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAC_ARM: Platform = Platform {
        os: Os::Macos,
        arch: Arch::Aarch64,
    };
    const LINUX_X86: Platform = Platform {
        os: Os::Linux,
        arch: Arch::X86_64,
    };

    #[test]
    fn uname_and_rust_names_meet_in_one_platform() {
        assert_eq!(Platform::from_uname("Darwin", "arm64"), Some(MAC_ARM));
        assert_eq!(Platform::from_rust("macos", "aarch64"), Some(MAC_ARM));
        assert_eq!(Platform::from_uname("Linux", "x86_64"), Some(LINUX_X86));
        assert_eq!(Platform::from_rust("linux", "x86_64"), Some(LINUX_X86));
        assert_eq!(
            Platform::from_uname("Linux", "aarch64"),
            Platform::from_rust("linux", "aarch64")
        );
        assert_eq!(
            Platform::from_uname("Linux", "amd64").map(|p| p.arch),
            Some(Arch::X86_64)
        );
        assert_eq!(
            Platform::from_uname("Darwin", "x86_64").map(|p| p.os),
            Some(Os::Macos)
        );
        // The names of one side are not the names of the other.
        assert_eq!(Platform::from_uname("linux", "x86_64"), None);
        assert_eq!(Platform::from_uname("macos", "arm64"), None);
        assert_eq!(Platform::from_rust("Darwin", "aarch64"), None);
        assert_eq!(Platform::from_rust("Linux", "x86_64"), None);
        assert_eq!(Platform::from_uname("Linux", "riscv64"), None);
        assert_eq!(Platform::from_rust("linux", "riscv64"), None);
        assert_eq!(Platform::from_uname("FreeBSD", "x86_64"), None);
        assert_eq!(Platform::from_rust("windows", "x86_64"), None);
    }

    #[test]
    fn every_platform_has_its_own_triple() {
        let triple = |system, machine| Platform::from_uname(system, machine).unwrap().triple();
        assert_eq!(triple("Darwin", "arm64"), "aarch64-apple-darwin");
        assert_eq!(triple("Darwin", "x86_64"), "x86_64-apple-darwin");
        assert_eq!(triple("Linux", "aarch64"), "aarch64-unknown-linux-gnu");
        assert_eq!(triple("Linux", "x86_64"), "x86_64-unknown-linux-gnu");
    }

    #[test]
    fn a_probe_is_read_line_by_line_and_only_the_exact_word_is_staged() {
        assert_eq!(
            parse_probe("Linux\naarch64\nstaged\n"),
            Probe {
                system: "Linux",
                machine: "aarch64",
                staged: true
            }
        );
        assert!(!parse_probe("Linux\naarch64\nabsent\n").staged);
        assert!(!parse_probe("Linux\naarch64\n").staged);
        assert!(!parse_probe("Linux\naarch64\nstaged-not\n").staged);
        assert!(parse_probe("Darwin\r\narm64\r\n staged \r\n").staged);
        assert_eq!(
            parse_probe(""),
            Probe {
                system: "",
                machine: "",
                staged: false
            }
        );
        assert_eq!(parse_probe(" Darwin \n arm64 ").machine, "arm64");
        assert_eq!(parse_probe(" Darwin \n arm64 ").system, "Darwin");
    }

    #[test]
    fn the_own_binary_goes_only_to_the_parents_own_platform() {
        assert_eq!(decide(Some(MAC_ARM), "Darwin", "arm64", false), Ok(MAC_ARM));
        assert_eq!(
            decide(Some(LINUX_X86), "Linux", "x86_64", false),
            Ok(LINUX_X86)
        );
        // Another system, and the same system on another architecture.
        let refused = decide(Some(MAC_ARM), "Linux", "x86_64", false).unwrap_err();
        assert!(refused.contains("Linux/x86_64"), "{refused}");
        assert!(refused.contains("x86_64-unknown-linux-gnu"), "{refused}");
        assert!(refused.contains("--binary <path>"), "{refused}");
        let refused = decide(Some(MAC_ARM), "Darwin", "x86_64", false).unwrap_err();
        assert!(refused.contains("x86_64-apple-darwin"), "{refused}");
        let refused = decide(Some(LINUX_X86), "Linux", "aarch64", false).unwrap_err();
        assert!(refused.contains("aarch64-unknown-linux-gnu"), "{refused}");
        // A parent that is not a worker platform has no binary of its own to send.
        let refused = decide(None, "Linux", "x86_64", false).unwrap_err();
        assert!(refused.contains("--binary <path>"), "{refused}");
    }

    #[test]
    fn a_named_build_goes_to_any_worker_platform_and_to_nothing_else() {
        assert_eq!(
            decide(Some(MAC_ARM), "Linux", "x86_64", true),
            Ok(LINUX_X86)
        );
        assert_eq!(decide(None, "Darwin", "arm64", true), Ok(MAC_ARM));
        assert_eq!(decide(Some(MAC_ARM), "Darwin", "arm64", true), Ok(MAC_ARM));
        for explicit in [false, true] {
            let refused = decide(Some(MAC_ARM), "FreeBSD", "amd64", explicit).unwrap_err();
            assert!(refused.contains("FreeBSD/amd64"), "{refused}");
            assert!(refused.contains("not a platform"), "{refused}");
            assert!(!refused.contains("--binary"), "{refused}");
        }
    }

    #[test]
    fn a_version_matches_only_when_it_is_the_same_text() {
        let own = "belt 1.2.3\n";
        assert!(version_matches(own, b"belt 1.2.3\n"));
        assert!(version_matches(own, b"  belt 1.2.3\r\n"));
        assert!(!version_matches(own, b"belt 1.2.4\n"));
        assert!(!version_matches(own, b"belt 1.2.3 extra"));
        assert!(!version_matches(own, b""));
        assert!(!version_matches(own, &[0xff, 0xfe]));
        // Nothing matches a parent that knows no version of its own.
        assert!(!version_matches("", b""));
        assert!(!version_matches(" \n", b" \n"));
    }

    #[test]
    fn a_reported_version_is_shown_as_one_bounded_plain_line() {
        assert_eq!(shown_version(b"belt 0.1.0\nmore\n"), "belt 0.1.0");
        assert_eq!(shown_version(b""), "nothing");
        assert_eq!(shown_version(b"  \n"), "nothing");
        assert_eq!(shown_version(b" a\x1b[31mb \n"), "a[31mb");
        assert_eq!(shown_version("x".repeat(200).as_bytes()).len(), 80);
        assert_eq!(shown_version(&[b'v', 0xff]), "v\u{fffd}");
    }
}
