//! Pure parts of the systemd linger check (spec chapter 15). A systemd user
//! service runs only while its user has a session, unless lingering is
//! enabled for the user: without it a Linux worker stops at the last logout
//! and does not come back at boot. The host asks `loginctl`; the parent
//! turns the answer into a check. Nothing here runs a command.

use super::{Check, Level};
use serde::{Deserialize, Serialize};

/// What a host found out about lingering. Only a host whose service is a
/// systemd user service reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Linger {
    /// The numeric id of the user the service runs as.
    pub uid: u32,
    /// `None`: `loginctl` could not be run or did not say.
    pub enabled: Option<bool>,
}

/// The arguments of the one question asked of `loginctl`.
pub fn arguments(uid: u32) -> [String; 3] {
    [
        "show-user".into(),
        uid.to_string(),
        "--property=Linger".into(),
    ]
}

/// Read the answer of `loginctl show-user <uid> --property=Linger`: exit 0
/// and exactly one `Linger=yes` or `Linger=no` line. Anything else is no
/// answer, never "enabled".
pub fn parse(code: i32, stdout: &str) -> Option<bool> {
    if code != 0 {
        return None;
    }
    let mut lines = stdout.lines().map(str::trim).filter(|l| !l.is_empty());
    let answer = match lines.next()? {
        "Linger=yes" => true,
        "Linger=no" => false,
        _ => return None,
    };
    lines.next().is_none().then_some(answer)
}

/// The command that turns lingering on, exactly as it is typed on the host.
pub fn fix_command(uid: u32) -> String {
    format!("loginctl enable-linger {uid}")
}

/// The check for one host. Lingering that is off is a warning (`notice`):
/// the worker works while its user is logged in, and the message carries
/// the fix. No answer is `not_checked`, never a pass.
pub fn check(host: &str, linger: &Linger) -> Check {
    let uid = linger.uid;
    match linger.enabled {
        Some(true) => Check::new(
            host,
            "linger",
            Level::Ok,
            format!("lingering is enabled for uid {uid}: the systemd user service keeps running without a login session"),
        ),
        Some(false) => Check::new(
            host,
            "linger",
            Level::Notice,
            format!(
                "lingering is off for uid {uid}: the systemd user service stops when the user's last session ends and does not start at boot; on the host, run: {}",
                fix_command(uid)
            ),
        ),
        None => Check::new(
            host,
            "linger",
            Level::NotChecked,
            format!(
                "loginctl did not say whether lingering is enabled for uid {uid}; on the host, look at: loginctl {}",
                arguments(uid).join(" ")
            ),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_question_names_the_user_and_the_one_property() {
        assert_eq!(arguments(1000), ["show-user", "1000", "--property=Linger"]);
        assert_eq!(fix_command(1000), "loginctl enable-linger 1000");
        assert_eq!(fix_command(0), "loginctl enable-linger 0");
    }

    #[test]
    fn only_an_exact_answer_of_a_successful_loginctl_counts() {
        assert_eq!(parse(0, "Linger=yes\n"), Some(true));
        assert_eq!(parse(0, "Linger=no\n"), Some(false));
        assert_eq!(parse(0, "  Linger=yes  \n\n"), Some(true));
        assert_eq!(parse(0, "Linger=no"), Some(false));
        // A failed command says nothing, whatever it printed.
        assert_eq!(parse(1, "Linger=yes\n"), None);
        assert_eq!(parse(-1, "Linger=no\n"), None);
        assert_eq!(parse(0, ""), None);
        assert_eq!(parse(0, "\n\n"), None);
        assert_eq!(parse(0, "Linger=maybe\n"), None);
        assert_eq!(parse(0, "linger=yes\n"), None);
        assert_eq!(parse(0, "Linger=yes please\n"), None);
        assert_eq!(parse(0, "State=active\n"), None);
        // More than the one property that was asked for is not this answer.
        assert_eq!(parse(0, "Linger=yes\nLinger=no\n"), None);
        assert_eq!(parse(0, "Linger=no\nState=active\n"), None);
    }

    #[test]
    fn lingering_off_is_a_warning_that_carries_the_fix() {
        let on = check(
            "mini-l",
            &Linger {
                uid: 1000,
                enabled: Some(true),
            },
        );
        assert_eq!((on.host.as_str(), on.id.as_str()), ("mini-l", "linger"));
        assert_eq!(on.level, Level::Ok);
        assert!(on.detail.contains("uid 1000"), "{}", on.detail);
        let off = check(
            "mini-l",
            &Linger {
                uid: 1001,
                enabled: Some(false),
            },
        );
        assert_eq!((off.host.as_str(), off.id.as_str()), ("mini-l", "linger"));
        assert_eq!(off.level, Level::Notice);
        assert!(
            off.detail.ends_with("run: loginctl enable-linger 1001"),
            "{}",
            off.detail
        );
        let silent = check(
            "parent",
            &Linger {
                uid: 7,
                enabled: None,
            },
        );
        assert_eq!(
            (silent.host.as_str(), silent.id.as_str()),
            ("parent", "linger")
        );
        assert_eq!(silent.level, Level::NotChecked);
        assert!(
            silent
                .detail
                .ends_with("loginctl show-user 7 --property=Linger"),
            "{}",
            silent.detail
        );
    }
}
