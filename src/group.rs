//! Membership of the group (spec 24.2): the proof two members give each
//! other after `hello`, the fingerprint a key generation is known by, and
//! the files that hold the current key and, during a rotation, the next one.
//!
//! The proof is an HMAC-SHA256 under the group key. Its input starts with a
//! label that names the purpose and the direction, so a proof is never a
//! valid secret-bundle HMAC under the same key, and what one side sends can
//! never be played back as the other side's answer. Every field is length
//! prefixed: two different sets of fields never encode to the same bytes.
//!
//! Holding the key is what makes a machine a member. The proof does not
//! separate members from each other (spec 24.2, "honest members").

use crate::ids::{ClusterId, CoordinatorId, WorkerId};
use crate::secrets::key::{self, ClusterKey};
use sha2::{Digest as _, Sha256};
use std::path::{Path, PathBuf};

/// The hello feature of the mutual proof. Required by both sides.
pub const MEMBERSHIP_FEATURE: &str = "group_membership_v1";
/// The hello feature of the `identity_retired` notice. Required by both sides.
pub const IDENTITY_RETIRED_FEATURE: &str = "identity_retired_v1";
/// The admission window of a worker is one budget over every member: the
/// `capacity` reason of a refused submit, the window in `inspect`, and the
/// `foreign_demand` of a forecast (spec 24.4, 24.5).
pub const SHARED_ADMISSION_FEATURE: &str = "shared_admission_v1";

const BLOCK: usize = 64;
const NONCE_BYTES: usize = 32;
const FINGERPRINT_HEX: usize = 32;

/// HMAC-SHA256 (RFC 2104).
fn hmac(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(message);
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Whether two strings are equal, in time that depends on their lengths only.
fn same(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn message(label: &str, fields: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in std::iter::once(&label).chain(fields) {
        out.extend_from_slice(&(part.len() as u64).to_be_bytes());
        out.extend_from_slice(part.as_bytes());
    }
    out
}

/// The short digest a key generation is known by. It says which key a
/// member holds and cannot be turned back into the key.
pub fn fingerprint(key: &ClusterKey) -> String {
    let mac = hmac(
        key.as_bytes(),
        &message("belt group key fingerprint v1", &[]),
    );
    hex(&mac)[..FINGERPRINT_HEX].to_string()
}

/// A value used once: 32 bytes from the operating system, as hex.
pub fn nonce() -> std::io::Result<String> {
    let mut bytes = [0u8; NONCE_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(|e| std::io::Error::other(format!("no random bytes: {e}")))?;
    Ok(hex(&bytes))
}

/// Whether `text` has the form of [`nonce`].
pub fn is_nonce(text: &str) -> bool {
    text.len() == NONCE_BYTES * 2 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

/// What the connecting member proves: that it holds the key, for this
/// group, as this coordinator under this name, answering this nonce of the
/// receiving side.
pub fn connector_proof(
    key: &ClusterKey,
    cluster_id: &ClusterId,
    coordinator_id: &CoordinatorId,
    member: &WorkerId,
    receiver_nonce: &str,
) -> String {
    hex(&hmac(
        key.as_bytes(),
        &message(
            "belt group_proof v1 connector",
            &[
                cluster_id.as_str(),
                coordinator_id.as_str(),
                member.as_str(),
                receiver_nonce,
            ],
        ),
    ))
}

/// What the receiving member answers: that it holds the key, under this
/// name, answering this nonce of the connecting side.
pub fn receiver_proof(key: &ClusterKey, member: &WorkerId, connector_nonce: &str) -> String {
    hex(&hmac(
        key.as_bytes(),
        &message(
            "belt group_proof v1 receiver",
            &[member.as_str(), connector_nonce],
        ),
    ))
}

/// Whether `got` is the proof `expected`. Compared without an early exit.
pub fn proof_matches(expected: &str, got: &str) -> bool {
    same(expected, got)
}

/// The keys a member holds: the one it signs with and, between the two
/// stages of a rotation, the next one. Both are accepted from a peer.
#[derive(Debug, Clone)]
pub struct Keys {
    pub current: ClusterKey,
    pub next: Option<ClusterKey>,
}

impl Keys {
    /// Only the current key (no rotation is under way).
    pub fn only(current: ClusterKey) -> Self {
        Keys {
            current,
            next: None,
        }
    }

    /// The key whose fingerprint is `fingerprint`, if this member holds it.
    pub fn by_fingerprint(&self, wanted: &str) -> Option<&ClusterKey> {
        std::iter::once(&self.current)
            .chain(self.next.as_ref())
            .find(|k| same(&fingerprint(k), wanted))
    }
}

/// Where the next key of a rotation is kept: beside the current one.
pub fn next_key_path(key_path: &Path) -> PathBuf {
    let mut name = key_path.as_os_str().to_owned();
    name.push(".next");
    PathBuf::from(name)
}

/// Read the keys at `key_path` (the single key file of before the group is
/// the current key, generation 1) and beside it.
pub fn load_keys(key_path: &Path) -> crate::secrets::Result<Keys> {
    let current = key::load(key_path)?;
    let next_path = next_key_path(key_path);
    let next = match std::fs::symlink_metadata(&next_path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        _ => Some(key::load(&next_path)?),
    };
    Ok(Keys { current, next })
}

/// Make the next key the current one and drop the old one: the second stage
/// of a rotation. One rename, so a crash leaves either the old state or the
/// new one. Returns whether there was a next key to switch to; without one
/// nothing changes (the switch was already made).
pub fn switch_to_next(key_path: &Path) -> std::io::Result<bool> {
    let staged = next_key_path(key_path);
    if !staged.try_exists()? {
        return Ok(false);
    }
    crate::service::init::install_key(&staged, key_path).map_err(std::io::Error::other)?;
    Ok(true)
}

/// Whether the key with `fingerprint` is one this member still holds (its
/// current key or, during a rotation, its next one). A connection that was
/// proven with any other key is closed and proven again (spec 24.2), on the
/// side that accepted it and on the side that made it. A key file that
/// cannot be read holds nothing.
pub fn still_holds(key_path: &Path, fingerprint: &str) -> bool {
    load_keys(key_path).is_ok_and(|keys| keys.by_fingerprint(fingerprint).is_some())
}

/// A member that holds a next key was refused with `group_key_mismatch` by a
/// peer that showed `peer_fingerprint`. When that is the fingerprint of the
/// next key, the peer has already switched and the instruction to switch did
/// not reach this member: switch now (spec 24.2). Returns whether it did.
/// The fingerprint is the peer's word before any proof; acting on it is
/// accepted under the honest-member premise, and it can only ever move this
/// member to a key it was already given.
pub fn switch_if_peer_is_on_next(key_path: &Path, peer_fingerprint: &str) -> std::io::Result<bool> {
    let keys = load_keys(key_path).map_err(|e| std::io::Error::other(e.to_string()))?;
    match &keys.next {
        Some(next) if same(&fingerprint(next), peer_fingerprint) => switch_to_next(key_path),
        _ => Ok(false),
    }
}

/// A fixed key for tests of this crate.
#[cfg(test)]
pub(crate) fn test_key() -> ClusterKey {
    ClusterKey::from_bytes(vec![7; 32]).expect("long enough")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> ClusterKey {
        ClusterKey::from_bytes(vec![byte; 32]).unwrap()
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn hmac_matches_the_rfc_4231_vectors() {
        // Test case 1.
        assert_eq!(
            hex(&hmac(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 2.
        assert_eq!(
            hex(&hmac(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 6: a key longer than the block is hashed first.
        assert_eq!(
            hex(&hmac(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        // Test case 4: a 25 byte key.
        assert_eq!(
            hex(&hmac(
                &unhex("0102030405060708090a0b0c0d0e0f10111213141516171819"),
                &[0xcd; 50]
            )),
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
        );
    }

    #[test]
    fn a_proof_holds_for_exactly_its_key_group_member_and_nonce() {
        let (cluster, coordinator) = (ClusterId::generate(), CoordinatorId::generate());
        let member = WorkerId::parse("main").unwrap();
        let n = nonce().unwrap();
        let good = connector_proof(&key(1), &cluster, &coordinator, &member, &n);
        assert_eq!(good.len(), 64);
        assert!(proof_matches(
            &connector_proof(&key(1), &cluster, &coordinator, &member, &n),
            &good
        ));
        let other = WorkerId::parse("sub").unwrap();
        for different in [
            connector_proof(&key(2), &cluster, &coordinator, &member, &n),
            connector_proof(&key(1), &ClusterId::generate(), &coordinator, &member, &n),
            connector_proof(&key(1), &cluster, &CoordinatorId::generate(), &member, &n),
            connector_proof(&key(1), &cluster, &coordinator, &other, &n),
            connector_proof(&key(1), &cluster, &coordinator, &member, &nonce().unwrap()),
        ] {
            assert!(!proof_matches(&good, &different));
        }
        assert!(!proof_matches(&good, &good[..63]));
        assert!(!proof_matches(&good, ""));
        // The receiver's answer is another message: a connector's proof sent
        // back is not a receiver's proof, whatever the fields.
        let back = receiver_proof(&key(1), &member, &n);
        assert!(proof_matches(&receiver_proof(&key(1), &member, &n), &back));
        assert!(!proof_matches(&back, &receiver_proof(&key(2), &member, &n)));
        assert!(!proof_matches(&back, &receiver_proof(&key(1), &other, &n)));
        assert!(!proof_matches(
            &back,
            &receiver_proof(&key(1), &member, &nonce().unwrap())
        ));
        assert_ne!(back, good);
    }

    #[test]
    fn fields_cannot_be_shifted_into_each_other() {
        assert_ne!(message("l", &["ab", "c"]), message("l", &["a", "bc"]));
        assert_ne!(message("l", &["ab"]), message("la", &["b"]));
        assert_ne!(message("l", &["a", ""]), message("l", &["a"]));
    }

    #[test]
    fn nonces_are_fresh_and_a_fingerprint_names_a_key_without_showing_it() {
        let (a, b) = (nonce().unwrap(), nonce().unwrap());
        assert_ne!(a, b);
        assert!(is_nonce(&a) && is_nonce(&b));
        assert!(!is_nonce("") && !is_nonce(&a[1..]) && !is_nonce(&format!("{}zz", &a[2..])));
        let f = fingerprint(&key(1));
        assert_eq!(f.len(), 32);
        assert_eq!(f, fingerprint(&key(1)));
        assert_ne!(f, fingerprint(&key(2)));
        assert!(!hex(key(1).as_bytes()).contains(&f));
        let keys = Keys {
            current: key(1),
            next: Some(key(2)),
        };
        assert_eq!(keys.by_fingerprint(&f), Some(&key(1)));
        assert_eq!(keys.by_fingerprint(&fingerprint(&key(2))), Some(&key(2)));
        assert_eq!(keys.by_fingerprint(&fingerprint(&key(3))), None);
        assert_eq!(
            Keys::only(key(1)).by_fingerprint(&fingerprint(&key(2))),
            None
        );
    }

    #[test]
    fn the_next_key_becomes_the_current_one_only_when_the_peer_shows_it() {
        let dir = crate::test_tmp::tempdir().unwrap();
        let path = dir.path().join("keys").join("cluster.key");
        let first = key::init(&path).unwrap();
        // The single key of before the group is the current one.
        let keys = load_keys(&path).unwrap();
        assert_eq!(keys.current, first);
        assert!(keys.next.is_none());
        assert!(!switch_to_next(&path).unwrap(), "nothing to switch to");
        assert!(still_holds(&path, &fingerprint(&first)));
        assert!(!still_holds(&path, &fingerprint(&key(9))));
        assert!(!still_holds(
            &dir.path().join("absent"),
            &fingerprint(&first)
        ));
        assert!(!switch_if_peer_is_on_next(&path, &fingerprint(&first)).unwrap());
        // The first stage of a rotation hands over the next key.
        let second = key::init(&next_key_path(&path)).unwrap();
        assert_ne!(first, second);
        let keys = load_keys(&path).unwrap();
        assert_eq!((&keys.current, keys.next.as_ref()), (&first, Some(&second)));
        // A peer on some other key, or still on the current one, moves nothing.
        assert!(!switch_if_peer_is_on_next(&path, &fingerprint(&key(9))).unwrap());
        assert!(!switch_if_peer_is_on_next(&path, &fingerprint(&first)).unwrap());
        assert_eq!(load_keys(&path).unwrap().current, first);
        // A peer that shows the next key has switched: follow it.
        assert!(switch_if_peer_is_on_next(&path, &fingerprint(&second)).unwrap());
        let keys = load_keys(&path).unwrap();
        assert_eq!(keys.current, second);
        assert!(
            keys.next.is_none(),
            "the old key is gone and no next is left"
        );
        assert!(!switch_if_peer_is_on_next(&path, &fingerprint(&second)).unwrap());
    }
}
