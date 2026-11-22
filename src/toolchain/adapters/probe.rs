//! Complete, sampled inspection of small private installations. No native commands,
//! creation, pins or admission grants. Unsupported links conservatively stay unknown.
use super::*;
use rustix::fs::{open, openat, Dir, Mode, OFlags};
use std::cell::Cell;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

const DIR: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK);
const REG: OFlags = OFlags::RDONLY
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC)
    .union(OFlags::NONBLOCK);
const MAX_OPEN: usize = 256;
const MAX_VISITS: usize = 1024;
const RECEIPT_BYTES: u64 = 1024 * 1024;
#[derive(PartialEq, Eq)]
struct Stamp {
    dev: u64,
    ino: u64,
    size: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    links: u64,
    mt: i64,
    mtn: i64,
    ct: i64,
    ctn: i64,
}
impl Stamp {
    fn of(m: &fs::Metadata) -> Self {
        Self {
            dev: m.dev(),
            ino: m.ino(),
            size: m.len(),
            mode: m.mode(),
            uid: m.uid(),
            gid: m.gid(),
            links: m.nlink(),
            mt: m.mtime(),
            mtn: m.mtime_nsec(),
            ct: m.ctime(),
            ctn: m.ctime_nsec(),
        }
    }
    fn anchor(m: &fs::Metadata) -> Self {
        let mut s = Self::of(m);
        // Ancestor directory membership is unrelated to a file identity. Full
        // inventory stamps are tracked separately for the private tool tree.
        if m.is_dir() {
            s.size = 0;
            s.links = 0;
            s.mt = 0;
            s.mtn = 0;
            s.ct = 0;
            s.ctn = 0;
        }
        s
    }
}
struct Held {
    fd: File,
    stamp: Stamp,
}
#[derive(Default)]
pub(super) struct Snapshot {
    paths: BTreeMap<PathBuf, Held>,
    inventories: BTreeMap<PathBuf, Stamp>,
    visits: Cell<usize>,
}
impl Snapshot {
    #[cfg(test)]
    pub(super) fn paths_len(&self) -> usize {
        self.paths.len()
    }
    #[cfg(test)]
    pub(super) fn visits(&self) -> usize {
        self.visits.get()
    }
    fn visit(&self) -> Result<()> {
        let n = self.visits.get();
        if n >= MAX_VISITS {
            return Err("probe_entry_budget");
        }
        self.visits.set(n + 1);
        Ok(())
    }
    fn record(&mut self, path: &Path, fd: File) -> Result<File> {
        let stamp = Stamp::anchor(&fd.metadata().map_err(|_| "probe_metadata")?);
        if let Some(old) = self.paths.get(path) {
            if old.stamp != stamp {
                return Err("probe_path_changed");
            }
        } else {
            if self.paths.len() >= MAX_OPEN {
                return Err("probe_fd_budget");
            }
            self.paths.insert(
                path.to_owned(),
                Held {
                    fd: fd.try_clone().map_err(|_| "probe_fd")?,
                    stamp,
                },
            );
        }
        Ok(fd)
    }
    fn open(&mut self, path: &Path, directory: bool) -> Result<File> {
        // Lexical validation only; every actual component open is relative to a
        // retained parent descriptor, with NOFOLLOW (including all ancestors).
        if !path.is_absolute() || path.as_os_str().len() > 4096 || path.components().count() > 32 {
            return Err("probe_path_budget");
        }
        let mut parent: File = open("/", DIR, Mode::empty())
            .map_err(|_| "probe_root")?
            .into();
        parent = self.record(Path::new("/"), parent)?;
        let mut at = PathBuf::from("/");
        let parts = path.components().skip(1).collect::<Vec<_>>();
        for (i, part) in parts.iter().enumerate() {
            let Component::Normal(name) = part else {
                return Err("probe_path_component");
            };
            at.push(name);
            let flags = if i + 1 < parts.len() || directory {
                DIR
            } else {
                REG
            };
            let file: File = openat(&parent, *name, flags, Mode::empty())
                .map_err(|_| "probe_open")?
                .into();
            parent = self.record(&at, file)?;
        }
        Ok(parent)
    }
    pub(super) fn finish(&self) -> Result<()> {
        for (path, held) in &self.paths {
            if Stamp::anchor(&held.fd.metadata().map_err(|_| "probe_metadata")?) != held.stamp {
                return Err("probe_changed");
            }
            if path == Path::new("/") {
                continue;
            }
            let parent = self
                .paths
                .get(path.parent().ok_or("probe_parent")?)
                .ok_or("probe_parent")?;
            let opened: File = openat(
                &parent.fd,
                path.file_name().ok_or("probe_name")?,
                REG,
                Mode::empty(),
            )
            .map_err(|_| "probe_replaced")?
            .into();
            if Stamp::anchor(&opened.metadata().map_err(|_| "probe_metadata")?) != held.stamp {
                return Err("probe_replaced");
            }
        }
        for (path, stamp) in &self.inventories {
            let held = self.paths.get(path).ok_or("probe_directory")?;
            if Stamp::of(&held.fd.metadata().map_err(|_| "probe_metadata")?) != *stamp {
                return Err("probe_inventory_changed");
            }
        }
        Ok(())
    }
}
pub(super) fn private_root(path: &Path, budget: &IdentityReadBudget) -> Result<()> {
    let file = budget.snapshot.borrow_mut().open(path, true)?;
    let m = file.metadata().map_err(|_| "probe_metadata")?;
    if m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Err("unsafe_private_directory");
    }
    Ok(())
}
fn elapsed(budget: &IdentityReadBudget) -> Result<()> {
    match crate::platform::monotonic_ms().checked_sub(budget.started) {
        Some(age) if age < 5000 => Ok(()),
        _ => Err("probe_elapsed_budget"),
    }
}
fn read(
    file: &mut File,
    budget: &IdentityReadBudget,
    max: u64,
    mut consume: impl FnMut(&[u8]),
) -> Result<u64> {
    let mut bytes = 0u64;
    let mut buf = [0u8; 65536];
    loop {
        elapsed(budget)?;
        let left = budget.remaining.get();
        if left == 0 {
            return Err("identity_byte_budget");
        }
        let want = buf
            .len()
            .min(usize::try_from((max.saturating_sub(bytes) + 1).min(left)).unwrap_or(usize::MAX));
        budget.remaining.set(left - want as u64);
        budget.reads.set(budget.reads.get().saturating_add(1));
        // Failed reads retain their attempted byte charge; successful reads refund
        // unread capacity. Growth sentinels are charged even when rejected.
        let n = file.read(&mut buf[..want]).map_err(|_| "identity_read")?;
        budget
            .remaining
            .set(budget.remaining.get() + want as u64 - n as u64);
        if n == 0 {
            break;
        }
        bytes += n as u64;
        if bytes > max {
            return Err("identity_too_large");
        }
        consume(&buf[..n]);
    }
    Ok(bytes)
}
pub(super) fn capture(path: &Path, budget: &IdentityReadBudget) -> Result<FileIdentity> {
    elapsed(budget)?;
    let mut file = budget.snapshot.borrow_mut().open(path, false)?;
    let m = file.metadata().map_err(|_| "probe_metadata")?;
    let limit = budget
        .per_file
        .min(budget.remaining.get().saturating_sub(1));
    if !m.is_file() || m.len() > limit {
        return Err("identity_not_bounded_regular");
    }
    let before = Stamp::of(&m);
    let mut hash = Sha256::new();
    let size = read(&mut file, budget, limit, |b| hash.update(b))?;
    if size != m.len() || Stamp::of(&file.metadata().map_err(|_| "probe_metadata")?) != before {
        return Err("identity_changed");
    }
    Ok(FileIdentity {
        path: path.into(),
        digest: Digest::parse(&format!("sha256:{:x}", hash.finalize()))
            .map_err(|_| "identity_digest")?,
        size,
        executable: m.mode() & 0o111 != 0,
    })
}
// Bounded sequences prevent a compact hostile receipt from allocating an
// unbounded tree inventory before the byte/entry validator can inspect it.
fn bounded<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Vec<T>, D::Error> {
    struct V<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for V<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("at most 1024 entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Vec<T>, A::Error> {
            let mut out = vec![];
            while let Some(item) = seq.next_element()? {
                if out.len() == MAX_VISITS {
                    return Err(serde::de::Error::custom("probe receipt entry budget"));
                }
                out.push(item);
            }
            Ok(out)
        }
    }
    d.deserialize_seq(V(std::marker::PhantomData))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SmallTree {
    #[serde(deserialize_with = "bounded")]
    files: Vec<FileIdentity>,
    #[serde(deserialize_with = "bounded")]
    links: Vec<LinkIdentity>,
    #[serde(deserialize_with = "bounded")]
    directories: Vec<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SmallReceipt {
    schema: u32,
    plan: Digest,
    version: Version,
    root: PathBuf,
    tree: SmallTree,
    #[serde(deserialize_with = "bounded")]
    external: Vec<FileIdentity>,
    probe_outputs: BTreeMap<String, Digest>,
}
fn receipt(plan: &InstallPlan, budget: &IdentityReadBudget) -> Result<InstallationReceipt> {
    let mut file = budget
        .snapshot
        .borrow_mut()
        .open(&plan.root.join("receipt.json"), false)?;
    let m = file.metadata().map_err(|_| "receipt_metadata")?;
    if !m.is_file()
        || m.nlink() != 1
        || m.uid() != rustix::process::geteuid().as_raw()
        || m.mode() & 0o077 != 0
        || m.len() > RECEIPT_BYTES
    {
        return Err("unsafe_receipt");
    }
    let mut bytes = vec![];
    let size = read(&mut file, budget, RECEIPT_BYTES, |b| {
        bytes.extend_from_slice(b)
    })?;
    if size != m.len()
        || Stamp::of(&m) != Stamp::of(&file.metadata().map_err(|_| "receipt_metadata")?)
    {
        return Err("receipt_changed");
    }
    let r: SmallReceipt = serde_json::from_slice(&bytes).map_err(|_| "receipt_invalid")?;
    if r.external.len() > 8
        || r.probe_outputs.len() > 16
        || r.tree.files.len() + r.tree.links.len() + r.tree.directories.len() > MAX_VISITS
    {
        return Err("probe_receipt_budget");
    }
    Ok(InstallationReceipt {
        schema: r.schema,
        plan: r.plan,
        version: r.version,
        root: r.root,
        tree: TreeIdentity {
            files: r.tree.files,
            links: r.tree.links,
            directories: r.tree.directories,
        },
        external: r.external,
        probe_outputs: r.probe_outputs,
    })
}
fn complete_tree(plan: &InstallPlan, budget: &IdentityReadBudget) -> Result<TreeIdentity> {
    let mut out = TreeIdentity {
        files: vec![],
        links: vec![],
        directories: vec![],
    };
    let mut pending = vec![plan.payload.clone()];
    if plan.kind == AdapterKind::Corepack {
        pending.push(plan.activation_bin.clone());
    }
    while let Some(path) = pending.pop() {
        elapsed(budget)?;
        budget.snapshot.borrow().visit()?;
        // Open without following links. Non-directories first fail the directory
        // open; fallback regular open still rejects every symlink/special object.
        let directory = budget.snapshot.borrow_mut().open(&path, true);
        let fd = match directory {
            Ok(fd) => fd,
            Err(_) => budget.snapshot.borrow_mut().open(&path, false)?,
        };
        let m = fd.metadata().map_err(|_| "probe_metadata")?;
        if m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o022 != 0 {
            return Err("unsafe_installation_owner_or_mode");
        }
        if m.is_dir() {
            budget
                .snapshot
                .borrow_mut()
                .inventories
                .insert(path.clone(), Stamp::of(&m));
            out.directories.push(path.clone());
            for entry in Dir::read_from(&fd).map_err(|_| "probe_directory")? {
                budget.snapshot.borrow().visit()?;
                let entry = entry.map_err(|_| "probe_directory")?;
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." {
                    continue;
                }
                if pending.len() >= MAX_OPEN {
                    return Err("probe_pending_budget");
                }
                pending.push(path.join(OsStr::from_bytes(name)));
            }
        } else if m.is_file() && m.nlink() == 1 {
            out.files.push(capture(&path, budget)?);
        } else {
            return Err("installation_special_or_hardlink");
        }
    }
    out.files.sort_by(|a, b| a.path.cmp(&b.path));
    out.directories.sort();
    Ok(out)
}
/// Non-serializable evidence usable only inside this single bounded query.
pub(crate) struct VerifiedInstallation<'a> {
    budget: &'a IdentityReadBudget,
    plan: InstallPlan,
    receipt: InstallationReceipt,
}
impl VerifiedInstallation<'_> {
    pub(crate) fn digest(&self) -> Digest {
        hash(&(&self.plan.key(), &self.receipt.digest()))
    }
}
pub(crate) fn inspect_installation<'a>(
    plan: InstallPlan,
    budget: &'a IdentityReadBudget,
) -> Result<VerifiedInstallation<'a>> {
    let root = budget.snapshot.borrow_mut().open(&plan.root, true)?;
    let m = root.metadata().map_err(|_| "probe_metadata")?;
    if m.uid() != rustix::process::geteuid().as_raw() || m.mode() & 0o077 != 0 {
        return Err("unsafe_private_directory");
    }
    budget
        .snapshot
        .borrow_mut()
        .inventories
        .insert(plan.root.clone(), Stamp::of(&m));
    let receipt = receipt(&plan, budget)?;
    receipt.validate_contract(&plan)?;
    if !receipt.tree.links.is_empty() {
        return Err("probe_unsupported_link");
    }
    for dep in &receipt.external {
        if capture(&dep.path, budget)? != *dep {
            return Err("external_identity_changed");
        }
    }
    if complete_tree(&plan, budget)? != receipt.tree {
        return Err("installation_tree_drift");
    }
    for (_, probe) in &plan.probes {
        let id = if let Some(id) = receipt.tree.files.iter().find(|f| f.path == probe.program) {
            id
        } else {
            receipt
                .external
                .iter()
                .find(|f| f.path == probe.program)
                .ok_or("probe_unsupported_program")?
        };
        if !id.executable {
            return Err("installed_tool_not_executable");
        }
    }
    Ok(VerifiedInstallation {
        budget,
        plan,
        receipt,
    })
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn prospective_activation(
    frozen: &FrozenToolchains,
    installed: &[VerifiedInstallation<'_>],
    native: &[super::super::contract::NativeTool],
    base_profile: Digest,
    observations: &BTreeMap<ToolId, super::super::versions::VersionObservation>,
    base_env: &BTreeMap<String, String>,
    setup_policy: Digest,
    shell_path: &Path,
    budget: &IdentityReadBudget,
) -> Result<ActivationReceipt> {
    if installed
        .iter()
        .any(|item| !std::ptr::eq(item.budget, budget))
    {
        return Err("probe_verification_query_mismatch");
    }
    let shell = capture(shell_path, budget)?;
    let pairs = installed
        .iter()
        .map(|v| (&v.plan, &v.receipt))
        .collect::<Vec<_>>();
    compose_activation(
        frozen,
        &pairs,
        native,
        base_profile,
        observations,
        base_env,
        setup_policy,
        &shell,
        || Ok(()),
        |plan, receipt| receipt.validate_contract(plan),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparation_probe_failed_and_growth_reads_consume_shared_budget() {
        let temp = crate::test_tmp::tempdir().unwrap();
        let root = fs::canonicalize(temp.path()).unwrap();
        let path = root.join("growing");
        fs::write(&path, b"aa").unwrap();
        let mut file = File::open(&path).unwrap();
        let budget = IdentityReadBudget::limited(4, 8);
        fs::write(&path, b"abcdefghij").unwrap();
        assert_eq!(
            read(&mut file, &budget, 4, |_| {}),
            Err("identity_too_large")
        );
        assert_eq!(budget.remaining.get(), 3);
        let mut directory = File::open(&root).unwrap();
        let budget = IdentityReadBudget::limited(4, 8);
        assert_eq!(
            read(&mut directory, &budget, 4, |_| {}),
            Err("identity_read")
        );
        assert_eq!(budget.remaining.get(), 3);
        assert_eq!(
            read(&mut directory, &budget, 4, |_| {}),
            Err("identity_read")
        );
        assert_eq!(budget.remaining.get(), 0);
        assert_eq!(
            read(&mut directory, &budget, 4, |_| {}),
            Err("identity_byte_budget")
        );
    }
}
