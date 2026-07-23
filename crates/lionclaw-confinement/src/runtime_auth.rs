use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, bail, Context, Result};
use lionclaw_runtime_api::{
    RuntimeAuthProjection, MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES, MAX_RUNTIME_CREDENTIAL_BYTES,
    MAX_RUNTIME_CREDENTIAL_PROJECTIONS,
};
use rustix::{
    fd::OwnedFd,
    fs::{chmodat, fchmod, mkdirat, open, openat, AtFlags, FileType, Mode, OFlags},
    io::Errno,
};

use super::{
    backend::ExecutionRequest,
    mount_validation::normalize_runtime_mount_target,
    plan::{MountAccess, MountSpec, RUNTIME_HOME_MOUNT_TARGET},
};

pub(crate) struct PreparedRuntimeAuth {
    environment: Vec<(String, String)>,
    runtime_home_mount: Option<MountSpec>,
    credential_mounts: Vec<PreparedCredentialMount>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PreparedCredentialMount {
    mount_spec: MountSpec,
}

impl PreparedRuntimeAuth {
    fn new(
        environment: Vec<(String, String)>,
        runtime_home_mount: Option<MountSpec>,
        credential_mounts: Vec<PreparedCredentialMount>,
    ) -> Self {
        Self {
            environment,
            runtime_home_mount,
            credential_mounts,
        }
    }

    pub(crate) fn empty() -> Self {
        Self::new(Vec::new(), None, Vec::new())
    }

    pub(crate) fn environment(&self) -> &[(String, String)] {
        &self.environment
    }

    pub(crate) fn runtime_home_mount(&self) -> Option<&MountSpec> {
        self.runtime_home_mount.as_ref()
    }

    pub(crate) fn credential_mounts(&self) -> &[PreparedCredentialMount] {
        &self.credential_mounts
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        runtime_home_mount: Option<MountSpec>,
        credential_mounts: Vec<MountSpec>,
    ) -> Self {
        Self::new(
            Vec::new(),
            runtime_home_mount,
            credential_mounts
                .into_iter()
                .map(PreparedCredentialMount::new)
                .collect(),
        )
    }
}

impl PreparedCredentialMount {
    fn new(mount_spec: MountSpec) -> Self {
        Self { mount_spec }
    }

    pub(crate) fn mount_spec(&self) -> &MountSpec {
        &self.mount_spec
    }
}

struct ProjectedCredential<'a> {
    staged_source: &'a Path,
    home_target: &'a Path,
    runtime_target: String,
}

pub(crate) fn prepare_runtime_auth(request: &ExecutionRequest) -> Result<PreparedRuntimeAuth> {
    let Some(auth) = &request.program.auth else {
        if request.runtime_auth.is_some() {
            bail!(
                "runtime '{}' materialized auth for a program that declares no auth kind",
                request.plan.runtime_id
            );
        }
        return Ok(PreparedRuntimeAuth::empty());
    };
    let materialization = request.runtime_auth.as_ref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requested unsupported runtime auth kind '{}'",
            request.plan.runtime_id,
            auth.as_str()
        )
    })?;
    if materialization.kind() != auth {
        return Err(anyhow!(
            "runtime '{}' requested auth kind '{}' but received materialized auth kind '{}'",
            request.plan.runtime_id,
            auth.as_str(),
            materialization.kind()
        ));
    }
    let staging_root = request.auth_staging_root.as_deref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' requested auth without an effect-owned auth staging root",
            request.plan.runtime_id
        )
    })?;
    validate_staging_root(staging_root)?;
    validate_projection(request, materialization.projection().clone())
}

fn validate_projection(
    request: &ExecutionRequest,
    projection: RuntimeAuthProjection,
) -> Result<PreparedRuntimeAuth> {
    let credential_count = projection.credentials().len();
    if credential_count > MAX_RUNTIME_CREDENTIAL_PROJECTIONS {
        bail!(
            "runtime '{}' projected {credential_count} credentials; limit is {MAX_RUNTIME_CREDENTIAL_PROJECTIONS}",
            request.plan.runtime_id
        );
    }
    if projection.credentials().is_empty() {
        return Ok(PreparedRuntimeAuth::new(
            projection.environment().to_vec(),
            None,
            Vec::new(),
        ));
    }
    let staging_root = request.auth_staging_root.as_deref().ok_or_else(|| {
        anyhow!(
            "runtime '{}' projected credentials without an auth staging root",
            request.plan.runtime_id
        )
    })?;
    if !staging_root.is_absolute() {
        bail!(
            "runtime '{}' auth staging root '{}' must be absolute",
            request.plan.runtime_id,
            staging_root.display()
        );
    }

    let mut seen_sources = BTreeSet::new();
    let mut seen_targets = BTreeSet::new();
    let mut credentials = Vec::with_capacity(projection.credentials().len());
    for credential in projection.credentials() {
        let source = credential.staged_source();
        let target = credential.native_home_target();
        validate_clean_relative(source, "staged credential source")?;
        validate_clean_relative(target, "native-home credential target")?;
        if !seen_sources.insert(source.to_path_buf()) {
            bail!(
                "runtime '{}' projected staged credential source '{}' more than once",
                request.plan.runtime_id,
                source.display()
            );
        }
        if !seen_targets.insert(target.to_path_buf()) {
            bail!(
                "runtime '{}' projected native-home credential target '{}' more than once",
                request.plan.runtime_id,
                target.display()
            );
        }
        let runtime_target = Path::new(RUNTIME_HOME_MOUNT_TARGET).join(target);
        let runtime_target = runtime_target.to_str().ok_or_else(|| {
            anyhow!(
                "runtime '{}' native-home credential target '{}' is not valid UTF-8",
                request.plan.runtime_id,
                target.display()
            )
        })?;
        credentials.push(ProjectedCredential {
            staged_source: source,
            home_target: target,
            runtime_target: runtime_target.to_string(),
        });
    }

    let home_mount_index = validate_credential_target_topology(&request.plan.mounts, &credentials)?;
    let mut aggregate_bytes = 0usize;
    let mut staged_sources = Vec::with_capacity(credentials.len());
    for credential in &credentials {
        let (source, observed_bytes) =
            validate_staged_regular_file(staging_root, credential.staged_source).with_context(
                || {
                    format!(
                        "runtime '{}' staged credential '{}' is invalid",
                        request.plan.runtime_id,
                        credential.staged_source.display()
                    )
                },
            )?;
        aggregate_bytes = aggregate_bytes.checked_add(observed_bytes).ok_or_else(|| {
            anyhow!(
                "runtime '{}' staged credential aggregate size overflowed",
                request.plan.runtime_id
            )
        })?;
        if aggregate_bytes > MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES {
            bail!(
                "runtime '{}' staged credential aggregate is {aggregate_bytes} bytes; limit is {MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES}",
                request.plan.runtime_id
            );
        }
        staged_sources.push(source);
    }
    let (home_mount, home_directory) =
        validate_runtime_home_mount(&request.plan.mounts, home_mount_index).with_context(|| {
            format!(
                "runtime '{}' cannot project credentials into its retained home",
                request.plan.runtime_id
            )
        })?;

    let mut credential_mounts = Vec::with_capacity(credentials.len());
    for (credential, source) in credentials.iter().zip(staged_sources) {
        prepare_credential_mountpoint(&home_directory, &home_mount.source, credential.home_target)
            .with_context(|| {
                format!(
                    "runtime '{}' native-home credential target '{}' is invalid",
                    request.plan.runtime_id,
                    credential.home_target.display()
                )
            })?;
        credential_mounts.push(PreparedCredentialMount::new(MountSpec {
            source,
            target: credential.runtime_target.clone(),
            access: MountAccess::ReadOnly,
        }));
    }

    Ok(PreparedRuntimeAuth::new(
        projection.environment().to_vec(),
        Some(home_mount),
        credential_mounts,
    ))
}

fn validate_credential_target_topology(
    plan_mounts: &[MountSpec],
    credentials: &[ProjectedCredential<'_>],
) -> Result<usize> {
    for (index, credential) in credentials.iter().enumerate() {
        for other in &credentials[index + 1..] {
            if paths_overlap(credential.home_target, other.home_target) {
                bail!(
                    "native-home credential targets '{}' and '{}' must not be ancestors or descendants of each other",
                    credential.home_target.display(),
                    other.home_target.display()
                );
            }
        }
    }

    let home_mount_index = canonical_runtime_home_mount_index(plan_mounts)?;
    let runtime_home = Path::new(RUNTIME_HOME_MOUNT_TARGET);
    for (index, mount) in plan_mounts.iter().enumerate() {
        if index == home_mount_index {
            continue;
        }
        let normalized = normalize_runtime_mount_target(&mount.target).map_err(|detail| {
            anyhow!(
                "runtime plan mount target '{}' is invalid while validating credential destinations: {detail}",
                mount.target
            )
        })?;
        let plan_target = Path::new(&normalized);
        if !plan_target.starts_with(runtime_home) {
            continue;
        }
        for credential in credentials {
            let credential_target = Path::new(&credential.runtime_target);
            if paths_overlap(plan_target, credential_target) {
                bail!(
                    "runtime plan mount target '{}' overlaps native-home credential target '{}'",
                    normalized,
                    credential.runtime_target
                );
            }
        }
    }
    Ok(home_mount_index)
}

fn canonical_runtime_home_mount_index(plan_mounts: &[MountSpec]) -> Result<usize> {
    let mut matches = plan_mounts
        .iter()
        .enumerate()
        .filter(|(_, mount)| mount.target == RUNTIME_HOME_MOUNT_TARGET);
    let Some((index, _)) = matches.next() else {
        bail!(
            "credential projection requires exactly one canonical '{}' plan mount; found none",
            RUNTIME_HOME_MOUNT_TARGET
        );
    };
    if matches.next().is_some() {
        bail!(
            "credential projection requires exactly one canonical '{}' plan mount; found more than one",
            RUNTIME_HOME_MOUNT_TARGET
        );
    }
    Ok(index)
}

fn validate_runtime_home_mount(
    plan_mounts: &[MountSpec],
    home_mount_index: usize,
) -> Result<(MountSpec, OwnedFd)> {
    let mount = &plan_mounts[home_mount_index];
    let (directory, source) =
        open_exact_absolute_directory(&mount.source, "retained runtime home source")?;

    Ok((
        MountSpec {
            source,
            target: RUNTIME_HOME_MOUNT_TARGET.to_string(),
            access: mount.access,
        },
        directory,
    ))
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn validate_clean_relative(path: &Path, label: &str) -> Result<()> {
    let mut saw_component = false;
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("{label} '{}' must be a clean relative path", path.display());
        }
        saw_component = true;
    }
    if !saw_component {
        bail!("{label} is required");
    }
    Ok(())
}

fn validate_staged_regular_file(root: &Path, relative: &Path) -> Result<(PathBuf, usize)> {
    let (mut directory, validated_root) = open_exact_absolute_directory(root, "auth staging root")?;
    let components = relative.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            unreachable!("relative credential path was validated");
        };
        let path = root.join(components[..=index].iter().fold(
            PathBuf::new(),
            |mut path, component| {
                path.push(component.as_os_str());
                path
            },
        ));
        if index + 1 == components.len() {
            let file = openat(
                &directory,
                *name,
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(|error| {
                anyhow!(
                    "failed to open staged credential '{}': {error}",
                    path.display()
                )
            })?;
            let stat = rustix::fs::fstat(&file).with_context(|| {
                format!("failed to inspect staged credential '{}'", path.display())
            })?;
            if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
                bail!(
                    "staged credential '{}' must be a regular file",
                    path.display()
                );
            }
            let observed_bytes = usize::try_from(stat.st_size).map_err(|_| {
                anyhow!(
                    "staged credential '{}' reported an invalid size",
                    path.display()
                )
            })?;
            if observed_bytes > MAX_RUNTIME_CREDENTIAL_BYTES {
                bail!(
                    "staged credential '{}' exceeds the {} byte limit",
                    path.display(),
                    MAX_RUNTIME_CREDENTIAL_BYTES
                );
            }
            return Ok((validated_root.join(relative), observed_bytes));
        } else {
            directory =
                openat(&directory, *name, directory_flags(), Mode::empty()).map_err(|error| {
                    anyhow!(
                        "staged credential parent '{}' must be an exact real directory: {error}",
                        path.display()
                    )
                })?;
        }
    }
    unreachable!("relative credential path validation requires a component")
}

fn prepare_credential_mountpoint(
    home_directory: &OwnedFd,
    home_source: &Path,
    relative: &Path,
) -> Result<()> {
    let components = relative.components().collect::<Vec<_>>();
    let Some((leaf, parents)) = components.split_last() else {
        bail!("native-home credential target is required");
    };
    let Component::Normal(leaf) = leaf else {
        unreachable!("native-home credential target was validated");
    };

    let mut directory = home_directory
        .try_clone()
        .context("failed to duplicate retained runtime home directory")?;
    let mut display = home_source.to_path_buf();
    for component in parents {
        let Component::Normal(name) = component else {
            unreachable!("native-home credential target was validated");
        };
        display.push(name);
        let created = match mkdirat(&directory, *name, Mode::from_raw_mode(0o700)) {
            Ok(()) => true,
            Err(Errno::EXIST) => false,
            Err(error) => {
                return Err(anyhow!(
                    "failed to create credential target parent '{}': {error}",
                    display.display()
                ));
            }
        };
        if created {
            chmodat(
                &directory,
                *name,
                Mode::from_raw_mode(0o700),
                AtFlags::empty(),
            )
            .with_context(|| {
                format!(
                    "failed to protect credential target parent '{}'",
                    display.display()
                )
            })?;
        }
        directory =
            openat(&directory, *name, directory_flags(), Mode::empty()).map_err(|error| {
                anyhow!(
                    "credential target parent '{}' must be an exact real directory: {error}",
                    display.display()
                )
            })?;
    }

    display.push(leaf);
    let file = match openat(
        &directory,
        *leaf,
        OFlags::WRONLY
            | OFlags::CREATE
            | OFlags::EXCL
            | OFlags::CLOEXEC
            | OFlags::NOFOLLOW
            | OFlags::NONBLOCK,
        Mode::from_raw_mode(0o600),
    ) {
        Ok(file) => file,
        Err(Errno::EXIST) => openat(
            &directory,
            *leaf,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(|error| {
            anyhow!(
                "credential target mountpoint '{}' must be an exact regular file: {error}",
                display.display()
            )
        })?,
        Err(error) => {
            return Err(anyhow!(
                "failed to create credential target mountpoint '{}': {error}",
                display.display()
            ));
        }
    };
    let stat = rustix::fs::fstat(&file).with_context(|| {
        format!(
            "failed to inspect credential target mountpoint '{}'",
            display.display()
        )
    })?;
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
        bail!(
            "credential target mountpoint '{}' must be a regular file",
            display.display()
        );
    }
    if stat.st_size != 0 {
        bail!(
            "credential target mountpoint '{}' must be empty",
            display.display()
        );
    }
    fchmod(&file, Mode::from_raw_mode(0o600)).with_context(|| {
        format!(
            "failed to protect credential target mountpoint '{}'",
            display.display()
        )
    })?;
    Ok(())
}

fn validate_staging_root(root: &Path) -> Result<()> {
    if !root.is_absolute() {
        bail!("auth staging root '{}' must be absolute", root.display());
    }
    let _root = open_exact_absolute_directory(root, "auth staging root")?;
    Ok(())
}

fn open_exact_absolute_directory(path: &Path, label: &str) -> Result<(OwnedFd, PathBuf)> {
    if !path.is_absolute() {
        bail!("{label} '{}' must be absolute", path.display());
    }

    let mut directory = open(Path::new("/"), directory_flags(), Mode::empty())
        .context("failed to open filesystem root")?;
    let mut validated = PathBuf::from("/");
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                validated.push(name);
                directory = openat(&directory, name, directory_flags(), Mode::empty()).map_err(
                    |error| {
                        anyhow!(
                            "{label} '{}' must be an exact real directory: {error}",
                            validated.display()
                        )
                    },
                )?;
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                bail!("{label} '{}' must be a clean absolute path", path.display());
            }
        }
    }
    Ok((directory, validated))
}

fn directory_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK
}

#[cfg(test)]
mod tests {
    use lionclaw_runtime_api::{
        RuntimeAuthIdentity, RuntimeAuthMaterialization, RuntimeCredentialProjection,
    };

    use super::*;
    use crate::{
        ConfinementConfig, EffectiveExecutionPlan, ExecutionLimits, MountAccess, MountSpec,
        NetworkMode, OciConfinementConfig, RuntimeAuthKind, RuntimeProgramSpec, WorkspaceAccess,
    };

    const TEST_AUTH_KIND: &str = "test-auth";

    fn materialization(projection: RuntimeAuthProjection) -> RuntimeAuthMaterialization {
        RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static(TEST_AUTH_KIND),
            RuntimeAuthIdentity::new("test-principal").unwrap(),
            projection,
        )
    }

    fn sample_request(
        network_mode: NetworkMode,
        staging_root: Option<PathBuf>,
        credential: bool,
    ) -> ExecutionRequest {
        let credentials = if credential {
            if let Some(root) = &staging_root {
                std::fs::write(root.join("credential"), b"secret").unwrap();
            }
            vec![
                RuntimeCredentialProjection::new("credential", ".agent/auth.json")
                    .expect("credential projection"),
            ]
        } else {
            Vec::new()
        };
        ExecutionRequest {
            plan: EffectiveExecutionPlan {
                runtime_id: "test-runtime".to_string(),
                preset_name: "everyday".to_string(),
                confinement: ConfinementConfig::Oci(OciConfinementConfig::default()),
                workspace_access: WorkspaceAccess::ReadWrite,
                network_mode,
                install_policy: crate::InstallPolicy::User,
                root_in_userns: false,
                working_dir: None,
                environment: Vec::new(),
                mcp_servers: Vec::new(),
                mounts: vec![
                    MountSpec {
                        source: "/tmp/lionclaw-runtime-auth-test".into(),
                        target: "/runtime".to_string(),
                        access: MountAccess::ReadWrite,
                    },
                    MountSpec {
                        source: "/tmp/lionclaw-runtime-auth-test-home".into(),
                        target: "/runtime/home".to_string(),
                        access: MountAccess::ReadWrite,
                    },
                ],
                mount_runtime_secrets: false,
                escape_classes: Default::default(),
                limits: ExecutionLimits::default(),
            },
            program: RuntimeProgramSpec {
                executable: "test-runtime".to_string(),
                args: vec!["exec".to_string()],
                environment: Vec::new(),
                stdin: "hello".to_string(),
                auth: Some(RuntimeAuthKind::from_static(TEST_AUTH_KIND)),
            },
            resource_name: None,
            runtime_secrets_mount: None,
            auth_staging_root: staging_root,
            runtime_auth: Some(materialization(RuntimeAuthProjection::new(
                vec![("TEST_AUTH".to_string(), "enabled".to_string())],
                credentials,
            ))),
        }
    }

    fn set_runtime_home_source(request: &mut ExecutionRequest, source: &Path) {
        request
            .plan
            .mounts
            .iter_mut()
            .find(|mount| mount.target == RUNTIME_HOME_MOUNT_TARGET)
            .expect("runtime home mount")
            .source = source.to_path_buf();
    }

    fn projection(credentials: &[(&str, &str)]) -> RuntimeAuthProjection {
        RuntimeAuthProjection::new(
            Vec::new(),
            credentials
                .iter()
                .map(|(source, target)| {
                    RuntimeCredentialProjection::new(source, target).expect("credential projection")
                })
                .collect(),
        )
    }

    fn write_staged_credentials(staging: &Path, names: &[&str]) {
        for name in names {
            std::fs::write(staging.join(name), b"secret").unwrap();
        }
    }

    fn numbered_projection(count: usize) -> RuntimeAuthProjection {
        RuntimeAuthProjection::new(
            Vec::new(),
            (0..count)
                .map(|index| {
                    RuntimeCredentialProjection::new(
                        format!("credential-{index}"),
                        format!(".agent/credential-{index}"),
                    )
                    .expect("credential projection")
                })
                .collect(),
        )
    }

    fn write_sized_staged_credentials(staging: &Path, sizes: &[usize]) {
        for (index, size) in sizes.iter().enumerate() {
            std::fs::File::create(staging.join(format!("credential-{index}")))
                .unwrap()
                .set_len(*size as u64)
                .unwrap();
        }
    }

    #[test]
    fn materialized_auth_kind_must_match_the_program_contract() {
        let staging = tempfile::tempdir().unwrap();
        let mut request =
            sample_request(NetworkMode::None, Some(staging.path().to_path_buf()), false);
        request.runtime_auth = Some(RuntimeAuthMaterialization::new(
            RuntimeAuthKind::from_static("different-auth"),
            RuntimeAuthIdentity::new("test-principal").unwrap(),
            RuntimeAuthProjection::default(),
        ));
        let err = prepare_runtime_auth(&request)
            .err()
            .expect("mismatched materialized auth must fail");

        assert!(err
            .to_string()
            .contains("received materialized auth kind 'different-auth'"));
    }

    #[test]
    fn validates_and_maps_exact_staged_credentials_under_native_home() {
        let staging = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let mut request = sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), true);
        set_runtime_home_source(&mut request, home.path());
        let prepared = prepare_runtime_auth(&request).expect("prepared auth");

        assert_eq!(
            prepared.environment(),
            [("TEST_AUTH".to_string(), "enabled".to_string())]
        );
        assert_eq!(
            prepared.credential_mounts()[0].mount_spec(),
            &MountSpec {
                source: staging.path().join("credential"),
                target: "/runtime/home/.agent/auth.json".to_string(),
                access: MountAccess::ReadOnly,
            }
        );
        let mountpoint = home.path().join(".agent/auth.json");
        assert!(mountpoint.is_file());
        assert_eq!(std::fs::read(&mountpoint).unwrap(), b"");
        assert_eq!(
            prepared
                .runtime_home_mount()
                .expect("validated home")
                .source,
            home.path()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(mountpoint).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn rejects_projection_count_before_staged_validation_or_mountpoint_creation() {
        let staging = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let mut request =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        set_runtime_home_source(&mut request, home.path());

        let error = validate_projection(
            &request,
            numbered_projection(MAX_RUNTIME_CREDENTIAL_PROJECTIONS + 1),
        )
        .err()
        .expect("projection above the credential count limit must fail");

        assert!(
            error.to_string().contains(&format!(
                "projected {} credentials; limit is {}",
                MAX_RUNTIME_CREDENTIAL_PROJECTIONS + 1,
                MAX_RUNTIME_CREDENTIAL_PROJECTIONS
            )),
            "unexpected error: {error:#}"
        );
        assert!(
            std::fs::read_dir(home.path()).unwrap().next().is_none(),
            "count rejection must happen before credential mountpoints are created"
        );
    }

    #[test]
    fn enforces_aggregate_credential_bytes_before_mountpoint_creation() {
        let full_sized_count =
            MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES / MAX_RUNTIME_CREDENTIAL_BYTES;
        assert_eq!(
            MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES % MAX_RUNTIME_CREDENTIAL_BYTES,
            0
        );
        assert!(full_sized_count < MAX_RUNTIME_CREDENTIAL_PROJECTIONS);

        {
            let staging = tempfile::tempdir().unwrap();
            let home = tempfile::tempdir().unwrap();
            let sizes = vec![MAX_RUNTIME_CREDENTIAL_BYTES; full_sized_count];
            write_sized_staged_credentials(staging.path(), &sizes);
            let mut request =
                sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
            set_runtime_home_source(&mut request, home.path());

            let prepared = validate_projection(&request, numbered_projection(full_sized_count))
                .expect("the exact aggregate credential byte limit must be accepted");
            assert_eq!(prepared.credential_mounts().len(), full_sized_count);
        }

        let staging = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let mut sizes = vec![MAX_RUNTIME_CREDENTIAL_BYTES; full_sized_count];
        sizes.push(1);
        write_sized_staged_credentials(staging.path(), &sizes);
        let mut request =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        set_runtime_home_source(&mut request, home.path());

        let error = validate_projection(&request, numbered_projection(sizes.len()))
            .err()
            .expect("aggregate above the credential byte limit must fail");
        assert!(
            error.to_string().contains(&format!(
                "aggregate is {} bytes; limit is {}",
                MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES + 1,
                MAX_RUNTIME_CREDENTIAL_AGGREGATE_BYTES
            )),
            "unexpected error: {error:#}"
        );
        assert!(
            !home.path().join(".agent").exists(),
            "aggregate rejection must happen before credential mountpoints are created"
        );
    }

    #[test]
    fn credential_projection_requires_one_canonical_runtime_home_mount() {
        let staging = tempfile::tempdir().unwrap();
        write_staged_credentials(staging.path(), &["credential"]);

        let mut missing =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        missing
            .plan
            .mounts
            .retain(|mount| mount.target != RUNTIME_HOME_MOUNT_TARGET);
        missing.plan.mounts.push(MountSpec {
            source: "/tmp/noncanonical-home".into(),
            target: "/runtime//home".to_string(),
            access: MountAccess::ReadWrite,
        });
        let error =
            validate_projection(&missing, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("noncanonical home mount must fail");
        assert!(error
            .to_string()
            .contains("exactly one canonical '/runtime/home'"));

        let mut duplicate =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        duplicate.plan.mounts.push(MountSpec {
            source: "/tmp/other-home".into(),
            target: RUNTIME_HOME_MOUNT_TARGET.to_string(),
            access: MountAccess::ReadWrite,
        });
        let error = validate_projection(
            &duplicate,
            projection(&[("credential", ".agent/auth.json")]),
        )
        .err()
        .expect("duplicate home mounts must fail");
        assert!(error
            .to_string()
            .contains("exactly one canonical '/runtime/home'"));
    }

    #[test]
    fn rejects_overlapping_credential_and_nested_plan_targets() {
        let staging = tempfile::tempdir().unwrap();
        write_staged_credentials(staging.path(), &["first", "second"]);
        let request = sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        let error = validate_projection(
            &request,
            projection(&[("first", ".agent"), ("second", ".agent/auth.json")]),
        )
        .err()
        .expect("credential ancestor overlap must fail");
        assert!(error.to_string().contains("ancestors or descendants"));

        for plan_target in [
            "/runtime/home/.agent",
            "/runtime/home/.agent/auth.json",
            "/runtime/home/.agent/auth.json/nested",
            "/runtime//home",
        ] {
            let mut request =
                sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
            request.plan.mounts.push(MountSpec {
                source: "/tmp/competing-mount".into(),
                target: plan_target.to_string(),
                access: MountAccess::ReadWrite,
            });
            let error = validate_projection(&request, projection(&[("first", ".agent/auth.json")]))
                .err()
                .expect("overlapping nested plan mount must fail");
            assert!(error.to_string().contains("overlaps"));
        }

        let home = tempfile::tempdir().unwrap();
        let mut disjoint =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        set_runtime_home_source(&mut disjoint, home.path());
        disjoint.plan.mounts.push(MountSpec {
            source: "/tmp/disjoint-mount".into(),
            target: "/runtime/home/.cache".to_string(),
            access: MountAccess::ReadWrite,
        });
        validate_projection(&disjoint, projection(&[("first", ".agent/auth.json")]))
            .expect("disjoint nested plan mount is allowed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_symlinked_and_non_regular_staged_credentials() {
        use std::os::unix::fs::symlink;

        let staging = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        symlink(outside.path(), staging.path().join("credential")).unwrap();
        let projection = RuntimeAuthProjection::new(
            Vec::new(),
            vec![RuntimeCredentialProjection::new("credential", ".agent/auth.json").unwrap()],
        );
        let request = sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        let err = validate_projection(&request, projection)
            .err()
            .expect("symlinked credential must fail");
        assert!(format!("{err:#}").contains("staged credential"));

        std::fs::remove_file(staging.path().join("credential")).unwrap();
        std::fs::create_dir(staging.path().join("credential")).unwrap();
        let projection = RuntimeAuthProjection::new(
            Vec::new(),
            vec![RuntimeCredentialProjection::new("credential", ".agent/auth.json").unwrap()],
        );
        let err = validate_projection(&request, projection)
            .err()
            .expect("credential directory must fail");
        assert!(format!("{err:#}").contains("regular file"));

        std::fs::remove_dir(staging.path().join("credential")).unwrap();
        let oversized = std::fs::File::create(staging.path().join("credential")).unwrap();
        oversized
            .set_len((MAX_RUNTIME_CREDENTIAL_BYTES + 1) as u64)
            .unwrap();
        let projection = RuntimeAuthProjection::new(
            Vec::new(),
            vec![RuntimeCredentialProjection::new("credential", ".agent/auth.json").unwrap()],
        );
        let err = validate_projection(&request, projection)
            .err()
            .expect("oversized credential must fail");
        assert!(format!("{err:#}").contains("exceeds"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_retained_home_and_credential_mountpoints() {
        use std::os::unix::fs::symlink;

        let staging = tempfile::tempdir().unwrap();
        write_staged_credentials(staging.path(), &["credential"]);

        let parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("home")).unwrap();
        symlink(outside.path(), parent.path().join("linked")).unwrap();
        let mut request =
            sample_request(NetworkMode::On, Some(staging.path().to_path_buf()), false);
        set_runtime_home_source(&mut request, &parent.path().join("linked/home"));
        let error =
            validate_projection(&request, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("symlinked retained-home ancestor must fail");
        assert!(format!("{error:#}").contains("retained runtime home source"));

        let home = tempfile::tempdir().unwrap();
        symlink(outside.path(), home.path().join(".agent")).unwrap();
        set_runtime_home_source(&mut request, home.path());
        let error =
            validate_projection(&request, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("symlinked credential target parent must fail");
        assert!(format!("{error:#}").contains("exact real directory"));
        assert!(!outside.path().join("auth.json").exists());

        std::fs::remove_file(home.path().join(".agent")).unwrap();
        std::fs::create_dir(home.path().join(".agent")).unwrap();
        let outside_file = outside.path().join("outside-auth");
        std::fs::write(&outside_file, b"outside").unwrap();
        symlink(&outside_file, home.path().join(".agent/auth.json")).unwrap();
        let error =
            validate_projection(&request, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("symlinked credential mountpoint must fail");
        assert!(format!("{error:#}").contains("exact regular file"));
        assert_eq!(std::fs::read(outside_file).unwrap(), b"outside");

        std::fs::remove_file(home.path().join(".agent/auth.json")).unwrap();
        std::fs::create_dir(home.path().join(".agent/auth.json")).unwrap();
        let error =
            validate_projection(&request, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("non-regular credential mountpoint must fail");
        assert!(format!("{error:#}").contains("regular file"));

        std::fs::remove_dir(home.path().join(".agent/auth.json")).unwrap();
        std::fs::write(home.path().join(".agent/auth.json"), b"retained").unwrap();
        let error =
            validate_projection(&request, projection(&[("credential", ".agent/auth.json")]))
                .err()
                .expect("nonempty credential mountpoint must fail");
        assert!(format!("{error:#}").contains("must be empty"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_symlinked_staging_root_before_validating_materialized_auth() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let staging = parent.path().join("auth-staging");
        symlink(outside.path(), &staging).unwrap();

        let err = prepare_runtime_auth(&sample_request(NetworkMode::On, Some(staging), false))
            .err()
            .expect("symlinked root must fail");

        assert!(format!("{err:#}").contains("exact real directory"));
        assert!(!outside.path().join("credential").exists());
    }
}
