//! Kernel-owned mission-driver exclusion.

use std::fs::{File, OpenOptions};
use std::path::Path;

use anyhow::{Context, Result};
use rustix::fs::{flock, FlockOperation};

/// An exclusive mission-driver lock. The lock is released when this file is
/// dropped, including when the process exits abnormally.
pub struct DriverGuard {
    _file: File,
}

impl DriverGuard {
    pub fn try_acquire(path: &Path) -> Result<Option<Self>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("creating mission lock directory '{}'", parent.display())
            })?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("opening mission driver lock '{}'", path.display()))?;
        match flock(&file, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => Ok(Some(Self { _file: file })),
            Err(rustix::io::Errno::AGAIN) => Ok(None),
            Err(error) => Err(error)
                .with_context(|| format!("locking mission driver lock '{}'", path.display())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::AsFd;
    use std::process::Command;
    use std::time::Duration;

    use super::*;

    fn is_held(path: &Path) -> bool {
        DriverGuard::try_acquire(path).unwrap().is_none()
    }

    #[test]
    fn lock_is_exclusive_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("driver.lock");
        let first = DriverGuard::try_acquire(&path).unwrap().unwrap();
        assert!(is_held(&path));
        drop(first);
        assert!(!is_held(&path));
    }

    #[test]
    fn lock_descriptor_is_close_on_exec() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("driver.lock");
        let guard = DriverGuard::try_acquire(&path).unwrap().unwrap();
        let flags = rustix::io::fcntl_getfd(guard._file.as_fd()).unwrap();
        assert!(flags.contains(rustix::io::FdFlags::CLOEXEC));
    }

    #[test]
    fn child_process_holds_lock_fixture() {
        let Ok(path) = std::env::var("LIONCLAW_TEST_DRIVER_LOCK") else {
            return;
        };
        let ready = std::env::var("LIONCLAW_TEST_DRIVER_READY").unwrap();
        let _guard = DriverGuard::try_acquire(Path::new(&path))
            .unwrap()
            .expect("child acquires lock");
        std::fs::write(ready, b"ready").unwrap();
        std::thread::sleep(Duration::from_secs(60));
    }

    #[test]
    fn process_death_releases_the_lock_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("driver.lock");
        let ready = dir.path().join("ready");
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "driver_lock::tests::child_process_holds_lock_fixture",
                "--exact",
            ])
            .env("LIONCLAW_TEST_DRIVER_LOCK", &path)
            .env("LIONCLAW_TEST_DRIVER_READY", &ready)
            .spawn()
            .unwrap();
        for _ in 0..200 {
            if ready.is_file() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.is_file(), "child did not acquire the lock");
        assert!(is_held(&path));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(DriverGuard::try_acquire(&path).unwrap().is_some());
    }
}
