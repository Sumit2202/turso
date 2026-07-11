use crate::sync::atomic::Ordering;
use crate::util::escape_sql_string_literal;
use crate::{io_error, Connection, LimboError, Result};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;

struct SnapshotOptions<'a> {
    attempt_reflink: bool,
    after_checkpoint: Option<&'a dyn Fn()>,
    before_publish: Option<&'a dyn Fn()>,
}

impl Default for SnapshotOptions<'_> {
    fn default() -> Self {
        Self {
            attempt_reflink: true,
            after_checkpoint: None,
            before_publish: None,
        }
    }
}

enum ReflinkOutcome {
    Created(File),
    Unsupported,
}

impl Connection {
    /// Create a durable, self-contained snapshot at `destination` without
    /// overwriting an existing path.
    ///
    /// This API is for ordinary-WAL databases opened with experimental
    /// multiprocess WAL. It performs a durable FULL checkpoint and keeps
    /// the shared checkpoint and writer authority until a CoW clone is
    /// established. Filesystems without reflink support use `VACUUM INTO`
    /// under a shared-reader snapshot instead of copying the live database
    /// file.
    pub fn snapshot_to_file(self: &Arc<Self>, destination: impl AsRef<Path>) -> Result<()> {
        self.snapshot_to_file_inner(destination.as_ref(), SnapshotOptions::default())
    }

    #[cfg(test)]
    pub(crate) fn snapshot_to_file_for_testing(
        self: &Arc<Self>,
        destination: impl AsRef<Path>,
        attempt_reflink: bool,
        after_checkpoint: Option<&dyn Fn()>,
        before_publish: Option<&dyn Fn()>,
    ) -> Result<()> {
        self.snapshot_to_file_inner(
            destination.as_ref(),
            SnapshotOptions {
                attempt_reflink,
                after_checkpoint,
                before_publish,
            },
        )
    }

    fn snapshot_to_file_inner(
        self: &Arc<Self>,
        destination: &Path,
        options: SnapshotOptions<'_>,
    ) -> Result<()> {
        self.validate_snapshot_preconditions(destination)?;
        ensure_destination_absent(destination)?;

        let parent = destination_parent(destination);
        let staging_dir = tempfile::Builder::new()
            .prefix(".turso-snapshot-")
            .tempdir_in(parent)
            .map_err(|error| io_error(error, "create snapshot staging directory"))?;
        let staged_path = staging_dir.path().join("snapshot.db");

        let pager = self.pager.load();
        let checkpoint_guard = pager.blocking_snapshot_checkpoint()?;
        if let Some(after_checkpoint) = options.after_checkpoint {
            after_checkpoint();
        }

        let reflink = if options.attempt_reflink {
            try_create_reflink(Path::new(&self.db.path), &staged_path)?
        } else {
            ReflinkOutcome::Unsupported
        };

        let staged_file = match reflink {
            ReflinkOutcome::Created(file) => {
                // FICLONE has fixed the staged inode's CoW view; later source
                // writes cannot change it, so peer writers may resume now.
                drop(checkpoint_guard);
                file
            }
            ReflinkOutcome::Unsupported => {
                drop(checkpoint_guard);
                self.create_logical_snapshot(&staged_path)?
            }
        };

        staged_file
            .sync_all()
            .map_err(|error| io_error(error, "sync staged snapshot"))?;
        if let Some(before_publish) = options.before_publish {
            before_publish();
        }

        std::fs::hard_link(&staged_path, destination)
            .map_err(|error| io_error(error, "publish snapshot without replacement"))?;
        let destination_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(destination)
            .map_err(|error| io_error(error, "open published snapshot"))?;
        destination_file
            .sync_all()
            .map_err(|error| io_error(error, "sync published snapshot"))?;
        sync_parent_directory(parent)?;
        Ok(())
    }

    fn validate_snapshot_preconditions(&self, destination: &Path) -> Result<()> {
        if self.is_closed() {
            return Err(LimboError::InternalError("Connection closed".to_string()));
        }
        if !self.experimental_multiprocess_wal_enabled() {
            return Err(LimboError::InvalidArgument(
                "snapshot_to_file requires experimental multiprocess WAL".to_string(),
            ));
        }
        if self.mvcc_enabled() {
            return Err(LimboError::InvalidArgument(
                "snapshot_to_file supports ordinary WAL only, not MVCC".to_string(),
            ));
        }
        if !self.get_auto_commit() || self.n_active_root_statements.load(Ordering::Acquire) != 0 {
            return Err(LimboError::TxError(
                "cannot create a snapshot while a transaction or SQL statement is in progress"
                    .to_string(),
            ));
        }
        if destination.as_os_str().is_empty() || destination.file_name().is_none() {
            return Err(LimboError::InvalidArgument(
                "snapshot destination must name a file".to_string(),
            ));
        }
        if self.pager.load().wal.is_none() {
            return Err(LimboError::InvalidArgument(
                "snapshot_to_file requires an ordinary WAL database".to_string(),
            ));
        }

        #[cfg(host_shared_wal)]
        if self.db.shared_wal_coordination()?.is_none() {
            return Err(LimboError::InvalidArgument(
                "snapshot_to_file requires active shared multiprocess WAL coordination".to_string(),
            ));
        }

        #[cfg(not(host_shared_wal))]
        return Err(LimboError::InvalidArgument(
            "snapshot_to_file is not supported on this platform".to_string(),
        ));

        #[cfg(host_shared_wal)]
        Ok(())
    }

    fn create_logical_snapshot(self: &Arc<Self>, staged_path: &Path) -> Result<File> {
        let staged_path_str = staged_path.to_str().ok_or_else(|| {
            LimboError::InvalidArgument("snapshot path must be valid UTF-8".to_string())
        })?;
        let escaped_path = escape_sql_string_literal(staged_path_str);
        if let Err(error) = self.execute(format!("VACUUM INTO '{escaped_path}'")) {
            let _ = std::fs::remove_file(staged_path);
            return Err(error);
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(staged_path)
            .map_err(|error| io_error(error, "open logical snapshot"))
    }
}

fn ensure_destination_absent(destination: &Path) -> Result<()> {
    match std::fs::symlink_metadata(destination) {
        Ok(_) => Err(io_error(
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "snapshot destination already exists",
            ),
            "create snapshot destination",
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error, "inspect snapshot destination")),
    }
}

fn destination_parent(destination: &Path) -> &Path {
    destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(target_arch = "sparc", target_arch = "sparc64"))
))]
fn try_create_reflink(source: &Path, destination: &Path) -> Result<ReflinkOutcome> {
    let source_file =
        File::open(source).map_err(|error| io_error(error, "open snapshot source"))?;
    let destination_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|error| io_error(error, "create reflink snapshot"))?;

    match rustix::fs::ioctl_ficlone(&destination_file, &source_file) {
        Ok(()) => Ok(ReflinkOutcome::Created(destination_file)),
        Err(error) if reflink_unsupported(error) => {
            drop(destination_file);
            std::fs::remove_file(destination)
                .map_err(|error| io_error(error, "remove unsupported reflink target"))?;
            Ok(ReflinkOutcome::Unsupported)
        }
        Err(error) => {
            drop(destination_file);
            let _ = std::fs::remove_file(destination);
            Err(io_error(
                std::io::Error::from_raw_os_error(error.raw_os_error()),
                "FICLONE snapshot",
            ))
        }
    }
}

#[cfg(all(
    any(target_os = "linux", target_os = "android"),
    not(any(target_arch = "sparc", target_arch = "sparc64"))
))]
fn reflink_unsupported(error: rustix::io::Errno) -> bool {
    matches!(
        error,
        rustix::io::Errno::OPNOTSUPP
            | rustix::io::Errno::XDEV
            | rustix::io::Errno::NOTTY
            | rustix::io::Errno::INVAL
            | rustix::io::Errno::NOSYS
            | rustix::io::Errno::PERM
            | rustix::io::Errno::ACCESS
    )
}

#[cfg(not(all(
    any(target_os = "linux", target_os = "android"),
    not(any(target_arch = "sparc", target_arch = "sparc64"))
)))]
fn try_create_reflink(_source: &Path, _destination: &Path) -> Result<ReflinkOutcome> {
    Ok(ReflinkOutcome::Unsupported)
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error(error, "sync snapshot parent directory"))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<()> {
    Ok(())
}
