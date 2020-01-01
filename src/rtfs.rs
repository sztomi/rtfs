use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::ffi::{CStr, CString, OsStr, OsString};

use anyhow;
use fuse_mt::*;
use libc;
use rand::Rng;
use time::*;

use crate::artifactory::{Artifactory, Listing};
use crate::artifactory::Listing::{File, Directory, Error};

const TTL: Timespec = Timespec { sec: 10, nsec: 0 };
const ENOTDIR: libc::c_int = 20;

pub struct RtFS {
  pub rt: Box<Artifactory>,
  pub repo: String,
  _dir_handles: Mutex<HashMap<u64, String>>,
  _last_dir_handle: Mutex<u64>,
  _last_file_handle: u64,
}

fn timestamp_to_timespec(timestamp: &String) -> anyhow::Result<Timespec> {
  const FMT: &'static str = "%Y-%m-%dT%H:%M:%S";
  let parsed = time::strptime(&timestamp, FMT)?;
  Ok(parsed.to_timespec())
}

impl RtFS {
  pub fn new(rt: Box<Artifactory>, repo: String) -> Self {
    let mut rng = rand::thread_rng();
    Self {
      rt: rt,
      repo: repo,
      _dir_handles: Mutex::new(HashMap::new()),
      _last_dir_handle: Mutex::new(rng.gen()),
      _last_file_handle: rng.gen(),
    }
  }

  fn stat_for_path(&self, path: &String) -> anyhow::Result<FileAttr> {
    let path = String::from(match path.as_str() {
      "/" => "",
      _ => path,
    });
    let path = if path.starts_with("/") {
      path[1..].to_owned()
    } else {
      path.to_owned()
    };
    let path = format!("{}/{}", self.repo, path);
    let listing_result = self.rt.storage(&path);
    let listing = match listing_result {
      Ok(lst) => lst,
      Err(e) => panic!(format!("{:?}", e))
    };
    let kind = match listing {
      Listing::File(_) => FileType::RegularFile,
      Listing::Directory(_) => FileType::Directory,
      _ => FileType::Directory,
    };
    let perm = 0o0440;

    Ok(FileAttr {
      size: match &listing {
        Listing::File(fi) => fi.size.parse::<u64>().unwrap(),
        _ => 4096u64,
      },
      blocks: 0,
      atime: match &listing {
        Listing::File(fi) => timestamp_to_timespec(&fi.last_updated)?,
        Listing::Directory(di) => timestamp_to_timespec(&di.last_updated)?,
        _ => Timespec::new(1, 1),
      },
      mtime: match &listing {
        Listing::File(fi) => timestamp_to_timespec(&fi.last_modified)?,
        Listing::Directory(di) => timestamp_to_timespec(&di.last_modified)?,
        _ => Timespec::new(1, 1),
      },
      ctime: match &listing {
        Listing::File(fi) => timestamp_to_timespec(&fi.created)?,
        Listing::Directory(di) => timestamp_to_timespec(&di.created)?,
        _ => Timespec::new(1, 1),
      },
      crtime: Timespec { sec: 0, nsec: 0 },
      kind,
      perm,
      nlink: 1,
      uid: 0,
      gid: 0,
      rdev: 0,
      flags: 0,
    })
  }

  fn get_dir_handle(&self, path: &Path) -> u64 {
    let mut dh = self
      ._last_dir_handle
      .lock()
      .expect("could not lock _last_dir_handle");
    *dh += 1;
    let mut dh_registry = self
      ._dir_handles
      .lock()
      .expect("Could not lock _dir_handles");
    dh_registry.insert(
      *dh,
      path.to_str().expect("Could not convert path").to_string(),
    );
    *dh
  }
}

impl FilesystemMT for RtFS {
  fn init(&self, _req: RequestInfo) -> ResultEmpty {
    debug!("init");
    Ok(())
  }

  fn destroy(&self, _req: RequestInfo) {
    debug!("destroy");
  }

  fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
    debug!("getattr: {:?}", path);
    let path_str = String::from(path.to_str().unwrap_or("/"));
    let attr = self.stat_for_path(&path_str).expect("boo");
    Ok((TTL, attr))
  }

  fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
    debug!("opendir: {:?} (flags = {:#o})", path, _flags);
    let (_, attr) = self.getattr(_req, path, None)?;
    match attr.kind {
      FileType::Directory => {
        let fh = self.get_dir_handle(&path);
        Ok((fh, 0))
      }
      _ => Err(ENOTDIR),
    }
  }

  fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, _flags: u32) -> ResultEmpty {
    debug!("releasedir: {:?}", path);
    let mut dh_registry = self
      ._dir_handles
      .lock()
      .expect("Could not lock _dir_handles");
    if dh_registry.contains_key(&fh) {
      dh_registry.remove(&fh);
    }
    Ok(())
  }

  fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
    debug!("readdir: {:?}", path);
    let mut entries: Vec<DirectoryEntry> = vec![];

    if fh == 0 {
      error!("readdir: missing fh");
      return Err(libc::EINVAL);
    }

    let path_str = format!("{}/{}", self.repo, String::from(path.to_str().unwrap_or("/")));
    let listing = match self.rt.storage(&path_str) {
      Ok(lst) => lst,
      Err(_) => return Ok(entries)
    };

    let listing = match &listing {
      File(_) => panic!("readdir called for non-directory entry"),
      Error(_) => return Ok(entries),
      Directory(d) => d
    };

    for item in &listing.children {
      entries.push(DirectoryEntry {
        name: OsString::from(item.get_name()),
        kind: if item.folder { FileType::Directory } else { FileType::RegularFile }
      });
    }
    Ok(entries)
  }
}
