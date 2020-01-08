use std::collections::HashMap;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Mutex;

use anyhow;
use fuse_mt::*;
use libc;
use rand::Rng;
use time::*;

use crate::artifactory::Listing::{Directory, Error, File};
use crate::artifactory::{Artifactory, Listing};

const TTL: Timespec = Timespec { sec: 10, nsec: 0 };
const ENOTDIR: libc::c_int = 20;
const EISDIR: libc::c_int = 21;

#[derive(Clone)]
struct FsInfo {
  attr: FileAttr,
  path: String,
}

pub struct RtFS {
  pub rt: Box<Artifactory>,
  pub repo: String,
  _dir_handles: Mutex<HashMap<u64, FsInfo>>,
  _file_handles: Mutex<HashMap<u64, FsInfo>>,
  _last_dir_handle: Mutex<u64>,
  _last_file_handle: Mutex<u64>,
  _uris: Mutex<HashMap<String, String>>,
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
      _file_handles: Mutex::new(HashMap::new()),
      _uris: Mutex::new(HashMap::new()),
      // totally arbitrary range, I just don't want it too high or too low.
      _last_dir_handle: Mutex::new(rng.gen_range(0xaaaa, std::u64::MAX / 2)),
      _last_file_handle: Mutex::new(rng.gen_range(0xaaaa, std::u64::MAX / 2)),
    }
  }

  fn stat_for_path(&self, path: &String, req: &RequestInfo) -> anyhow::Result<FileAttr> {
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
      Err(e) => panic!(format!("{:?}", e)),
    };
    let kind = match listing {
      Listing::File(_) => FileType::RegularFile,
      Listing::Directory(_) => FileType::Directory,
      _ => FileType::Directory,
    };

    let mut _uri_registry = self._uris.lock().expect("could not lock _uris");
    if let Listing::File(f) = &listing {
      let path = path.trim_start_matches(&self.repo);
      _uri_registry.insert(path.to_string(), f.uri.clone());
    }

    let perm = 0o0666;

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
      uid: req.uid,
      gid: req.gid,
      rdev: 0,
      flags: 0,
    })
  }

  fn get_dir_handle(&self, fs_info: &FsInfo) -> u64 {
    let mut dh = self
      ._last_dir_handle
      .lock()
      .expect("could not lock _last_dir_handle");
    *dh += 1;
    let mut dh_registry = self
      ._dir_handles
      .lock()
      .expect("Could not lock _dir_handles");
    dh_registry.insert(*dh, fs_info.clone());
    *dh
  }

  fn get_file_handle(&self, fs_info: &FsInfo) -> u64 {
    let mut fh = self
      ._last_file_handle
      .lock()
      .expect("could not lock _last_file_handle");
    *fh += 1;
    let mut fh_registry = self
      ._file_handles
      .lock()
      .expect("Could not lock _file_handles");
    fh_registry.insert(*fh, fs_info.clone());
    *fh
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

  fn getattr(&self, req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
    debug!("getattr: {:?}", path);
    let path_str = String::from(path.to_str().unwrap_or("/"));
    let attr = self.stat_for_path(&path_str, &req).expect("boo");
    Ok((TTL, attr))
  }

  fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
    debug!("opendir: {:?} (flags = {:#o})", path, _flags);
    let (_, attr) = self.getattr(_req, path, None)?;
    match attr.kind {
      FileType::Directory => {
        let fs_info = FsInfo {
          path: path.to_string_lossy().to_string(),
          attr: attr,
        };
        let fh = self.get_dir_handle(&fs_info);
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

    let path_str = format!(
      "{}/{}",
      self.repo,
      String::from(path.to_str().unwrap_or("/"))
    );
    let listing = match self.rt.storage(&path_str) {
      Ok(lst) => lst,
      Err(_) => return Ok(entries),
    };

    let listing = match &listing {
      File(_) => panic!("readdir called for non-directory entry"),
      Error(_) => return Ok(entries),
      Directory(d) => d,
    };

    for item in &listing.children {
      entries.push(DirectoryEntry {
        name: OsString::from(item.get_name()),
        kind: if item.folder {
          FileType::Directory
        } else {
          FileType::RegularFile
        },
      });
    }
    Ok(entries)
  }

  fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
    debug!("open: {:?} flags={:#x}", path, flags);
    let (_, attr) = self.getattr(_req, path, None)?;
    match attr.kind {
      FileType::RegularFile => {
        let fs_info = FsInfo {
          path: path.to_string_lossy().to_string(),
          attr: attr,
        };
        let fh = self.get_file_handle(&fs_info);
        Ok((fh, 0))
      }
      _ => Err(EISDIR),
    }
  }

  fn release(
    &self,
    _req: RequestInfo,
    path: &Path,
    fh: u64,
    _flags: u32,
    _lock_owner: u64,
    _flush: bool,
  ) -> ResultEmpty {
    debug!("release: {:?}", path);
    let mut fh_registry = self
      ._file_handles
      .lock()
      .expect("Could not lock _dir_handles");
    if fh_registry.contains_key(&fh) {
      fh_registry.remove(&fh);
    }
    Ok(())
  }

  fn read(
    &self,
    _req: RequestInfo,
    path: &Path,
    _fh: u64,
    offset: u64,
    size: u32,
    result: impl FnOnce(Result<&[u8], libc::c_int>),
  ) {
    debug!("read: {:?} {:#x} @ {:#x}", path, size, offset);
    let mut data = Vec::<u8>::with_capacity(size as usize);
    unsafe { data.set_len(size as usize) };

    let _uri_registry = self._uris.lock().expect("could not lock _uris");
    let path_str = path.to_str().expect("could not convert path to str");
    match _uri_registry.get(path_str) {
      Some(uri) => {
        debug!("uri for file: {}", uri);
        self.rt.read_file(&uri, offset, size, &mut data).expect("could not read file");
      },
      None => {
        println!("{:?}", _uri_registry);
        panic!("at the disco");
      }
    }

    result(Ok(&data));
  }
}
