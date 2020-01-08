use base64;
use reqwest;
use anyhow;

#[derive(Deserialize, Debug)]
pub struct DirEntry {
  pub folder: bool,
  pub uri: String,
}

#[derive(Deserialize, Debug)]
pub struct DirInfo {
  pub children: Vec<DirEntry>,
  pub created: String,

  #[serde(rename = "lastModified")]
  pub last_modified: String,

  #[serde(rename = "lastUpdated")]
  pub last_updated: String,

  pub path: String,
  pub repo: String,
  pub uri: String,
}

#[derive(Deserialize, Debug)]
pub struct Checksums {
  #[serde(default)]
  pub md5: String,
  #[serde(default)]
  pub sha1: String,
  #[serde(default)]
  pub sha256: String,
}

#[derive(Deserialize, Debug)]
pub struct FileInfo {
  pub checksums: Checksums,
  pub created: String,

  #[serde(rename = "createdBy")]
  pub created_by: String,

  #[serde(rename = "downloadUri")]
  pub download_uri: String,

  #[serde(rename = "lastModified")]
  pub last_modified: String,

  #[serde(rename = "lastUpdated")]
  pub last_updated: String,

  #[serde(rename = "mimeType")]
  pub mime_type: String,

  #[serde(rename = "modifiedBy")]
  pub modified_by: String,

  #[serde(rename = "originalChecksums")]
  pub original_checksums: Checksums,

  pub path: String,
  pub repo: String,
  pub size: String,
  pub uri: String,
}

#[derive(Deserialize, Debug)]
pub struct RtError {
  message: String,
  status: i16,
}

#[derive(Deserialize, Debug)]
pub struct RtErrors {
  errors: Vec<RtError>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Listing {
  Directory(DirInfo),
  File(FileInfo),
  Error(RtErrors),
}

pub struct Artifactory {
  pub host: String,
  pub user: String,
  pub token: String,
  _client: reqwest::Client,
  _auth: String,
}

impl Artifactory {
  pub fn new(host: &String, user: &String, token: &String) -> Self {
    fn make_auth(user: &String, token: &String) -> String {
      let compound = format!("{}:{}", &user, &token);
      format!("Basic {}", base64::encode(&compound))
    }
    Self {
      host: host.clone(),
      user: user.clone(),
      token: token.clone(),
      _client: reqwest::Client::new(),
      _auth: make_auth(&user, &token),
    }
  }

  pub fn storage(&self, path: &String) -> anyhow::Result<Listing> {
    let endpoint = format!("storage/{}", path);
    let listing = self._api(&endpoint)?.json()?;
    Ok(listing)
  }

  pub fn read_file(&self, uri: &String, offset: u64, size: u32, buf: &mut Vec<u8>) -> reqwest::Result<reqwest::Response> {
    let mut resp = self
      ._client
      .get(uri)
      .header("Authorization", &self._auth)
      .header("Range", format!("bytes={}-{}", offset, offset+(size as u64)))
      .send()?;
    resp.copy_to(buf).expect("could not copy file data to buffer");
    Ok(resp)
  }

  fn _api(&self, endpoint: &str) -> reqwest::Result<reqwest::Response> {
    let url = format!("{}/api/{}", self.host, endpoint);
    self._get(&url)
  }

  fn _get(&self, url: &str) -> reqwest::Result<reqwest::Response> {
    self
      ._client
      .get(url)
      .header("Authorization", &self._auth)
      .send()
  }

}

impl DirEntry {
  pub fn get_name(&self) -> &str {
    &self.uri[1..]
  }
}
