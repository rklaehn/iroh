use std::collections::VecDeque;
use std::fmt::{self, Debug, Display, Formatter};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use anyhow::{anyhow, bail, ensure, Context as _, Result};
use async_trait::async_trait;
use bytes::Bytes;
use cid::multihash::{Code, MultihashDigest};
use cid::Cid;
use futures::{Future, Stream};
use iroh_metrics::inc;
use iroh_rpc_client::Client;
use libipld::codec::{Decode, Encode};
use libipld::error::{InvalidMultihash, UnsupportedMultihash};
use libipld::prelude::Codec as _;
use libipld::{Ipld, IpldCodec};
use tokio::io::{AsyncRead, AsyncSeek};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use iroh_metrics::{
    core::{MObserver, MRecorder},
    gateway::{GatewayHistograms, GatewayMetrics},
    observe, record,
    resolver::ResolverMetrics,
};

use crate::codecs::Codec;
use crate::unixfs::{
    poll_read_buf_at_pos, DataType, UnixfsChildStream, UnixfsContentReader, UnixfsNode,
};

pub const IROH_STORE: &str = "iroh-store";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    cid: Cid,
    data: Bytes,
    links: Vec<Cid>,
}

impl Block {
    pub fn new(cid: Cid, data: Bytes, links: Vec<Cid>) -> Self {
        Self { cid, data, links }
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }

    pub fn links(&self) -> &[Cid] {
        &self.links
    }

    pub fn raw_data_size(&self) -> Option<u64> {
        let codec = Codec::try_from(self.cid.codec()).unwrap();
        match codec {
            Codec::Raw => Some(self.data.len() as u64),
            _ => None,
        }
    }

    /// Validate the block. Will return an error if the hash or the links are wrong.
    pub fn validate(&self) -> Result<()> {
        // check that the cid is supported
        let code = self.cid.hash().code();
        let mh = Code::try_from(code)
            .map_err(|_| UnsupportedMultihash(code))?
            .digest(&self.data);
        // check that the hash matches the data
        if mh.digest() != self.cid.hash().digest() {
            return Err(InvalidMultihash(mh.to_bytes()).into());
        }
        // check that the links are complete
        let links = parse_links(&self.cid, &self.data)?;
        anyhow::ensure!(links == self.links, "links do not match");
        Ok(())
    }

    pub fn into_parts(self) -> (Cid, Bytes, Vec<Cid>) {
        (self.cid, self.data, self.links)
    }
}

/// Represents an ipfs path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path {
    typ: PathType,
    root: CidOrDomain,
    tail: Vec<String>,
}

impl Path {
    pub fn from_cid(cid: Cid) -> Self {
        Path {
            typ: PathType::Ipfs,
            root: CidOrDomain::Cid(cid),
            tail: Vec::new(),
        }
    }

    pub fn typ(&self) -> PathType {
        self.typ
    }

    pub fn root(&self) -> &CidOrDomain {
        &self.root
    }

    pub fn tail(&self) -> &[String] {
        &self.tail
    }

    // used only for string path manipulation
    pub fn has_trailing_slash(&self) -> bool {
        !self.tail.is_empty() && self.tail.last().unwrap().is_empty()
    }

    pub fn push(&mut self, str: impl AsRef<str>) {
        self.tail.push(str.as_ref().to_owned());
    }

    // Empty path segments in the *middle* shouldn't occur,
    // though they can occur at the end, which `join` handles.
    // TODO(faassen): it would make sense to return a `RelativePathBuf` here at some
    // point in the future so we don't deal with bare strings anymore and
    // we're forced to handle various cases more explicitly.
    pub fn to_relative_string(&self) -> String {
        self.tail.join("/")
    }

    pub fn cid(&self) -> Option<&Cid> {
        match &self.root {
            CidOrDomain::Cid(cid) => Some(cid),
            CidOrDomain::Domain(_) => None,
        }
    }
}

/// Holds information if we should clip the response and to what offset
#[derive(Debug, Clone, Copy)]
pub enum ResponseClip {
    NoClip,
    Clip(usize),
}

impl From<usize> for ResponseClip {
    fn from(item: usize) -> Self {
        if item == 0 {
            ResponseClip::NoClip
        } else {
            ResponseClip::Clip(item)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CidOrDomain {
    Cid(Cid),
    Domain(String),
}

impl Display for CidOrDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CidOrDomain::Cid(c) => std::fmt::Display::fmt(&c, f),
            CidOrDomain::Domain(s) => std::fmt::Display::fmt(&s, f),
        }
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "/{}/{}", self.typ.as_str(), self.root)?;

        for part in &self.tail {
            if part.is_empty() {
                continue;
            }
            write!(f, "/{}", part)?;
        }

        if self.has_trailing_slash() {
            write!(f, "/")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathType {
    /// `/ipfs`
    Ipfs,
    /// `/ipns`
    Ipns,
}

impl PathType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            PathType::Ipfs => "ipfs",
            PathType::Ipns => "ipns",
        }
    }
}

impl FromStr for Path {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(&['/', '\\']).filter(|s| !s.is_empty());

        let first_part = parts.next().ok_or_else(|| anyhow!("path too short"))?;
        let (typ, root) = if first_part.eq_ignore_ascii_case("ipns") {
            let root = parts.next().ok_or_else(|| anyhow!("path too short"))?;
            let root = if let Ok(c) = Cid::from_str(root) {
                CidOrDomain::Cid(c)
            } else {
                // TODO: url validation?
                CidOrDomain::Domain(root.to_string())
            };

            (PathType::Ipns, root)
        } else {
            let root = if first_part.eq_ignore_ascii_case("ipfs") {
                parts.next().ok_or_else(|| anyhow!("path too short"))?
            } else {
                first_part
            };

            let root = Cid::from_str(root).context("invalid cid")?;

            (PathType::Ipfs, CidOrDomain::Cid(root))
        };

        let mut tail: Vec<String> = parts.map(Into::into).collect();

        if s.ends_with('/') {
            tail.push("".to_owned());
        }

        Ok(Path { typ, root, tail })
    }
}

#[async_trait]
pub trait LinksContainer: Sync + Send + std::fmt::Debug + Clone + 'static {
    /// Extract links out of a container struct.
    fn links(&self) -> Result<Vec<Cid>>;
}

#[async_trait]
impl LinksContainer for OutRaw {
    fn links(&self) -> Result<Vec<Cid>> {
        parse_links(&self.cid, &self.content)
    }
}

#[async_trait]
impl LinksContainer for Out {
    fn links(&self) -> Result<Vec<Cid>> {
        Out::links(self)
    }
}

#[derive(Debug, Clone)]
pub struct OutRaw {
    source: Source,
    content: Bytes,
    cid: Cid,
}

impl OutRaw {
    pub fn from_loaded(cid: Cid, loaded: LoadedCid) -> Self {
        Self {
            source: loaded.source,
            content: loaded.data,
            cid,
        }
    }
    pub fn source(&self) -> &Source {
        &self.source
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn content(&self) -> &Bytes {
        &self.content
    }
}

#[derive(Debug, Clone)]
pub struct Out {
    metadata: Metadata,
    pub(crate) content: OutContent,
    context: LoaderContext,
}

impl Out {
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Is this content mutable?
    ///
    /// Returns `true` if the underlying root is an IPNS entry.
    pub fn is_mutable(&self) -> bool {
        matches!(self.metadata.path.typ, PathType::Ipns)
    }

    pub fn is_dir(&self) -> bool {
        match &self.content {
            OutContent::Unixfs(node) => node.is_dir(),
            _ => false,
        }
    }

    pub fn is_symlink(&self) -> bool {
        self.metadata.unixfs_type == Some(UnixfsType::Symlink)
    }

    /// What kind of content this is this.
    pub fn typ(&self) -> OutType {
        self.content.typ()
    }

    pub fn links(&self) -> Result<Vec<Cid>> {
        self.content.links()
    }

    /// Returns links with an associated file or directory name if the content
    /// is unixfs
    pub fn named_links(&self) -> Result<Vec<(Option<&str>, Cid)>> {
        match &self.content {
            OutContent::Unixfs(node) => node.links().map(|l| l.map(|l| (l.name, l.cid))).collect(),
            _ => {
                let links = self.content.links();
                links.map(|l| l.into_iter().map(|l| (None, l)).collect())
            }
        }
    }

    /// Returns a stream over the content of this directory.
    /// Only if this is of type `unixfs` and a directory.
    pub fn unixfs_read_dir<'a, 'b: 'a, C: ContentLoader>(
        &'a self,
        loader: &'b Resolver<C>,
        om: OutMetrics,
    ) -> Result<Option<UnixfsChildStream<'a>>> {
        match &self.content {
            OutContent::Unixfs(node) => node.as_child_reader(self.context.clone(), loader, om),
            _ => Ok(None),
        }
    }

    pub fn pretty<T: ContentLoader>(
        self,
        loader: Resolver<T>,
        om: OutMetrics,
        clip: ResponseClip,
    ) -> Result<OutPrettyReader<T>> {
        let pos = 0;
        match self.content {
            OutContent::DagPb(_, mut bytes) => {
                if let ResponseClip::Clip(n) = clip {
                    bytes.truncate(n);
                }
                Ok(OutPrettyReader::DagPb(BytesReader { pos, bytes, om }))
            }
            OutContent::DagCbor(_, mut bytes) => {
                if let ResponseClip::Clip(n) = clip {
                    bytes.truncate(n);
                }
                Ok(OutPrettyReader::DagCbor(BytesReader { pos, bytes, om }))
            }
            OutContent::DagJson(_, mut bytes) => {
                if let ResponseClip::Clip(n) = clip {
                    bytes.truncate(n);
                }
                Ok(OutPrettyReader::DagJson(BytesReader { pos, bytes, om }))
            }
            OutContent::Raw(_, mut bytes) => {
                if let ResponseClip::Clip(n) = clip {
                    bytes.truncate(n);
                }
                Ok(OutPrettyReader::Raw(BytesReader { pos, bytes, om }))
            }
            OutContent::Unixfs(node) => {
                let ctx = self.context;
                let reader = node
                    .into_content_reader(ctx, loader, om, clip)?
                    .ok_or_else(|| anyhow!("cannot read the contents of a directory"))?;

                Ok(OutPrettyReader::Unixfs(reader))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum OutContent {
    DagPb(Ipld, Bytes),
    Unixfs(UnixfsNode),
    DagCbor(Ipld, Bytes),
    DagJson(Ipld, Bytes),
    Raw(Ipld, Bytes),
}

impl OutContent {
    pub(crate) fn typ(&self) -> OutType {
        match self {
            OutContent::DagPb(_, _) => OutType::DagPb,
            OutContent::Unixfs(_) => OutType::Unixfs,
            OutContent::DagCbor(_, _) => OutType::DagCbor,
            OutContent::DagJson(_, _) => OutType::DagJson,
            OutContent::Raw(_, _) => OutType::Raw,
        }
    }

    pub(crate) fn links(&self) -> Result<Vec<Cid>> {
        match self {
            OutContent::DagPb(ipld, _)
            | OutContent::DagCbor(ipld, _)
            | OutContent::DagJson(ipld, _)
            | OutContent::Raw(ipld, _) => {
                let mut links = Vec::new();
                ipld.references(&mut links);
                Ok(links)
            }
            OutContent::Unixfs(node) => node.links().map(|r| r.map(|r| r.cid)).collect(),
        }
    }
}

/// Metadata for the reolution result.
#[derive(Debug, Clone)]
pub struct Metadata {
    /// The original path for that was resolved.
    pub path: Path,
    /// Size in bytes.
    pub size: Option<u64>,
    pub typ: OutType,
    pub unixfs_type: Option<UnixfsType>,
    /// List of resolved cids. In order of the `path`.
    ///
    /// Only contains the "top level cids", and only path segments that actually map
    /// to a block.
    pub resolved_path: Vec<Cid>,
    pub source: Source,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutType {
    DagPb,
    Unixfs,
    DagCbor,
    DagJson,
    Raw,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnixfsType {
    Dir,
    File,
    Symlink,
}

pub enum OutPrettyReader<T: ContentLoader> {
    DagPb(BytesReader),
    Unixfs(UnixfsContentReader<T>),
    DagCbor(BytesReader),
    DagJson(BytesReader),
    Raw(BytesReader),
}

impl<T: ContentLoader> Debug for OutPrettyReader<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            OutPrettyReader::DagPb(_) => write!(f, "OutPrettyReader::DabPb"),
            OutPrettyReader::Unixfs(_) => write!(f, "OutPrettyReader::Unixfs"),
            OutPrettyReader::DagCbor(_) => write!(f, "OutPrettyReader::DagCbor"),
            OutPrettyReader::DagJson(_) => write!(f, "OutPrettyReader::DagJson"),
            OutPrettyReader::Raw(_) => write!(f, "OutPrettyReader::Raw"),
        }
    }
}

impl<T: ContentLoader> OutPrettyReader<T> {
    /// Returns the size in bytes, if known in advance.
    pub fn size(&self) -> Option<u64> {
        match self {
            OutPrettyReader::DagPb(reader)
            | OutPrettyReader::DagCbor(reader)
            | OutPrettyReader::DagJson(reader)
            | OutPrettyReader::Raw(reader) => reader.size(),
            OutPrettyReader::Unixfs(reader) => reader.size(),
        }
    }
}

pub struct BytesReader {
    pos: usize,
    bytes: Bytes,
    om: OutMetrics,
}

impl BytesReader {
    /// Returns the size in bytes, if known in advance.
    pub fn size(&self) -> Option<u64> {
        Some(self.bytes.len() as u64)
    }
}

pub struct OutMetrics {
    pub start: Instant,
}

impl OutMetrics {
    pub fn observe_bytes_read(&self, pos: usize, bytes_read: usize) {
        if pos == 0 && bytes_read > 0 {
            record!(
                GatewayMetrics::TimeToServeFirstBlock,
                self.start.elapsed().as_millis() as u64
            );
        }
        if bytes_read == 0 {
            record!(
                GatewayMetrics::TimeToServeFullFile,
                self.start.elapsed().as_millis() as u64
            );
            observe!(
                GatewayHistograms::TimeToServeFullFile,
                self.start.elapsed().as_millis() as f64
            );
        }
        record!(GatewayMetrics::BytesStreamed, bytes_read as u64);
    }
}

impl Default for OutMetrics {
    fn default() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl<T: ContentLoader + Unpin + 'static> AsyncRead for OutPrettyReader<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            OutPrettyReader::DagPb(bytes_reader)
            | OutPrettyReader::DagCbor(bytes_reader)
            | OutPrettyReader::DagJson(bytes_reader)
            | OutPrettyReader::Raw(bytes_reader) => {
                let pos_current = bytes_reader.pos;
                let res = poll_read_buf_at_pos(
                    &mut bytes_reader.pos,
                    ResponseClip::Clip(bytes_reader.bytes.len()),
                    &bytes_reader.bytes,
                    buf,
                );
                let bytes_read = bytes_reader.pos - pos_current;
                bytes_reader.om.observe_bytes_read(pos_current, bytes_read);
                Poll::Ready(res)
            }
            OutPrettyReader::Unixfs(r) => Pin::new(&mut *r).poll_read(cx, buf),
        }
    }
}

impl<T: ContentLoader + Unpin + 'static> AsyncSeek for OutPrettyReader<T> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        match &mut *self {
            OutPrettyReader::DagPb(bytes_reader)
            | OutPrettyReader::DagCbor(bytes_reader)
            | OutPrettyReader::DagJson(bytes_reader)
            | OutPrettyReader::Raw(bytes_reader) => {
                let pos_current = bytes_reader.pos as i64;
                let data_len = bytes_reader.bytes.len();
                if data_len == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "cannot seek on empty data",
                    ));
                }
                match position {
                    std::io::SeekFrom::Start(pos) => {
                        let i = std::cmp::min(data_len - 1, pos as usize);
                        bytes_reader.pos = i;
                    }
                    std::io::SeekFrom::End(pos) => {
                        let mut i = (data_len as i64 + pos) % data_len as i64;
                        if i < 0 {
                            i += data_len as i64;
                        }
                        bytes_reader.pos = i as usize;
                    }
                    std::io::SeekFrom::Current(pos) => {
                        let mut i = std::cmp::min(data_len as i64 - 1, pos_current as i64 + pos);
                        i = std::cmp::max(0, i);
                        bytes_reader.pos = i as usize;
                    }
                }
                Ok(())
            }
            OutPrettyReader::Unixfs(r) => Pin::new(&mut *r).start_seek(position),
        }
    }

    fn poll_complete(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        match &mut *self {
            OutPrettyReader::DagPb(bytes_reader)
            | OutPrettyReader::DagCbor(bytes_reader)
            | OutPrettyReader::DagJson(bytes_reader)
            | OutPrettyReader::Raw(bytes_reader) => Poll::Ready(Ok(bytes_reader.pos as u64)),
            OutPrettyReader::Unixfs(r) => Pin::new(&mut *r).poll_complete(_cx),
        }
    }
}

#[derive(Debug)]
pub struct LoadedCid {
    pub data: Bytes,
    pub source: Source,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Bitswap,
    Http(String),
    Store(&'static str),
}

#[derive(Debug, Clone)]
pub struct Resolver<T: ContentLoader> {
    loader: T,
    next_id: Arc<AtomicU64>,
    worker: Option<Arc<(oneshot::Sender<()>, JoinHandle<()>)>>,
    session_closer: mpsc::Sender<ContextId>,
}

impl<T: ContentLoader> Drop for Resolver<T> {
    fn drop(&mut self) {
        if self.worker.is_none() {
            panic!("drop called multiple times");
        }
        if Arc::strong_count(self.worker.as_ref().unwrap()) == 1 {
            let worker = Arc::try_unwrap(self.worker.take().unwrap()).expect("last arc");
            worker.0.send(()).ok();
        }
    }
}

#[derive(Debug, Clone)]
pub struct LoaderContext {
    id: ContextId,
    inner: Arc<Mutex<InnerLoaderContext>>,
}

impl LoaderContext {
    pub fn from_path(id: ContextId, closer: mpsc::Sender<ContextId>, path: Path) -> Self {
        trace!("new loader context: {:?}", id);
        LoaderContext {
            id,
            inner: Arc::new(Mutex::new(InnerLoaderContext { path, closer })),
        }
    }

    pub fn id(&self) -> ContextId {
        self.id
    }
}

impl Drop for LoaderContext {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            if let Err(err) = self
                .inner
                .try_lock()
                .expect("last reference, no lock")
                .closer
                .try_send(self.id)
            {
                warn!(
                    "failed to send session stop for session {}: {:?}",
                    self.id, err
                );
            }
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ContextId(u64);

impl Display for ContextId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ContextId({})", self.0)
    }
}

impl From<u64> for ContextId {
    fn from(id: u64) -> Self {
        ContextId(id)
    }
}

impl From<ContextId> for u64 {
    fn from(id: ContextId) -> Self {
        id.0
    }
}

#[derive(Debug)]
struct InnerLoaderContext {
    #[allow(dead_code)]
    path: Path,
    closer: mpsc::Sender<ContextId>,
}

#[async_trait]
pub trait ContentLoader: Sync + Send + std::fmt::Debug + Clone + 'static {
    /// Loads the actual content of a given cid.
    async fn load_cid(&self, cid: &Cid, ctx: &LoaderContext) -> Result<LoadedCid>;
    /// Signal that the passend in session is not used anymore.
    async fn stop_session(&self, ctx: ContextId) -> Result<()>;
    /// Checks if the given cid is present in the local storage.
    async fn has_cid(&self, cid: &Cid) -> Result<bool>;
}

#[async_trait]
impl<T: ContentLoader> ContentLoader for Arc<T> {
    async fn load_cid(&self, cid: &Cid, ctx: &LoaderContext) -> Result<LoadedCid> {
        self.as_ref().load_cid(cid, ctx).await
    }

    async fn stop_session(&self, ctx: ContextId) -> Result<()> {
        self.as_ref().stop_session(ctx).await
    }

    async fn has_cid(&self, cid: &Cid) -> Result<bool> {
        self.as_ref().has_cid(cid).await
    }
}

#[async_trait]
impl ContentLoader for Client {
    async fn stop_session(&self, ctx: ContextId) -> Result<()> {
        self.try_p2p()?.stop_session_bitswap(ctx.into()).await?;
        Ok(())
    }

    async fn load_cid(&self, cid: &Cid, ctx: &LoaderContext) -> Result<LoadedCid> {
        trace!("{:?} loading {}", ctx.id(), cid);

        // TODO: better strategy

        let cid = *cid;
        match self.try_store()?.get(cid).await {
            Ok(Some(data)) => {
                trace!("{:?} retrieved from store", ctx.id());
                return Ok(LoadedCid {
                    data,
                    source: Source::Store(IROH_STORE),
                });
            }
            Ok(None) => {}
            Err(err) => {
                warn!(
                    "{:?} failed to fetch data from store {}: {:?}",
                    ctx.id(),
                    cid,
                    err
                );
            }
        }

        // launch fetching using the initial set of cached providers
        let bytes = self
            .try_p2p()?
            .fetch_bitswap(ctx.id().into(), cid, Default::default())
            .await?;

        // trigger storage in the background
        let clone = bytes.clone();
        let store = self.store.as_ref().cloned();
        let p2p = self.try_p2p()?.clone();

        tokio::spawn(async move {
            let clone2 = clone.clone();
            let links =
                tokio::task::spawn_blocking(move || parse_links(&cid, &clone2).unwrap_or_default())
                    .await
                    .unwrap_or_default();

            let len = clone.len();
            let links_len = links.len();
            if let Some(store_rpc) = store.as_ref() {
                match store_rpc.put(cid, clone.clone(), links).await {
                    Ok(_) => {
                        debug!("stored {} ({}bytes, {}links)", cid, len, links_len);

                        // Notify bitswap about new blocks
                        p2p.notify_new_blocks_bitswap(vec![(cid, clone)]).await.ok();
                    }
                    Err(err) => {
                        warn!("failed to store {}: {:?}", cid, err);
                    }
                }
            } else {
                warn!("failed to store: missing store rpc conn");
            }
        });

        trace!("retrieved from p2p");

        Ok(LoadedCid {
            data: bytes,
            source: Source::Bitswap,
        })
    }

    async fn has_cid(&self, cid: &Cid) -> Result<bool> {
        let cid = *cid;
        self.try_store()?.has(cid).await
    }
}

impl<T: ContentLoader> Resolver<T> {
    pub fn new(loader: T) -> Self {
        let (closer_s, mut closer_r) = oneshot::channel();
        let (session_closer_s, mut session_closer_r) = mpsc::channel(64);

        let loader_thread = loader.clone();
        let worker = tokio::task::spawn(async move {
            // GC Loop for sessions
            loop {
                tokio::select! {
                    biased;
                    session = session_closer_r.recv() => {
                        match session {
                            Some(session) => {
                                let loader = loader_thread.clone();
                                // Spawn to make sure the channel has always capacity.
                                tokio::task::spawn(async move {
                                    debug!("stopping session {}", session);
                                    if let Err(err) = loader.stop_session(session).await {
                                        warn!("failed to stop session {}: {:?}", session, err);
                                    }
                                });
                            }
                            None => {
                                warn!("session_closer channel broke");
                                break;
                            }
                        }
                    }
                    _ = &mut closer_r => {
                        break;
                    }
                }
            }
        });

        Resolver {
            loader,
            next_id: Arc::new(AtomicU64::new(0)),
            worker: Some(Arc::new((closer_s, worker))),
            session_closer: session_closer_s,
        }
    }

    fn next_id(&self) -> ContextId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        ContextId(id)
    }

    pub fn loader(&self) -> &T {
        &self.loader
    }

    #[tracing::instrument(skip(self))]
    pub fn resolve_recursive_with_paths(
        &self,
        root: Path,
    ) -> impl Stream<Item = Result<(Path, Out)>> {
        let mut blocks = VecDeque::new();
        let this = self.clone();
        async_stream::try_stream! {
            let output_path = root.clone();
            blocks.push_back((output_path, this.resolve(root).await));
            loop {
                if let Some((current_output_path, current_out)) = blocks.pop_front() {
                    let current = current_out?;
                    let links = current.named_links()?;
                    // TODO: configurable limit
                    for link_chunk in links.chunks(8) {
                        let next = futures::future::join_all(
                            link_chunk.iter().map(|(link_name, link)| {
                                let this = this.clone();
                                let mut this_path = current_output_path.clone();
                                match link_name {
                                    None => this_path.push(link.to_string()),
                                    Some(p) =>  this_path.push(p),
                                };
                                async move {
                                    (this_path, this.resolve(Path::from_cid(*link)).await)
                                }
                            })
                        ).await;
                        for res in next.into_iter() {
                            blocks.push_back(res);
                        }
                    }
                    yield (current_output_path, current);
                } else {
                    // no links left to resolve
                    break;
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub fn resolve_recursive(&self, root: Path) -> impl Stream<Item = Result<Out>> {
        let this = self.clone();
        self.resolve_recursive_mapped(root, None, move |cid, ctx| {
            let this = this.clone();
            async move { this.resolve_with_ctx(ctx, Path::from_cid(cid)).await }
        })
    }

    /// Resolve a path recursively and yield the raw bytes plus metadata.
    #[tracing::instrument(skip(self))]
    pub fn resolve_recursive_raw(
        &self,
        root: Path,
        recursion_limit: Option<usize>,
    ) -> impl Stream<Item = Result<OutRaw>> {
        let this = self.clone();
        self.resolve_recursive_mapped(root, recursion_limit, move |cid, mut ctx| {
            let this = this.clone();
            async move {
                this.load_cid(&cid, &mut ctx)
                    .await
                    .map(|loaded| OutRaw::from_loaded(cid, loaded))
            }
        })
    }

    /// Resolve a path recursively and supply a closure to resolve cids to outputs.
    #[tracing::instrument(skip(self, resolve))]
    pub fn resolve_recursive_mapped<O, M, F>(
        &self,
        root: Path,
        recursion_limit: Option<usize>,
        resolve: M,
    ) -> impl Stream<Item = Result<O>>
    where
        O: LinksContainer,
        M: Fn(Cid, LoaderContext) -> F + Clone,
        F: Future<Output = Result<O>> + Send + 'static,
    {
        let mut ctx =
            LoaderContext::from_path(self.next_id(), self.session_closer.clone(), root.clone());

        let mut cids = VecDeque::new();
        let this = self.clone();
        let mut counter = 0;
        let chunk_size = 8;
        async_stream::try_stream! {
            let root_cid = this.resolve_path_to_cid(&root, &mut ctx).await?;
            let root_block = resolve(root_cid, ctx.clone()).await?;
            cids.push_back(root_block);
            loop {
                if let Some(current) = cids.pop_front() {
                    let links = current.links()?;
                    counter += links.len();
                    if let Some(limit) = recursion_limit {
                        if counter > limit {
                            Err(anyhow::anyhow!("Number of links exceeds the recursion limit."))?;
                        }
                    }

                    // TODO: configurable limit
                    for link_chunk in links.chunks(chunk_size) {
                        let next = futures::future::join_all(
                            link_chunk.iter().map(|link| {
                                let resolve = resolve.clone();
                                let ctx = ctx.clone();
                                async move {
                                    resolve(*link, ctx).await
                                }
                            })
                        ).await;
                        for res in next.into_iter() {
                            let res = res?;
                            cids.push_back(res);
                        }
                    }
                    yield current;

                } else {
                    // no links left to resolve
                    break;
                }
            }
        }
    }

    /// Resolves through a given path, returning the [`Cid`] and raw bytes of the final leaf.
    #[tracing::instrument(skip(self))]
    pub async fn resolve(&self, path: Path) -> Result<Out> {
        let ctx =
            LoaderContext::from_path(self.next_id(), self.session_closer.clone(), path.clone());

        self.resolve_with_ctx(ctx, path).await
    }

    pub async fn resolve_with_ctx(&self, mut ctx: LoaderContext, path: Path) -> Result<Out> {
        // Resolve the root block.
        let (root_cid, loaded_cid) = self.resolve_root(&path, &mut ctx).await?;
        match loaded_cid.source {
            Source::Store(_) => inc!(ResolverMetrics::CacheHit),
            _ => inc!(ResolverMetrics::CacheMiss),
        }

        let codec = Codec::try_from(root_cid.codec()).context("unknown codec")?;

        match codec {
            Codec::DagPb => {
                self.resolve_dag_pb_or_unixfs(path, root_cid, loaded_cid, ctx)
                    .await
            }
            Codec::DagCbor => self.resolve_dag_cbor(path, root_cid, loaded_cid, ctx).await,
            Codec::DagJson => self.resolve_dag_json(path, root_cid, loaded_cid, ctx).await,
            Codec::Raw => self.resolve_raw(path, root_cid, loaded_cid, ctx).await,
            _ => bail!("unsupported codec {:?}", codec),
        }
    }

    async fn inner_resolve(
        &self,
        current: &mut UnixfsNode,
        resolved_path: &mut Vec<Cid>,
        part: &str,
        ctx: &mut LoaderContext,
    ) -> Result<()> {
        match current {
            UnixfsNode::Directory(_) => {
                let next_link = current
                    .get_link_by_name(&part)
                    .await?
                    .ok_or_else(|| anyhow!("UnixfsNode::Directory link '{}' not found", part))?;
                let loaded_cid = self.load_cid(&next_link.cid, ctx).await?;
                let next_node = UnixfsNode::decode(&next_link.cid, loaded_cid.data)?;
                resolved_path.push(next_link.cid);

                *current = next_node;
            }
            UnixfsNode::HamtShard(_, hamt) => {
                let (next_link, next_node) = hamt
                    .get(ctx.clone(), self, part.as_bytes())
                    .await?
                    .ok_or_else(|| anyhow!("UnixfsNode::HamtShard link '{}' not found", part))?;
                // TODO: is this the right way to to resolved path here?
                resolved_path.push(next_link.cid);

                *current = next_node.clone();
            }
            _ => {
                bail!("unexpected unixfs type {:?}", current.typ());
            }
        }

        Ok(())
    }

    /// Resolves through both DagPb and nested UnixFs DAGs.
    #[tracing::instrument(skip(self, loaded_cid))]
    async fn resolve_dag_pb_or_unixfs(
        &self,
        root_path: Path,
        cid: Cid,
        loaded_cid: LoadedCid,
        mut ctx: LoaderContext,
    ) -> Result<Out> {
        trace!("{:?} resolving {} for {}", ctx.id(), cid, root_path);
        if let Ok(node) = UnixfsNode::decode(&cid, loaded_cid.data.clone()) {
            let tail = &root_path.tail;
            let mut current = node;
            let mut resolved_path = vec![cid];

            for part in tail {
                self.inner_resolve(&mut current, &mut resolved_path, part, &mut ctx)
                    .await?;
            }

            let unixfs_type = match current.typ() {
                Some(DataType::Directory) => Some(UnixfsType::Dir),
                Some(DataType::HamtShard) => Some(UnixfsType::Dir),
                Some(DataType::File) | Some(DataType::Raw) => Some(UnixfsType::File),
                Some(DataType::Symlink) => Some(UnixfsType::Symlink),
                Some(DataType::Metadata) => None,
                None => {
                    // this means the file is raw
                    Some(UnixfsType::File)
                }
            };
            let metadata = Metadata {
                path: root_path,
                size: current.filesize(),
                typ: OutType::Unixfs,
                unixfs_type,
                resolved_path,
                source: loaded_cid.source,
            };
            Ok(Out {
                metadata,
                context: ctx,
                content: OutContent::Unixfs(current),
            })
        } else {
            self.resolve_dag_pb(root_path, cid, loaded_cid, ctx).await
        }
    }

    #[tracing::instrument(skip(self, loaded_cid))]
    async fn resolve_dag_pb(
        &self,
        root_path: Path,
        cid: Cid,
        loaded_cid: LoadedCid,
        mut ctx: LoaderContext,
    ) -> Result<Out> {
        trace!("{:?} resolving {} for {}", ctx.id(), cid, root_path);
        let ipld: libipld::Ipld = libipld::IpldCodec::DagPb
            .decode(&loaded_cid.data)
            .map_err(|e| anyhow!("invalid dag cbor: {:?}", e))?;

        let out = self
            .resolve_ipld(
                cid,
                libipld::IpldCodec::DagPb,
                ipld,
                &root_path.tail,
                &mut ctx,
            )
            .await?;

        // reencode if we only return part of the original
        let bytes = if root_path.tail.is_empty() {
            loaded_cid.data
        } else {
            let mut bytes = Vec::new();
            out.encode(libipld::IpldCodec::DagCbor, &mut bytes)?;
            bytes.into()
        };

        let metadata = Metadata {
            path: root_path,
            size: Some(bytes.len() as u64),
            typ: OutType::DagPb,
            unixfs_type: None,
            resolved_path: vec![cid],
            source: loaded_cid.source,
        };
        Ok(Out {
            metadata,
            context: ctx,
            content: OutContent::DagPb(out, bytes),
        })
    }

    #[tracing::instrument(skip(self, loaded_cid))]
    async fn resolve_dag_cbor(
        &self,
        root_path: Path,
        cid: Cid,
        loaded_cid: LoadedCid,
        mut ctx: LoaderContext,
    ) -> Result<Out> {
        trace!("{:?} resolving {} for {}", ctx.id(), cid, root_path);
        let ipld: libipld::Ipld = libipld::IpldCodec::DagCbor
            .decode(&loaded_cid.data)
            .map_err(|e| anyhow!("invalid dag cbor: {:?}", e))?;

        let out = self
            .resolve_ipld(
                cid,
                libipld::IpldCodec::DagCbor,
                ipld,
                &root_path.tail,
                &mut ctx,
            )
            .await?;

        // reencode if we only return part of the original
        let bytes = if root_path.tail.is_empty() {
            loaded_cid.data
        } else {
            let mut bytes = Vec::new();
            out.encode(libipld::IpldCodec::DagCbor, &mut bytes)?;
            bytes.into()
        };

        let metadata = Metadata {
            path: root_path,
            size: Some(bytes.len() as u64),
            typ: OutType::DagCbor,
            unixfs_type: None,
            resolved_path: vec![cid],
            source: loaded_cid.source,
        };
        Ok(Out {
            metadata,
            context: ctx,
            content: OutContent::DagCbor(out, bytes),
        })
    }

    #[tracing::instrument(skip(self, loaded_cid))]
    async fn resolve_dag_json(
        &self,
        root_path: Path,
        cid: Cid,
        loaded_cid: LoadedCid,
        mut ctx: LoaderContext,
    ) -> Result<Out> {
        trace!("{:?} resolving {} for {}", ctx.id(), cid, root_path);
        let ipld: libipld::Ipld = libipld::IpldCodec::DagJson
            .decode(&loaded_cid.data)
            .map_err(|e| anyhow!("invalid dag json: {:?}", e))?;

        let out = self
            .resolve_ipld(
                cid,
                libipld::IpldCodec::DagJson,
                ipld,
                &root_path.tail,
                &mut ctx,
            )
            .await?;

        // reencode if we only return part of the original
        let bytes = if root_path.tail.is_empty() {
            loaded_cid.data
        } else {
            let mut bytes = Vec::new();
            out.encode(libipld::IpldCodec::DagJson, &mut bytes)?;
            bytes.into()
        };

        let metadata = Metadata {
            path: root_path,
            size: Some(bytes.len() as u64),
            typ: OutType::DagJson,
            unixfs_type: None,
            resolved_path: vec![cid],
            source: loaded_cid.source,
        };
        Ok(Out {
            metadata,
            context: ctx,
            content: OutContent::DagJson(out, bytes),
        })
    }

    #[tracing::instrument(skip(self, loaded_cid))]
    async fn resolve_raw(
        &self,
        root_path: Path,
        cid: Cid,
        loaded_cid: LoadedCid,
        mut ctx: LoaderContext,
    ) -> Result<Out> {
        trace!("{:?} resolving {} for {}", ctx.id(), cid, root_path);
        let ipld: libipld::Ipld = libipld::IpldCodec::Raw
            .decode(&loaded_cid.data)
            .map_err(|e| anyhow!("invalid raw: {:?}", e))?;

        let out = self
            .resolve_ipld(
                cid,
                libipld::IpldCodec::Raw,
                ipld,
                &root_path.tail,
                &mut ctx,
            )
            .await?;

        let metadata = Metadata {
            path: root_path,
            size: Some(loaded_cid.data.len() as u64),
            typ: OutType::Raw,
            unixfs_type: None,
            resolved_path: vec![cid],
            source: loaded_cid.source,
        };
        Ok(Out {
            metadata,
            context: ctx,
            content: OutContent::Raw(out, loaded_cid.data),
        })
    }

    #[tracing::instrument(skip(self, root))]
    async fn resolve_ipld(
        &self,
        _cid: Cid,
        codec: libipld::IpldCodec,
        root: Ipld,
        path: &[String],
        ctx: &mut LoaderContext,
    ) -> Result<Ipld> {
        let mut root = root;
        let mut current = root;

        for part in path {
            if let libipld::Ipld::Link(c) = current {
                let new_codec: libipld::IpldCodec = c.codec().try_into()?;
                ensure!(
                    new_codec == codec,
                    "can only resolve the same codec {:?} != {:?}",
                    new_codec,
                    codec
                );

                // resolve link and update if we have encountered a link
                let loaded_cid = self.load_cid(&c, ctx).await?;
                root = codec
                    .decode(&loaded_cid.data)
                    .map_err(|e| anyhow!("invalid dag json: {:?}", e))?;
                current = root;
            }

            let index: libipld::ipld::IpldIndex = if let Ok(i) = part.parse::<usize>() {
                i.into()
            } else {
                part.clone().into()
            };

            current = current.take(index)?;
        }

        Ok(current)
    }

    #[tracing::instrument(skip(self))]
    async fn resolve_path_to_cid(&self, root: &Path, ctx: &mut LoaderContext) -> Result<Cid> {
        let mut current = root.clone();

        // maximum cursion of ipns lookups
        const MAX_LOOKUPS: usize = 16;

        for _ in 0..MAX_LOOKUPS {
            match current.typ {
                PathType::Ipfs => match current.root {
                    CidOrDomain::Cid(ref c) => {
                        return Ok(*c);
                    }
                    CidOrDomain::Domain(_) => bail!("invalid domain encountered"),
                },
                PathType::Ipns => match current.root {
                    CidOrDomain::Cid(ref c) => {
                        let c = self.load_ipns_record(c).await?;
                        current = Path::from_cid(c);
                    }
                    CidOrDomain::Domain(ref domain) => {
                        let mut records = resolve_dnslink(domain).await?;
                        if records.is_empty() {
                            bail!("no valid dnslink records found for {}", domain);
                        }
                        current = records.remove(0);
                    }
                },
            }
        }

        bail!("cannot resolve {}, too many recursive lookups", root);
    }

    #[tracing::instrument(skip(self))]
    async fn resolve_root(&self, root: &Path, ctx: &mut LoaderContext) -> Result<(Cid, LoadedCid)> {
        let cid = self.resolve_path_to_cid(root, ctx).await?;
        let loaded_cid = self.load_cid(&cid, ctx).await?;
        Ok((cid, loaded_cid))
    }

    #[tracing::instrument(skip(self))]
    async fn load_cid(&self, cid: &Cid, ctx: &mut LoaderContext) -> Result<LoadedCid> {
        self.loader.load_cid(cid, ctx).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn has_cid(&self, cid: &Cid) -> Result<bool> {
        self.loader.has_cid(cid).await
    }

    #[tracing::instrument(skip(self))]
    async fn load_ipns_record(&self, cid: &Cid) -> Result<Cid> {
        todo!()
    }
}

/// Extract links from the given content.
pub fn parse_links(cid: &Cid, bytes: &[u8]) -> Result<Vec<Cid>> {
    let codec = Codec::try_from(cid.codec()).context("unknown codec")?;
    let codec = match codec {
        Codec::DagPb => IpldCodec::DagPb,
        Codec::DagCbor => IpldCodec::DagCbor,
        Codec::DagJson => IpldCodec::DagJson,
        Codec::Raw => IpldCodec::Raw,
        _ => bail!("unsupported codec {:?}", codec),
    };

    let decoded: Ipld = Ipld::decode(codec, &mut std::io::Cursor::new(bytes))?;
    let mut links = Vec::new();
    decoded.references(&mut links);

    Ok(links)
}

#[tracing::instrument]
async fn resolve_dnslink(url: &str) -> Result<Vec<Path>> {
    let url = format!("_dnslink.{}.", url);
    let records = resolve_txt_record(&url).await?;
    let records = records
        .into_iter()
        .filter(|r| r.starts_with("dnslink="))
        .map(|r| {
            let p = r.trim_start_matches("dnslink=").trim();
            p.parse()
        })
        .collect::<Result<_>>()?;
    Ok(records)
}

async fn resolve_txt_record(url: &str) -> Result<Vec<String>> {
    use trust_dns_resolver::config::*;
    use trust_dns_resolver::AsyncResolver;

    // Construct a new Resolver with default configuration options
    let resolver = AsyncResolver::tokio(ResolverConfig::default(), ResolverOpts::default())?;

    let txt_response = resolver.txt_lookup(url).await?;

    let out = txt_response.into_iter().map(|r| r.to_string()).collect();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        hash::BuildHasher,
        sync::Arc,
    };

    use super::*;
    use cid::multihash::{Code, MultihashDigest};
    use futures::{StreamExt, TryStreamExt};
    use libipld::{codec::Encode, Ipld, IpldCodec};
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    #[async_trait]
    impl<S: BuildHasher + Clone + Send + Sync + 'static> ContentLoader for HashMap<Cid, Bytes, S> {
        async fn load_cid(&self, cid: &Cid, _ctx: &LoaderContext) -> Result<LoadedCid> {
            match self.get(cid) {
                Some(b) => Ok(LoadedCid {
                    data: b.clone(),
                    source: Source::Bitswap,
                }),
                None => bail!("not found"),
            }
        }

        async fn stop_session(&self, _ctx: ContextId) -> Result<()> {
            // no session tracking
            Ok(())
        }

        async fn has_cid(&self, cid: &Cid) -> Result<bool> {
            Ok(self.contains_key(cid))
        }
    }

    async fn load_fixture(p: &str) -> Bytes {
        Bytes::from(tokio::fs::read(format!("./fixtures/{p}")).await.unwrap())
    }

    async fn read_to_vec<T: AsyncRead + Unpin>(mut reader: T) -> Vec<u8> {
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.unwrap();
        out
    }
    async fn read_to_string<T: AsyncRead + Unpin>(reader: T) -> String {
        String::from_utf8(read_to_vec(reader).await).unwrap()
    }

    async fn seek_and_clip<T: ContentLoader + Unpin>(
        ctx: LoaderContext,
        node: &UnixfsNode,
        resolver: Resolver<T>,
        range: std::ops::Range<u64>,
    ) -> UnixfsContentReader<T> {
        let mut cr = node
            .clone()
            .into_content_reader(
                ctx,
                resolver.clone(),
                OutMetrics::default(),
                ResponseClip::Clip(range.end as usize),
            )
            .unwrap()
            .unwrap();
        let n = cr
            .seek(tokio::io::SeekFrom::Start(range.start))
            .await
            .unwrap();
        assert_eq!(n, range.start);
        cr
    }

    #[test]
    fn test_paths() {
        let roundtrip_tests = [
            "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
            "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy/bar",
            "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy/bar/baz/foo",
            "/ipns/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
            "/ipns/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy/bar",
            "/ipns/ipfs.io",
        ];

        for test in roundtrip_tests {
            println!("{}", test);
            let p: Path = test.parse().unwrap();
            assert_eq!(p.to_string(), test);
        }

        let valid_tests = [(
            "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
            "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
        )];
        for (test_in, test_out) in valid_tests {
            println!("{}", test_in);
            let p: Path = test_in.parse().unwrap();
            assert_eq!(p.to_string(), test_out);
        }

        let invalid_tests = [
            "/bla/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy",
            "bla",
            "/bla/blub",
            "/ipfs/ipfs.io",
        ];
        for test in invalid_tests {
            println!("{}", test);
            assert!(test.parse::<Path>().is_err());
        }
    }

    #[test]
    fn test_dir_paths() {
        let non_dir_test = "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy";
        let dir_test = "/ipfs/bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy/";
        let non_dir_path: Path = non_dir_test.parse().unwrap();
        let dir_path: Path = dir_test.parse().unwrap();
        assert!(non_dir_path.tail().is_empty());
        assert!(dir_path.tail().len() == 1);
        assert!(dir_path.tail()[0].is_empty());

        assert!(non_dir_path.to_string() == non_dir_test);
        assert!(dir_path.to_string() == dir_test);
        assert!(dir_path.has_trailing_slash());
        assert!(!non_dir_path.has_trailing_slash());
    }

    fn make_ipld() -> Ipld {
        let mut map = BTreeMap::new();
        map.insert("name".to_string(), Ipld::String("Foo".to_string()));
        map.insert("details".to_string(), Ipld::List(vec![Ipld::Integer(1)]));
        map.insert(
            "my-link".to_string(),
            Ipld::Link(
                "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy"
                    .parse()
                    .unwrap(),
            ),
        );

        Ipld::Map(map)
    }

    #[test]
    fn test_verify_hash() {
        for codec in [IpldCodec::DagCbor, IpldCodec::DagJson] {
            let ipld = make_ipld();

            let mut bytes = Vec::new();
            ipld.encode(codec, &mut bytes).unwrap();
            let digest = Code::Blake3_256.digest(&bytes);
            let c = Cid::new_v1(codec.into(), digest);

            assert_eq!(iroh_util::verify_hash(&c, &bytes), Some(true));
        }
    }

    #[test]
    fn test_parse_links() {
        for codec in [IpldCodec::DagCbor, IpldCodec::DagJson] {
            let ipld = make_ipld();

            let mut bytes = Vec::new();
            ipld.encode(codec, &mut bytes).unwrap();
            let digest = Code::Blake3_256.digest(&bytes);
            let c = Cid::new_v1(codec.into(), digest);

            let links = parse_links(&c, &bytes).unwrap();
            assert_eq!(links.len(), 1);
            assert_eq!(
                links[0].to_string(),
                "bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy"
            );
        }
    }

    #[tokio::test]
    async fn test_resolve_ipld() {
        for codec in [IpldCodec::DagCbor, IpldCodec::DagJson] {
            let ipld = make_ipld();

            let mut bytes = Vec::new();
            ipld.encode(codec, &mut bytes).unwrap();
            let digest = Code::Blake3_256.digest(&bytes);
            let c = Cid::new_v1(codec.into(), digest);
            let bytes = Bytes::from(bytes);

            let loader: Arc<HashMap<_, _>> = Arc::new([(c, bytes)].into_iter().collect());
            let resolver = Resolver::new(loader.clone());

            {
                let path = format!("/ipfs/{c}/name");
                let new_ipld = resolver.resolve(path.parse().unwrap()).await.unwrap();
                let m = new_ipld.metadata().clone();

                let out_bytes = read_to_vec(
                    new_ipld
                        .pretty(
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap(),
                )
                .await;
                let out_ipld: Ipld = codec.decode(&out_bytes).unwrap();
                assert_eq!(out_ipld, Ipld::String("Foo".to_string()));

                assert_eq!(m.unixfs_type, None);
                assert_eq!(m.path.to_string(), path);
                match codec {
                    IpldCodec::DagCbor => {
                        assert_eq!(m.typ, OutType::DagCbor);
                    }
                    IpldCodec::DagJson => {
                        assert_eq!(m.typ, OutType::DagJson);
                    }
                    _ => unreachable!(),
                }
                assert_eq!(m.size, Some(out_bytes.len() as u64));
                assert_eq!(m.resolved_path, vec![c]);
            }
            {
                let path = format!("/ipfs/{c}/details/0");
                let new_ipld = resolver.resolve(path.parse().unwrap()).await.unwrap();
                let m = new_ipld.metadata().clone();

                let out_bytes = read_to_vec(
                    new_ipld
                        .pretty(
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap(),
                )
                .await;
                let out_ipld: Ipld = codec.decode(&out_bytes).unwrap();
                assert_eq!(out_ipld, Ipld::Integer(1));

                assert_eq!(m.unixfs_type, None);
                assert_eq!(m.path.to_string(), path);
                match codec {
                    IpldCodec::DagCbor => {
                        assert_eq!(m.typ, OutType::DagCbor);
                    }
                    IpldCodec::DagJson => {
                        assert_eq!(m.typ, OutType::DagJson);
                    }
                    _ => unreachable!(),
                }
                assert_eq!(m.size, Some(out_bytes.len() as u64));
                assert_eq!(m.resolved_path, vec![c]);
            }
        }
    }

    #[tokio::test]
    async fn test_unixfs_basics_cid_v0() {
        // Test content
        // ------------
        // QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL foo/bar/bar.txt
        //   contains: "world"
        // QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN foo/hello.txt
        //   contains: "hello"
        // QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn foo/bar
        // QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go foo

        let bar_txt_cid_str = "QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL";
        let bar_txt_block_bytes = load_fixture(bar_txt_cid_str).await;

        let bar_cid_str = "QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn";
        let bar_block_bytes = load_fixture(bar_cid_str).await;

        let hello_txt_cid_str = "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].cid, bar_cid_str.parse().unwrap());
        assert_eq!(links[0].name.unwrap(), "bar");

        assert_eq!(links[1].cid, hello_txt_cid_str.parse().unwrap());
        assert_eq!(links[1].name.unwrap(), "hello.txt");

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
            (bar_cid_str.parse().unwrap(), bar_block_bytes),
            (bar_txt_cid_str.parse().unwrap(), bar_txt_block_bytes),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let path = format!("/ipfs/{root_cid_str}");
            let ipld_foo = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let ls = ipld_foo
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(ls.len(), 2);
            assert_eq!(ls[0].name.as_ref().unwrap(), "bar");
            assert_eq!(ls[1].name.as_ref().unwrap(), "hello.txt");

            let m = ipld_foo.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Dir));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, None);
            assert_eq!(m.resolved_path, vec![root_cid_str.parse().unwrap()]);
        }

        {
            let path = format!("/ipfs/{root_cid_str}/hello.txt");
            let ipld_hello_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            assert!(ipld_hello_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .is_none());

            let m = ipld_hello_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(6));
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    hello_txt_cid_str.parse().unwrap(),
                ]
            );

            if let OutContent::Unixfs(node) = ipld_hello_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_hello_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "hello\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_hello_txt);
            }
        }

        {
            let path = format!("/ipfs/{hello_txt_cid_str}");
            let ipld_hello_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            assert!(ipld_hello_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .is_none());

            let m = ipld_hello_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(6));
            assert_eq!(m.resolved_path, vec![hello_txt_cid_str.parse().unwrap()]);

            if let OutContent::Unixfs(node) = ipld_hello_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_hello_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "hello\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_hello_txt);
            }
        }

        {
            let path = format!("/ipfs/{root_cid_str}/bar");
            let ipld_bar = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let ls = ipld_bar
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(ls.len(), 1);
            assert_eq!(ls[0].name.as_ref().unwrap(), "bar.txt");

            let m = ipld_bar.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Dir));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, None);
            assert_eq!(
                m.resolved_path,
                vec![root_cid_str.parse().unwrap(), bar_cid_str.parse().unwrap(),]
            );
        }

        {
            let path = format!("/ipfs/{root_cid_str}/bar/bar.txt");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(6));
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    bar_cid_str.parse().unwrap(),
                    bar_txt_cid_str.parse().unwrap(),
                ]
            );

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "world\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }
    }

    #[tokio::test]
    async fn test_resolver_seeking() {
        // Test content
        // ------------
        // QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN foo/hello.txt
        //   contains: "hello"
        // QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go foo

        let hello_txt_cid_str = "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        let path = format!("/ipfs/{root_cid_str}/hello.txt");
        let ipld_hello_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

        let ctx = ipld_hello_txt.context.clone();
        if let OutContent::Unixfs(node) = ipld_hello_txt.content {
            // clip response
            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 0..2).await;
            assert_eq!(read_to_string(cr).await, "he");

            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 0..5).await;
            assert_eq!(read_to_string(cr).await, "hello");

            // clip to the end
            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 0..6).await;
            assert_eq!(read_to_string(cr).await, "hello\n");

            // clip beyond the end
            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 0..100).await;
            assert_eq!(read_to_string(cr).await, "hello\n");

            // seek
            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 1..100).await;
            assert_eq!(read_to_string(cr).await, "ello\n");

            // seek and clip
            let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 1..3).await;
            assert_eq!(read_to_string(cr).await, "el");
        } else {
            panic!("invalid result: {:?}", ipld_hello_txt);
        }
    }

    #[tokio::test]
    async fn test_resolver_seeking_chunked() {
        // Test content
        // ------------
        // QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9 README.md
        //
        // imported with `go-ipfs add --chunker size-100`

        let pieces_cid_str = [
            "QmccJ8pV5hG7DEbq66ih1ZtowxgvqVS6imt98Ku62J2WRw",
            "QmUajVwSkEp9JvdW914Qh1BCMRSUf2ztiQa6jqy1aWhwJv",
            "QmNyLad1dWGS6mv2zno4iEviBSYSUR2SrQ8JoZNDz1UHYy",
            "QmcXoBdCgmFMoNbASaQCNVswRuuuqbw4VvA7e5GtHbhRNp",
            "QmP9yKRwuji5i7RTgrevwJwXp7uqQu1prv88nxq9uj99rW",
        ];

        // read root
        let root_cid_str = "QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 5);

        let mut loader: HashMap<Cid, Bytes> =
            [(root_cid, root_block_bytes.clone())].into_iter().collect();

        for c in &pieces_cid_str {
            let bytes = load_fixture(c).await;
            loader.insert(c.parse().unwrap(), bytes);
        }

        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let path = format!("/ipfs/{root_cid_str}");
            let ipld_readme = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_readme.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(426));
            assert_eq!(m.resolved_path, vec![root_cid_str.parse().unwrap(),]);

            let size = m.size.unwrap();

            let ctx = ipld_readme.context.clone();
            if let OutContent::Unixfs(node) = ipld_readme.content {
                let cr = seek_and_clip(ctx.clone(), &node, resolver.clone(), 1..size - 1).await;
                let content = read_to_string(cr).await;
                assert_eq!(content.len(), (size - 2) as usize);
                assert!(content.starts_with(" iroh")); // without seeking '# iroh'
                assert!(content.ends_with("</sub>\n")); // without clipping '</sub>\n\n'

                let cr = seek_and_clip(ctx, &node, resolver.clone(), 101..size - 101).await;
                let content = read_to_string(cr).await;
                assert_eq!(content.len(), (size - 202) as usize);
                assert!(content.starts_with("2.0</a>"));
                assert!(content.ends_with("the Apac"));
            } else {
                panic!("invalid result: {:?}", ipld_readme);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_recursive_unixfs_basics_cid_v0() {
        // Test content
        // ------------
        // QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL foo/bar/bar.txt
        //   contains: "world"
        // QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN foo/hello.txt
        //   contains: "hello"
        // QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn foo/bar
        // QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go foo

        let bar_txt_cid_str = "QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL";
        let bar_txt_block_bytes = load_fixture(bar_txt_cid_str).await;

        let bar_cid_str = "QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn";
        let bar_block_bytes = load_fixture(bar_cid_str).await;

        let hello_txt_cid_str = "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].cid, bar_cid_str.parse().unwrap());
        assert_eq!(links[0].name.unwrap(), "bar");

        assert_eq!(links[1].cid, hello_txt_cid_str.parse().unwrap());
        assert_eq!(links[1].name.unwrap(), "hello.txt");

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
            (bar_cid_str.parse().unwrap(), bar_block_bytes),
            (bar_txt_cid_str.parse().unwrap(), bar_txt_block_bytes),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        let path = format!("/ipfs/{root_cid_str}");
        let results: Vec<_> = resolver
            .resolve_recursive(path.parse().unwrap())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(results.len(), 4);

        for result in &results {
            assert_eq!(result.typ(), OutType::Unixfs);
        }

        assert_eq!(
            results[0].metadata().path.to_string(),
            format!("/ipfs/{root_cid_str}")
        );
        assert_eq!(
            results[1].metadata().path.to_string(),
            format!("/ipfs/{bar_cid_str}")
        );
        assert_eq!(
            results[2].metadata().path.to_string(),
            format!("/ipfs/{hello_txt_cid_str}")
        );
        assert_eq!(
            results[3].metadata().path.to_string(),
            format!("/ipfs/{bar_txt_cid_str}")
        );
    }

    #[tokio::test]
    async fn test_unixfs_basics_cid_v1() {
        // uses raw leaves

        // Test content
        // ------------
        // bafkreihcldjer7njjrrxknqh67cestxa7s7jf4nhnp62y6k4twcbahvtc4 foo/bar/bar.txt
        //   contains: "world"
        // bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am foo/hello.txt
        //   contains: "hello"
        // bafybeihmgpuwcdrfi47gfxisll7kmurvi6kd7rht5hlq2ed5omxobfip3a foo/bar
        // bafybeietod5kx72jgbngoontthoax6nva4edkjnieghwqfzenstg4gil5i foo

        let bar_txt_cid_str = "bafkreihcldjer7njjrrxknqh67cestxa7s7jf4nhnp62y6k4twcbahvtc4";
        let bar_txt_block_bytes = load_fixture(bar_txt_cid_str).await;

        let bar_cid_str = "bafybeihmgpuwcdrfi47gfxisll7kmurvi6kd7rht5hlq2ed5omxobfip3a";
        let bar_block_bytes = load_fixture(bar_cid_str).await;

        let hello_txt_cid_str = "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "bafybeietod5kx72jgbngoontthoax6nva4edkjnieghwqfzenstg4gil5i";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].cid, bar_cid_str.parse().unwrap());
        assert_eq!(links[0].name.unwrap(), "bar");

        assert_eq!(links[1].cid, hello_txt_cid_str.parse().unwrap());
        assert_eq!(links[1].name.unwrap(), "hello.txt");

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
            (bar_cid_str.parse().unwrap(), bar_block_bytes),
            (bar_txt_cid_str.parse().unwrap(), bar_txt_block_bytes),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let ipld_foo = resolver
                .resolve(root_cid_str.parse().unwrap())
                .await
                .unwrap();

            let ls = ipld_foo
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(ls.len(), 2);
            assert_eq!(ls[0].name.as_ref().unwrap(), "bar");
            assert_eq!(ls[1].name.as_ref().unwrap(), "hello.txt");
        }

        {
            let ipld_hello_txt = resolver
                .resolve(format!("{root_cid_str}/hello.txt").parse().unwrap())
                .await
                .unwrap();

            if let OutContent::Unixfs(node) = ipld_hello_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_hello_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "hello\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_hello_txt);
            }
        }

        {
            let ipld_bar = resolver
                .resolve(format!("{root_cid_str}/bar").parse().unwrap())
                .await
                .unwrap();

            let ls = ipld_bar
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(ls.len(), 1);
            assert_eq!(ls[0].name.as_ref().unwrap(), "bar.txt");
        }

        {
            let ipld_bar_txt = resolver
                .resolve(format!("{root_cid_str}/bar/bar.txt").parse().unwrap())
                .await
                .unwrap();

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "world\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }
    }

    #[tokio::test]
    async fn test_unixfs_split_file_regular() {
        // Test content
        // ------------
        // QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9 README.md
        //
        // imported with `go-ipfs add --chunker size-100`

        let pieces_cid_str = [
            "QmccJ8pV5hG7DEbq66ih1ZtowxgvqVS6imt98Ku62J2WRw",
            "QmUajVwSkEp9JvdW914Qh1BCMRSUf2ztiQa6jqy1aWhwJv",
            "QmNyLad1dWGS6mv2zno4iEviBSYSUR2SrQ8JoZNDz1UHYy",
            "QmcXoBdCgmFMoNbASaQCNVswRuuuqbw4VvA7e5GtHbhRNp",
            "QmP9yKRwuji5i7RTgrevwJwXp7uqQu1prv88nxq9uj99rW",
        ];

        // read root
        let root_cid_str = "QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 5);

        let mut loader: HashMap<Cid, Bytes> =
            [(root_cid, root_block_bytes.clone())].into_iter().collect();

        for c in &pieces_cid_str {
            let bytes = load_fixture(c).await;
            loader.insert(c.parse().unwrap(), bytes);
        }

        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let path = format!("/ipfs/{root_cid_str}");
            let ipld_readme = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_readme.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(426));
            assert_eq!(m.resolved_path, vec![root_cid_str.parse().unwrap(),]);

            if let OutContent::Unixfs(node) = ipld_readme.content {
                let content = read_to_string(
                    node.into_content_reader(
                        ipld_readme.context,
                        resolver.clone(),
                        OutMetrics::default(),
                        ResponseClip::NoClip,
                    )
                    .unwrap()
                    .unwrap(),
                )
                .await;
                print!("{}", content);
                assert_eq!(content.len(), 426);
                assert!(content.starts_with("# iroh"));
                assert!(content.ends_with("</sub>\n\n"));
            } else {
                panic!("invalid result: {:?}", ipld_readme);
            }
        }
    }

    #[tokio::test]
    async fn test_unixfs_split_file_recursive() {
        // Test content
        // ------------
        // QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9 README.md
        //
        // imported with `go-ipfs add --chunker size-100`

        let pieces_cid_str = [
            "QmccJ8pV5hG7DEbq66ih1ZtowxgvqVS6imt98Ku62J2WRw",
            "QmUajVwSkEp9JvdW914Qh1BCMRSUf2ztiQa6jqy1aWhwJv",
            "QmNyLad1dWGS6mv2zno4iEviBSYSUR2SrQ8JoZNDz1UHYy",
            "QmcXoBdCgmFMoNbASaQCNVswRuuuqbw4VvA7e5GtHbhRNp",
            "QmP9yKRwuji5i7RTgrevwJwXp7uqQu1prv88nxq9uj99rW",
        ];

        // read root
        let root_cid_str = "QmUr9cs4mhWxabKqm9PYPSQQ6AQGbHJBtyrNmxtKgxqUx9";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 5);

        let mut loader: HashMap<Cid, Bytes> =
            [(root_cid, root_block_bytes.clone())].into_iter().collect();

        for c in &pieces_cid_str {
            let bytes = load_fixture(c).await;
            loader.insert(c.parse().unwrap(), bytes);
        }

        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let path = format!("/ipfs/{root_cid_str}");
            let parts: Vec<_> = resolver
                .resolve_recursive(path.parse().unwrap())
                .try_collect()
                .await
                .unwrap();
            assert_eq!(parts.len(), 6);
            assert_eq!(parts[0].metadata().unixfs_type.unwrap(), UnixfsType::File);
            assert_eq!(parts[0].metadata().path, Path::from_cid(root_cid));
            assert_eq!(parts[1].metadata().path, pieces_cid_str[0].parse().unwrap());
            assert_eq!(parts[2].metadata().path, pieces_cid_str[1].parse().unwrap());
            assert_eq!(parts[3].metadata().path, pieces_cid_str[2].parse().unwrap());
            assert_eq!(parts[4].metadata().path, pieces_cid_str[3].parse().unwrap());
            assert_eq!(parts[5].metadata().path, pieces_cid_str[4].parse().unwrap());
        }
    }

    #[tokio::test]
    async fn test_unixfs_symlink() {
        // Test content
        // ------------
        // QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL foo/bar/bar.txt
        //   contains: "world"
        // QmTh6zphkkZXhLimR5hfy1QnWrzf6EwP15r5aQqSzhUCYz foo/bar/my-symlink-local.txt
        //   contains: ./bar.txt
        // QmZSCBhytmu1Mr5gVrsXsB6D8S2XMQXSoofHdPxtPGrZBj foo/bar/my-symlink-outer.txt
        //   contains: ../../hello.txt (out of bounds)
        // QmRZQMR6cpczdJAF4xXtisda3DbvFrHxuwi5nF2NJKZvzC foo/bar/my-symlink.txt
        //   contains: ../hello.txt
        // QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN foo/hello.txt
        // QmT7qkMZnZNDACJ8CT4PnVkxXKJfcKNVggkygzRcvZE72B foo/bar
        // QmfTVUNatSpmZUERu62hwSEuLHEUNuY8FFuzFL5n187yGq foo

        let bar_txt_cid_str = "QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL";
        let bar_txt_block_bytes = load_fixture(bar_txt_cid_str).await;

        let my_symlink_local_cid_str = "QmTh6zphkkZXhLimR5hfy1QnWrzf6EwP15r5aQqSzhUCYz";
        let my_symlink_local_block_bytes = load_fixture(my_symlink_local_cid_str).await;

        let my_symlink_cid_str = "QmRZQMR6cpczdJAF4xXtisda3DbvFrHxuwi5nF2NJKZvzC";
        let my_symlink_block_bytes = load_fixture(my_symlink_cid_str).await;

        let my_symlink_outer_cid_str = "QmZSCBhytmu1Mr5gVrsXsB6D8S2XMQXSoofHdPxtPGrZBj";
        let my_symlink_outer_block_bytes = load_fixture(my_symlink_outer_cid_str).await;

        let bar_cid_str = "QmT7qkMZnZNDACJ8CT4PnVkxXKJfcKNVggkygzRcvZE72B";
        let bar_block_bytes = load_fixture(bar_cid_str).await;

        let hello_txt_cid_str = "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "QmfTVUNatSpmZUERu62hwSEuLHEUNuY8FFuzFL5n187yGq";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].cid, bar_cid_str.parse().unwrap());
        assert_eq!(links[0].name.unwrap(), "bar");

        assert_eq!(links[1].cid, hello_txt_cid_str.parse().unwrap());
        assert_eq!(links[1].name.unwrap(), "hello.txt");

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
            (bar_cid_str.parse().unwrap(), bar_block_bytes),
            (bar_txt_cid_str.parse().unwrap(), bar_txt_block_bytes),
            (my_symlink_cid_str.parse().unwrap(), my_symlink_block_bytes),
            (
                my_symlink_local_cid_str.parse().unwrap(),
                my_symlink_local_block_bytes,
            ),
            (
                my_symlink_outer_cid_str.parse().unwrap(),
                my_symlink_outer_block_bytes,
            ),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        {
            let path = format!("/ipfs/{root_cid_str}/hello.txt");
            let ipld_hello_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            assert!(ipld_hello_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .is_none());

            let m = ipld_hello_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(6));
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    hello_txt_cid_str.parse().unwrap()
                ]
            );

            if let OutContent::Unixfs(node) = ipld_hello_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_hello_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "hello\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_hello_txt);
            }
        }

        {
            let path = format!("/ipfs/{root_cid_str}/bar");
            let ipld_bar = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let ls = ipld_bar
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(ls.len(), 4);
            assert_eq!(ls[0].name.as_ref().unwrap(), "bar.txt");
            assert_eq!(ls[1].name.as_ref().unwrap(), "my-symlink-local.txt");
            assert_eq!(ls[2].name.as_ref().unwrap(), "my-symlink-outer.txt");
            assert_eq!(ls[3].name.as_ref().unwrap(), "my-symlink.txt");
        }

        // regular file
        {
            let path = format!("/ipfs/{root_cid_str}/bar/bar.txt");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.size, Some(6));
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    bar_cid_str.parse().unwrap(),
                    bar_txt_cid_str.parse().unwrap()
                ]
            );

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "world\n"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }

        // symlink local file
        {
            let path = format!("/ipfs/{root_cid_str}/bar/my-symlink-local.txt");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Symlink));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    bar_cid_str.parse().unwrap(),
                    my_symlink_local_cid_str.parse().unwrap()
                ]
            );

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "./bar.txt"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }

        // symlink outside
        {
            let path = format!("/ipfs/{root_cid_str}/bar/my-symlink-outer.txt");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Symlink));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    bar_cid_str.parse().unwrap(),
                    my_symlink_outer_cid_str.parse().unwrap()
                ]
            );

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip,
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "../../hello.txt"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }

        // symlink file
        {
            let path = format!("/ipfs/{root_cid_str}/bar/my-symlink.txt");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Symlink));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(
                m.resolved_path,
                vec![
                    root_cid_str.parse().unwrap(),
                    bar_cid_str.parse().unwrap(),
                    my_symlink_cid_str.parse().unwrap()
                ]
            );

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "../hello.txt"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }

            let path = format!("/ipfs/{my_symlink_cid_str}");
            let ipld_bar_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let m = ipld_bar_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::Symlink));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert_eq!(m.resolved_path, vec![my_symlink_cid_str.parse().unwrap()]);

            if let OutContent::Unixfs(node) = ipld_bar_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_bar_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "../hello.txt"
                );
            } else {
                panic!("invalid result: {:?}", ipld_bar_txt);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_txt_record() {
        let result = resolve_txt_record("_dnslink.ipfs.io.").await.unwrap();
        assert!(!result.is_empty());
        assert_eq!(result[0], "dnslink=/ipns/website.ipfs.io");

        let result = resolve_txt_record("_dnslink.website.ipfs.io.")
            .await
            .unwrap();
        assert!(!result.is_empty());
        assert!(&result[0].starts_with("dnslink=/ipfs"));
    }

    #[tokio::test]
    async fn test_resolve_dnslink() {
        let result = resolve_dnslink("ipfs.io").await.unwrap();
        assert!(!result.is_empty());
        assert_eq!(result[0], "/ipns/website.ipfs.io".parse().unwrap());

        let result = resolve_dnslink("website.ipfs.io").await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].typ(), PathType::Ipfs);
    }

    #[tokio::test]
    async fn test_unixfs_hamt_dir() {
        // Test content
        // ------------
        // for n in $(seq 10000); do echo $n > foo/$n.txt; done
        // ipfs add --recursive foo
        //
        // QmUu8pzQ5yjhDrg4GiHYLeko2oT76vcmYX5bw6sjiEJ82k foo
        // QmWKbcq9HGfat7FsL85qrwNUxnmo3xAWzUo2nEj9BoAZeP foo/9999.txt

        let root_cid_str = "QmUu8pzQ5yjhDrg4GiHYLeko2oT76vcmYX5bw6sjiEJ82k";

        let reader = tokio::io::BufReader::new(
            tokio::fs::File::open("./fixtures/big-foo.car")
                .await
                .unwrap(),
        );
        let car_reader = iroh_car::CarReader::new(reader).await.unwrap();
        let files: HashMap<Cid, Bytes> = car_reader
            .stream()
            .map(|r| r.map(|(k, v)| (k, Bytes::from(v))))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(files.len(), 10938);

        let loader = Arc::new(files);
        let resolver = Resolver::new(loader.clone());

        // foo/bar/bar.txt
        {
            let path = format!("/ipfs/{root_cid_str}/bar/bar.txt");
            let ipld_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            assert!(ipld_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .is_none());

            let m = ipld_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert!(m.size.unwrap() > 0);

            if let OutContent::Unixfs(node) = ipld_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    "world\n",
                );
            } else {
                panic!("invalid result: {:?}", ipld_txt);
            }
        }
        // read the directory listing
        {
            let path = format!("/ipfs/{root_cid_str}");
            let ipld_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            let mut links = ipld_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .expect("missing listing")
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            // these are not sorted by name originally
            links.sort_by(|a, b| {
                let a = a.name.as_ref().unwrap();
                let b = b.name.as_ref().unwrap();

                match (
                    a.replace(".txt", "").parse::<usize>(),
                    b.replace(".txt", "").parse::<usize>(),
                ) {
                    (Ok(a), Ok(b)) => a.cmp(&b),
                    _ => a.cmp(b),
                }
            });

            for (i, link) in links.iter().take(10000).enumerate() {
                assert_eq!(link.name, Some(format!("{}.txt", i + 1)));
            }

            assert_eq!(links[10000].name, Some("bar".into()));
            assert_eq!(links[10001].name, Some("hello.txt".into()));

            assert_eq!(links.len(), 10_000 + 2);
        }

        for i in 1..=10000 {
            let path = format!("/ipfs/{root_cid_str}/{}.txt", i);
            let ipld_txt = resolver.resolve(path.parse().unwrap()).await.unwrap();

            assert!(ipld_txt
                .unixfs_read_dir(&resolver, OutMetrics::default())
                .unwrap()
                .is_none());

            let m = ipld_txt.metadata();
            assert_eq!(m.unixfs_type, Some(UnixfsType::File));
            assert_eq!(m.path.to_string(), path);
            assert_eq!(m.typ, OutType::Unixfs);
            assert!(m.size.unwrap() > 0);

            if let OutContent::Unixfs(node) = ipld_txt.content {
                assert_eq!(
                    read_to_string(
                        node.into_content_reader(
                            ipld_txt.context,
                            resolver.clone(),
                            OutMetrics::default(),
                            ResponseClip::NoClip
                        )
                        .unwrap()
                        .unwrap()
                    )
                    .await,
                    format!("{}\n", i),
                );
            } else {
                panic!("invalid result: {:?}", ipld_txt);
            }
        }
    }

    #[tokio::test]
    async fn test_resolve_recursive_with_path() {
        // Test content
        // ------------
        // QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL foo/bar/bar.txt
        //   contains: "world"
        // QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN foo/hello.txt
        //   contains: "hello"
        // QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn foo/bar
        // QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go foo

        let bar_txt_cid_str = "QmaRGe7bVmVaLmxbrMiVNXqW4pRNNp3xq7hFtyRKA3mtJL";
        let bar_txt_block_bytes = load_fixture(bar_txt_cid_str).await;

        let bar_cid_str = "QmcHTZfwWWYG2Gbv9wR6bWZBvAgpFV5BcDoLrC2XMCkggn";
        let bar_block_bytes = load_fixture(bar_cid_str).await;

        let hello_txt_cid_str = "QmZULkCELmmk5XNfCgTnCyFgAVxBRBXyDHGGMVoLFLiXEN";
        let hello_txt_block_bytes = load_fixture(hello_txt_cid_str).await;

        // read root
        let root_cid_str = "QmdkGfDx42RNdAZFALHn5hjHqUq7L9o6Ef4zLnFEu3Y4Go";
        let root_cid: Cid = root_cid_str.parse().unwrap();
        let root_block_bytes = load_fixture(root_cid_str).await;
        let root_block = UnixfsNode::decode(&root_cid, root_block_bytes.clone()).unwrap();

        let links: Vec<_> = root_block.links().collect::<Result<_>>().unwrap();
        assert_eq!(links.len(), 2);

        assert_eq!(links[0].cid, bar_cid_str.parse().unwrap());
        assert_eq!(links[0].name.unwrap(), "bar");

        assert_eq!(links[1].cid, hello_txt_cid_str.parse().unwrap());
        assert_eq!(links[1].name.unwrap(), "hello.txt");

        let loader: HashMap<Cid, Bytes> = [
            (root_cid, root_block_bytes.clone()),
            (hello_txt_cid_str.parse().unwrap(), hello_txt_block_bytes),
            (bar_cid_str.parse().unwrap(), bar_block_bytes),
            (bar_txt_cid_str.parse().unwrap(), bar_txt_block_bytes),
        ]
        .into_iter()
        .collect();
        let loader = Arc::new(loader);
        let resolver = Resolver::new(loader.clone());

        let path = format!("/ipfs/{root_cid_str}");
        let results: Vec<_> = resolver
            .resolve_recursive_with_paths(path.parse().unwrap())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(results.len(), 4);

        assert_eq!(results[0].0.to_string(), format!("/ipfs/{root_cid_str}"));
        assert_eq!(
            results[1].0.to_string(),
            format!("/ipfs/{root_cid_str}/bar")
        );
        assert_eq!(
            results[2].0.to_string(),
            format!("/ipfs/{root_cid_str}/hello.txt")
        );
        assert_eq!(
            results[3].0.to_string(),
            format!("/ipfs/{root_cid_str}/bar/bar.txt")
        );
    }
}
