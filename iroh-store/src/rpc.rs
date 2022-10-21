use std::{io::Cursor};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::{Bytes};
use cid::Cid;
use futures::{Future, future::{LocalBoxFuture}, FutureExt, stream::FuturesUnordered, StreamExt};
use iroh_rpc_types::store::{
    GetLinksRequest, GetLinksResponse, GetRequest, GetResponse, GetSizeRequest, GetSizeResponse,
    HasRequest, HasResponse, PutRequest, Store as RpcStore, StoreServerAddr, VersionResponse,
};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::store::Store;

#[cfg(feature = "rpc-grpc")]
impl iroh_rpc_types::NamedService for Store {
    const NAME: &'static str = "store";
}

pub struct LocalStore {
    store: Store,
    receiver: mpsc::Receiver<Box<dyn FnOnce(&Store) -> LocalBoxFuture<()> + Send>>
}

impl LocalStore {
    pub fn new(store: Store) -> (SendStore, LocalStore) {
        let (sender, receiver) = mpsc::channel(1);
        (SendStore { sender }, LocalStore { store, receiver })
    }
    pub async fn run(mut self) {
        let mut t = FuturesUnordered::new();
        loop {
            tokio::select! {
                Some(f) = self.receiver.recv() => {
                    t.push(f(&self.store));
                }
                _ = t.next() => {}
            }
        }
    }
}

pub struct SendStore {
    sender: mpsc::Sender<Box<dyn FnOnce(&Store) -> LocalBoxFuture<()> + Send>>
}

impl SendStore {
    async fn fut<F, Fut>(&self, f: F) -> Fut::Output
        where
            Fut: Future,
            F: FnOnce(&Store) -> Fut + Send + 'static,
            Fut::Output: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<Fut::Output>();
        self.sender.send(Box::new(move |store| {
            async move {
                tx.send(f(store).await).ok();
            }.boxed_local()
        })).await.ok();
        rx.map(|x| x.unwrap()).await
    }
}

#[async_trait]
impl RpcStore for SendStore {

    #[tracing::instrument(skip(self))]
    async fn version(&self, _: ()) -> Result<VersionResponse> {
        let version = env!("CARGO_PKG_VERSION").to_string();
        Ok(VersionResponse { version })
    }

    #[tracing::instrument(skip(self, req))]
    async fn put(&self, req: PutRequest) -> Result<()> {
        self.fut(move |s| {
            let s = s.clone();
            async move {
                let cid = cid_from_bytes(req.cid)?;
                let links = links_from_bytes(req.links)?;
                println!("put {} {:?}", cid, links);
                let res = s.put(cid, req.blob, links).await?;

                info!("store rpc call: put cid {}", cid);
                Ok(())
            }
        }).await
    }

    #[tracing::instrument(skip(self))]
    async fn get(&self, req: GetRequest) -> Result<GetResponse> {
        self.fut(move |s| {
            let s = s.clone();
            async move {
                let cid = cid_from_bytes(req.cid)?;
                let data = s.get(&cid).await?.map(|x| Bytes::from(x.to_vec()));
                Ok(GetResponse { data })
            }
        }).await
    }

    #[tracing::instrument(skip(self))]
    async fn has(&self, req: HasRequest) -> Result<HasResponse> {
        self.fut(move |s| {
            let s = s.clone();
            async move {
                let cid = cid_from_bytes(req.cid)?;
                let has = s.has(&cid).await?;
                Ok(HasResponse { has })
            }
        }).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_links(&self, req: GetLinksRequest) -> Result<GetLinksResponse> {
        self.fut(move |s| {
            let s = s.clone();
            async move {
                let cid = cid_from_bytes(req.cid)?;
                let links = s.get_links(&cid).await?;
                let links = links.unwrap_or_default().into_iter().map(|cid| cid.to_bytes()).collect();
                Ok(GetLinksResponse { links })
            }
        }).await
    }

    #[tracing::instrument(skip(self))]
    async fn get_size(&self, req: GetSizeRequest) -> Result<GetSizeResponse> {
        self.fut(move |s| {
            let s = s.clone();
            async move {
                let cid = cid_from_bytes(req.cid)?;
                let size = s.get_size(&cid).await?.map(|x| x as u64);
                Ok(GetSizeResponse { size })
            }
        }).await
    }
}

#[tracing::instrument(skip(store))]
pub async fn new(addr: StoreServerAddr, store: Store) -> Result<()> {
    info!("rpc listening on: {}", addr);
    let (rpc, local) = LocalStore::new(store);
    std::thread::spawn(|| {
        futures::executor::block_on(local.run());
    });
    iroh_rpc_types::store::serve(addr, rpc).await
}

#[tracing::instrument]
fn cid_from_bytes(b: Vec<u8>) -> Result<Cid> {
    Cid::read_bytes(Cursor::new(b)).context("invalid cid")
}

#[tracing::instrument]
fn links_from_bytes(l: Vec<Vec<u8>>) -> Result<Vec<Cid>> {
    l.into_iter().map(cid_from_bytes).collect()
}
