use std::{io::Cursor, time::Instant};

use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use cid::Cid;
use futures::StreamExt;
use iroh_rpc_types::store::{
    GetBlockCidsResponse, GetLinksRequest, GetLinksResponse, GetRequest, GetResponse,
    GetSizeRequest, GetSizeResponse, HasRequest, HasResponse, PutRequest, StatusResponse,
    Store as RpcStore, StoreServerAddr, VersionResponse,
};
use tracing::info;

use crate::store::Store;

struct StoreService {
    store: Store,
    start: Instant,
}

#[cfg(feature = "rpc-grpc")]
impl iroh_rpc_types::NamedService for StoreService {
    const NAME: &'static str = "store";
}

#[async_trait]
impl RpcStore for StoreService {
    #[tracing::instrument(skip(self))]
    async fn version(&self, _: ()) -> Result<VersionResponse> {
        let version = env!("CARGO_PKG_VERSION").to_string();
        Ok(VersionResponse { version })
    }

    #[tracing::instrument(skip(self))]
    async fn status(&self, _: ()) -> Result<StatusResponse> {
        let uptime = self.start.elapsed().as_secs();
        Ok(StatusResponse { uptime })
    }

    #[tracing::instrument(skip(self, req))]
    async fn put(&self, req: PutRequest) -> Result<()> {
        let cid = cid_from_bytes(req.cid)?;
        let links = links_from_bytes(req.links)?;
        let res = self.store.put(cid, req.blob, links).await?;

        info!("store rpc call: put cid {}", cid);
        Ok(res)
    }

    #[tracing::instrument(skip(self))]
    async fn get(&self, req: GetRequest) -> Result<GetResponse> {
        let cid = cid_from_bytes(req.cid)?;
        if let Some(res) = self.store.get(&cid).await? {
            Ok(GetResponse {
                data: Some(BytesMut::from(&res[..]).freeze()),
            })
        } else {
            Ok(GetResponse { data: None })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn has(&self, req: HasRequest) -> Result<HasResponse> {
        let cid = cid_from_bytes(req.cid)?;
        let has = self.store.has(&cid).await?;

        Ok(HasResponse { has })
    }

    #[tracing::instrument(skip(self))]
    async fn get_links(&self, req: GetLinksRequest) -> Result<GetLinksResponse> {
        let cid = cid_from_bytes(req.cid)?;
        if let Some(res) = self.store.get_links(&cid).await? {
            let links = res.into_iter().map(|cid| cid.to_bytes()).collect();
            Ok(GetLinksResponse { links })
        } else {
            Ok(GetLinksResponse { links: Vec::new() })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_size(&self, req: GetSizeRequest) -> Result<GetSizeResponse> {
        let cid = cid_from_bytes(req.cid)?;
        if let Some(size) = self.store.get_size(&cid).await? {
            Ok(GetSizeResponse {
                size: Some(size as u64),
            })
        } else {
            Ok(GetSizeResponse { size: None })
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_block_cids(
        &self,
        _: (),
    ) -> Result<
        std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<GetBlockCidsResponse>> + Send>>,
    > {
        // TODO: avoid having to collect into a vec to make the stream Send
        let iter = self
            .store
            .block_cids()?
            .map(|cid| {
                let cid = cid?;
                Ok(GetBlockCidsResponse {
                    cid: cid.to_bytes(),
                })
            })
            .collect::<Vec<_>>();
        Ok(futures::stream::iter(iter).boxed())
    }
}

#[tracing::instrument(skip(store))]
pub async fn new(addr: StoreServerAddr, store: Store) -> Result<()> {
    info!("rpc listening on: {}", addr);
    let service = StoreService {
        store,
        start: Instant::now(),
    };
    iroh_rpc_types::store::serve(addr, service).await
}

#[tracing::instrument]
fn cid_from_bytes(b: Vec<u8>) -> Result<Cid> {
    Cid::read_bytes(Cursor::new(b)).context("invalid cid")
}

#[tracing::instrument]
fn links_from_bytes(l: Vec<Vec<u8>>) -> Result<Vec<Cid>> {
    l.into_iter().map(cid_from_bytes).collect()
}
