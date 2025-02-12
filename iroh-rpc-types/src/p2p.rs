include_proto!("p2p");

proxy!(
    P2p,
    version: () => VersionResponse => VersionResponse,
    shutdown: () => () => (),
    fetch_bitswap: BitswapRequest => BitswapResponse => BitswapResponse,
    fetch_provider_dht: Key =>
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Providers, tonic::Status>> + Send>> =>
        std::pin::Pin<Box<dyn futures::Stream<Item = anyhow::Result<Providers>> + Send>> [FetchProviderDhtStream],
    stop_session_bitswap: StopSessionBitswapRequest => () => (),
    notify_new_blocks_bitswap: NotifyNewBlocksBitswapRequest => () => (),
    get_listening_addrs: () => GetListeningAddrsResponse =>  GetListeningAddrsResponse,
    get_peers: () => GetPeersResponse =>  GetPeersResponse,
    peer_connect: ConnectRequest => ConnectResponse =>  ConnectResponse,
    peer_disconnect: DisconnectRequest => () =>  (),
    gossipsub_add_explicit_peer: GossipsubPeerIdMsg => () =>  (),
    gossipsub_all_mesh_peers: () => GossipsubPeersResponse =>  GossipsubPeersResponse,
    gossipsub_all_peers: () => GossipsubAllPeersResponse =>  GossipsubAllPeersResponse,
    gossipsub_mesh_peers: GossipsubTopicHashMsg => GossipsubPeersResponse =>  GossipsubPeersResponse,
    gossipsub_publish: GossipsubPublishRequest => GossipsubPublishResponse =>  GossipsubPublishResponse,
    gossipsub_remove_explicit_peer: GossipsubPeerIdMsg => () =>  (),
    gossipsub_subscribe: GossipsubTopicHashMsg => GossipsubSubscribeResponse =>  GossipsubSubscribeResponse,
    gossipsub_topics: () => GossipsubTopicsResponse =>  GossipsubTopicsResponse,
    gossipsub_unsubscribe: GossipsubTopicHashMsg => GossipsubSubscribeResponse => GossipsubSubscribeResponse,
    start_providing: Key => () => (),
    stop_providing: Key => () => (),
    local_peer_id: () => PeerIdResponse => PeerIdResponse,
    external_addrs: () => Multiaddrs => Multiaddrs
);
