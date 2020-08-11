use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier};
use crate::store::Store;
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info, warn};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{
    packed::{self, Byte32},
    prelude::*,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const SEND_GET_HEADERS_TOKEN: u64 = 0;
const MAX_HEADERS_LEN: usize = 2_000;

pub struct SyncProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    // record last `GetHeaders` time
    peers: HashMap<PeerIndex, Instant>,
}

impl<S> SyncProtocol<S> {
    pub fn new(store: ChainStore<S>, consensus: Consensus) -> Self {
        Self {
            store,
            consensus,
            peers: HashMap::default(),
        }
    }
}

impl<S: Store + Send + Sync> SyncProtocol<S> {
    fn send_get_headers(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        let locator_hashes = self.store.get_locator().expect("store should be OK");
        let message = packed::SyncMessage::new_builder()
            .set(
                packed::GetHeaders::new_builder()
                    .block_locator_hashes(locator_hashes.pack())
                    .hash_stop(Byte32::zero())
                    .build(),
            )
            .build();
        if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
            debug!("SyncProtocol send GetHeaders error: {:?}", err);
        }
        self.peers.insert(peer, Instant::now());
    }
}

impl<S: Store + Send + Sync> CKBProtocolHandler for SyncProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_secs(1), SEND_GET_HEADERS_TOKEN)
            .expect("set_notify should be ok");
    }

    fn connected(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        _version: &str,
    ) {
        self.send_get_headers(nc, peer);
    }

    fn received(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex, data: Bytes) {
        let message = match packed::SyncMessage::from_slice(&data) {
            Ok(msg) => msg.to_enum(),
            _ => {
                info!("Peer {} sends us a malformed sync message", peer);
                nc.ban_peer(
                    peer,
                    BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed sync message"),
                );
                return;
            }
        };

        match message.as_reader() {
            packed::SyncMessageUnionReader::SendHeaders(reader) => {
                let headers = reader
                    .headers()
                    .to_entity()
                    .into_iter()
                    .map(packed::Header::into_view)
                    .collect::<Vec<_>>();

                let len = headers.len();
                info!("received SendHeaders from peer: {}, len: {}", peer, len);
                if len > 0 {
                    let header_provider = HeaderProviderWrapper { store: &self.store };
                    let header_verifier = HeaderVerifier::new(&self.consensus, &header_provider);
                    for header in headers {
                        match header_verifier.verify(&header) {
                            Ok(_) => self
                                .store
                                .insert_header(header)
                                .expect("store should be OK"),
                            Err(err) => {
                                warn!("Peer {} sends us an invalid header: {:?}", peer, err);
                                nc.ban_peer(
                                    peer,
                                    BAD_MESSAGE_BAN_TIME,
                                    String::from("send us an invalid header"),
                                );
                                return;
                            }
                        }
                    }

                    let header = self
                        .store
                        .tip()
                        .expect("store should be OK")
                        .expect("tip stored");
                    info!("new tip {:?}, {:?}", header.number(), header.hash());

                    if len == MAX_HEADERS_LEN {
                        self.send_get_headers(nc, peer);
                    }
                }
            }
            _ => {
                let msg = packed::SyncMessage::new_builder()
                    .set(packed::InIBD::new_builder().build())
                    .build();
                if let Err(err) = nc.send_message_to(peer, msg.as_bytes()) {
                    debug!("SyncProtocol send InIBD message error: {:?}", err);
                }
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        self.peers.remove(&peer);
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            SEND_GET_HEADERS_TOKEN => {
                let now = Instant::now();
                let duration = Duration::from_secs(15);
                let peers: Vec<PeerIndex> = self
                    .peers
                    .iter()
                    .filter_map(|(peer, last_send_at)| {
                        if now.duration_since(*last_send_at) > duration {
                            Some(*peer)
                        } else {
                            None
                        }
                    })
                    .collect();

                peers
                    .into_iter()
                    .for_each(|peer| self.send_get_headers(Arc::clone(&nc), peer));
            }
            _ => unreachable!(),
        }
    }
}
