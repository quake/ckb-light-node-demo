use super::ChainStore;
use crate::store::Store;
use ckb_logger::{debug, info};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{packed, prelude::*};
use std::sync::Arc;
use std::time::Duration;

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const MAX_HEADERS_LEN: usize = 2_000;

pub struct SyncProtocol<S> {
    pub store: ChainStore<S>,
}

impl<S: Store + Send + Sync> SyncProtocol<S> {
    fn send_get_headers(&self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex) {
        let locator_hashes = self.store.get_locator().expect("store should be OK");
        let message = packed::SyncMessage::new_builder()
            .set(
                packed::GetHeaders::new_builder()
                    .block_locator_hashes(locator_hashes.pack())
                    .hash_stop(packed::Byte32::zero())
                    .build(),
            )
            .build();
        if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
            debug!("SyncProtocol send GetHeaders error: {:?}", err);
        }
    }
}

impl<S: Store + Send + Sync> CKBProtocolHandler for SyncProtocol<S> {
    fn init(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>) {}

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

                info!(
                    "received SendHeaders from peer: {}, len: {}",
                    peer,
                    headers.len()
                );
                if headers.len() > 0 {
                    // TODO verify all headers
                    self.store
                        .insert_headers(&headers)
                        .expect("store should be OK");
                    let header = self
                        .store
                        .tip()
                        .expect("store should be OK")
                        .expect("tip stored");
                    info!("new tip {:?}, {:?}", header.number(), header.hash());
                }

                if headers.len() == MAX_HEADERS_LEN {
                    self.send_get_headers(nc, peer);
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

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, _peer: PeerIndex) {}
}
