use super::{ChainStore, HeaderProviderWrapper, HeaderVerifier};
use crate::store::Store;
use bloom_filters::{BloomFilter, ClassicBloomFilter, DefaultBuildHashKernels, DefaultBuildHasher};
use ckb_chain_spec::consensus::Consensus;
use ckb_logger::{debug, info};
use ckb_network::{bytes::Bytes, CKBProtocolContext, CKBProtocolHandler, PeerIndex};
use ckb_types::{packed, prelude::*};
use crossbeam_channel::Receiver;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);
const SEND_GET_FILTERED_BLOCKS_TOKEN: u64 = 0;
const CONTROL_RECEIVER_TOKEN: u64 = 1;
const MAX_GET_FILTERED_BLOCKS_LEN: usize = 512;

const FILTER_RAW_DATA_SIZE: usize = 128;
const FILTER_NUM_HASHES: u8 = 10;

pub type Filter = ClassicBloomFilter<DefaultBuildHashKernels<DefaultBuildHasher>>;

pub struct FilterProtocol<S> {
    store: ChainStore<S>,
    consensus: Consensus,
    control_receiver: Receiver<ControlMessage>,
    peer_filter_hash_seed: Option<(PeerIndex, Option<u32>)>,
    pending_get_filtered_blocks: HashSet<packed::Byte32>,
}

impl<S> FilterProtocol<S> {
    pub fn new(
        store: ChainStore<S>,
        consensus: Consensus,
        control_receiver: Receiver<ControlMessage>,
    ) -> Self {
        Self {
            store,
            consensus,
            control_receiver,
            peer_filter_hash_seed: None,
            pending_get_filtered_blocks: HashSet::new(),
        }
    }
}

pub enum ControlMessage {
    AddFilter(packed::Script),
    Start,
    Stop,
}

impl<S: Store + Send + Sync> FilterProtocol<S> {
    fn send_get_filtered_blocks(
        &mut self,
        nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
    ) {
        if self.pending_get_filtered_blocks.is_empty() {
            let block_hashes = self
                .store
                .get_unfiltered_block_hashes(MAX_GET_FILTERED_BLOCKS_LEN)
                .expect("store should be OK");
            let message = packed::FilterMessage::new_builder()
                .set(
                    packed::GetFilteredBlocks::new_builder()
                        .block_hashes(block_hashes.clone().pack())
                        .build(),
                )
                .build();
            if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
                debug!("FilterProtocol send GetFilteredBlocks error: {:?}", err);
            }
            self.pending_get_filtered_blocks = block_hashes.into_iter().collect();
        }
    }
}

impl<S: Store + Send + Sync> CKBProtocolHandler for FilterProtocol<S> {
    fn init(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>) {
        nc.set_notify(Duration::from_millis(10), SEND_GET_FILTERED_BLOCKS_TOKEN)
            .expect("set_notify should be ok");
        nc.set_notify(Duration::from_millis(100), CONTROL_RECEIVER_TOKEN)
            .expect("set_notify should be ok");
    }

    fn notify(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, token: u64) {
        match token {
            SEND_GET_FILTERED_BLOCKS_TOKEN => {
                if let Some((peer, Some(_filter_hash_seed))) = self.peer_filter_hash_seed {
                    self.send_get_filtered_blocks(Arc::clone(&nc), peer);
                }
            }
            CONTROL_RECEIVER_TOKEN => {
                if let Ok(msg) = self.control_receiver.try_recv() {
                    match msg {
                        ControlMessage::AddFilter(script) => {
                            self.store
                                .insert_script(script.clone(), 0)
                                .expect("store should be OK");
                            // send msg to peer to update the filter if service has been started
                            if let Some((peer, Some(filter_hash_seed))) = self.peer_filter_hash_seed
                            {
                                let mut filter = new_filter(filter_hash_seed);
                                filter.insert(&script.calc_script_hash());
                                let message = packed::FilterMessage::new_builder()
                                    .set(
                                        packed::AddFilter::new_builder()
                                            .filter(filter.buckets().raw_data().pack())
                                            .build(),
                                    )
                                    .build();

                                if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
                                    debug!("FilterProtocol send AddFilter error: {:?}", err);
                                }
                            }
                        }
                        ControlMessage::Start => {
                            if let Some((peer, None)) = self.peer_filter_hash_seed {
                                let scripts = self.store.get_scripts().expect("store should be OK");
                                if !scripts.is_empty() {
                                    let seed = rand::random();
                                    let mut filter = new_filter(seed);
                                    for (script, _block_number) in scripts {
                                        filter.insert(&script.calc_script_hash());
                                    }
                                    let message = packed::FilterMessage::new_builder()
                                        .set(
                                            packed::SetFilter::new_builder()
                                                .hash_seed(seed.pack())
                                                .filter(filter.buckets().raw_data().pack())
                                                .num_hashes(FILTER_NUM_HASHES.into())
                                                .build(),
                                        )
                                        .build();

                                    if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
                                        debug!("FilterProtocol send SetFilter error: {:?}", err);
                                    }
                                    self.peer_filter_hash_seed = Some((peer, Some(seed)));
                                }
                            }
                        }
                        ControlMessage::Stop => {
                            if let Some((peer, _)) = self.peer_filter_hash_seed {
                                let message = packed::FilterMessage::new_builder()
                                    .set(packed::ClearFilter::new_builder().build())
                                    .build();
                                if let Err(err) = nc.send_message_to(peer, message.as_bytes()) {
                                    debug!("FilterProtocol send ClearFilter error: {:?}", err);
                                }
                                self.peer_filter_hash_seed = Some((peer, None));
                            }
                        }
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn connected(
        &mut self,
        _nc: Arc<dyn CKBProtocolContext + Sync>,
        peer: PeerIndex,
        _version: &str,
    ) {
        if self.peer_filter_hash_seed.is_none() {
            self.peer_filter_hash_seed = Some((peer, None));
        }
    }

    fn received(&mut self, nc: Arc<dyn CKBProtocolContext + Sync>, peer: PeerIndex, data: Bytes) {
        let message = match packed::FilterMessage::from_slice(&data) {
            Ok(msg) => msg.to_enum(),
            _ => {
                info!("peer {} sends us a malformed filter message", peer);
                nc.ban_peer(
                    peer,
                    BAD_MESSAGE_BAN_TIME,
                    String::from("send us a malformed filter message"),
                );
                return;
            }
        };

        match message.as_reader() {
            packed::FilterMessageUnionReader::FilteredBlocks(reader) => {
                let filtered_blocks = reader.to_entity();
                info!(
                    "received FilteredBlocks from peer: {}, unmatched_block_hashes len: {}, matched_block_hashes len: {}",
                    peer,
                    filtered_blocks.unmatched_block_hashes().len(),
                    filtered_blocks.matched_block_hashes().len()
                );
                for block_hash in filtered_blocks
                    .unmatched_block_hashes()
                    .into_iter()
                    .chain(filtered_blocks.matched_block_hashes())
                {
                    self.pending_get_filtered_blocks.remove(&block_hash);
                }
                self.store
                    .insert_filtered_blocks(filtered_blocks)
                    .expect("store should be OK");
            }
            packed::FilterMessageUnionReader::FilteredBlock(reader) => {
                let header_provider = HeaderProviderWrapper { store: &self.store };
                let header_verifier = HeaderVerifier::new(&self.consensus, &header_provider);
                let filtered_block = reader.to_entity();
                let header = filtered_block.header().into_view();
                if header_verifier.verify(&header).is_ok() {
                    info!(
                        "received FilteredBlock from peer: {}, block number: {}, block hash: {}",
                        peer,
                        header.number(),
                        header.hash()
                    );
                    self.store
                        .append_filtered_block(filtered_block)
                        .expect("store should be OK");
                }
            }
            _ => {
                // ignore
            }
        }
    }

    fn disconnected(&mut self, _nc: Arc<dyn CKBProtocolContext + Sync>, _peer: PeerIndex) {
        self.peer_filter_hash_seed = None;
    }
}

fn new_filter(hash_seed: u32) -> Filter {
    Filter::with_raw_data(
        &[0; FILTER_RAW_DATA_SIZE],
        FILTER_NUM_HASHES as usize,
        DefaultBuildHashKernels::new(hash_seed as usize, DefaultBuildHasher),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_filter() {
        let seed = rand::random();
        let script = packed::Script::default();
        let mut filter1 = new_filter(seed);
        filter1.insert(&script.calc_script_hash());
        assert!(filter1.contains(&script.calc_script_hash()));

        let filter2 = Filter::with_raw_data(
            &filter1.buckets().raw_data(),
            FILTER_NUM_HASHES as usize,
            DefaultBuildHashKernels::new(seed as usize, DefaultBuildHasher),
        );
        assert_eq!(filter1.buckets().raw_data(), filter2.buckets().raw_data());
        assert!(filter2.contains(&script.calc_script_hash()));
    }
}
