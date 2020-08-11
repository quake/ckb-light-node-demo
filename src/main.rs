use ckb_app_config::NetworkConfig;
use ckb_light_node_demo::protocols::{ChainStore, FilterProtocol, SyncProtocol};
use ckb_light_node_demo::service::RpcService;
use ckb_light_node_demo::store::{RocksdbStore, Store};
use ckb_logger::info;
use ckb_network::{
    BlockingFlag, CKBProtocol, DefaultExitHandler, NetworkService, NetworkState, SupportProtocols, ExitHandler,
};
use ckb_types::{prelude::*, H256};
use clap::{App, Arg};
use crossbeam_channel::unbounded;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    pub logger: ckb_logger_service::Config,
    pub network: NetworkConfig,
}

fn main() {
    let matches = App::new("ckb light node demo")
        .arg(
            Arg::with_name("listen_uri")
                .short("l")
                .help("Light node rpc http service listen address, default 127.0.0.1:8121")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dir")
                .short("d")
                .help("Sets the working dir to use")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let mut path = PathBuf::new();
    path.push(matches.value_of("dir").expect("required arg"));
    path.push("config.toml");
    let mut config: Config =
        toml::from_slice(&std::fs::read(path.clone()).expect("load config file"))
            .expect("deserialize config file");
    path.pop();
    path.push("run.log");
    config.logger.file = path.clone();
    let _logger_guard = ckb_logger_service::init(config.logger).unwrap();
    path.pop();
    path.push("db");
    config.network.path = path;

    let rpc_listen_address = matches
        .value_of("listen_uri")
        .unwrap_or("127.0.0.1:8121")
        .to_owned();
    init(config.network, rpc_listen_address);
}

fn init(config: ckb_app_config::NetworkConfig, rpc_listen_address: String) {
    let rocksdb = Arc::new(RocksdbStore::new(config.path.to_str().unwrap()));
    info!("store statistics: {:?}", rocksdb.statistics().unwrap());
    let store = ChainStore { store: rocksdb };

    let resource = ckb_resource::Resource::bundled(format!("specs/{}.toml", "mainnet"));
    let spec = ckb_chain_spec::ChainSpec::load_from(&resource).expect("load spec by name");
    if store.tip().expect("store should be OK").is_none() {
        let genesis = spec.build_genesis().expect("build genesis");
        store.init(genesis.header()).expect("store should be OK");
    }
    let consensus = spec.build_consensus().expect("build consensus");

    let (sender, receiver) = unbounded();

    let _server = RpcService::new(store.clone(), sender, &rpc_listen_address).start();

    let genesis_hash = store
        .get_block_hash(0)
        .expect("store should be OK")
        .unwrap();

    let network_state =
        Arc::new(NetworkState::from_config(config).expect("Init network state failed"));
    let exit_handler = DefaultExitHandler::default();
    let required_protocol_ids = vec![
        SupportProtocols::Sync.protocol_id(),
        SupportProtocols::BloomFilter.protocol_id(),
    ];

    let mut blocking_recv_flag = BlockingFlag::default();
    blocking_recv_flag.disable_connected();
    blocking_recv_flag.disable_disconnected();
    blocking_recv_flag.disable_notify();

    let sync_protocol = Box::new(SyncProtocol::new(store.clone(), consensus.clone()));
    let filter_protocol = Box::new(FilterProtocol::new(store, consensus, receiver));

    let protocols = vec![
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::Sync,
            sync_protocol,
            Arc::clone(&network_state),
        ),
        CKBProtocol::new_with_support_protocol(
            SupportProtocols::BloomFilter,
            filter_protocol,
            Arc::clone(&network_state),
        ),
    ];

    let _network_controller = NetworkService::new(
        Arc::clone(&network_state),
        protocols,
        required_protocol_ids,
        format!(
            "/ckb/{}",
            &format!("{:x}", Unpack::<H256>::unpack(&genesis_hash))[..8]
        ),
        "ckb-light-node-demo".to_string(),
        exit_handler.clone(),
    )
    .start(Some("NetworkService"))
    .expect("Start network service failed");

    let exit_handler_clone = exit_handler.clone();
    ctrlc::set_handler(move || {
        exit_handler_clone.notify_exit();
    })
    .expect("Error setting Ctrl-C handler");
    exit_handler.wait_for_exit();
}
