use ckb_build_info::Version;
use ckb_light_node_demo::protocols::{ChainStore, FilterProtocol, SyncProtocol};
use ckb_light_node_demo::service::Service;
use ckb_light_node_demo::store::{RocksdbStore, Store};
use ckb_network::{
    BlockingFlag, CKBProtocol, NetworkService, NetworkState, MAX_FRAME_LENGTH_FILTER,
    MAX_FRAME_LENGTH_SYNC,
};
use ckb_types::{prelude::*, H256};
use ckb_util::{Condvar, Mutex};
use clap::{App, Arg};
use crossbeam_channel::unbounded;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Config {
    pub logger: ckb_logger::Config,
    pub network: ckb_network::NetworkConfig,
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
    config.logger.file = Some(path.clone());
    let _logger_guard = ckb_logger::init(config.logger).unwrap();
    path.pop();
    path.push("db");
    config.network.path = path;

    let rpc_listen_address = matches
        .value_of("listen_uri")
        .unwrap_or("127.0.0.1:8121")
        .to_owned();
    init(config.network, rpc_listen_address);
}

fn init(config: ckb_network::NetworkConfig, rpc_listen_address: String) {
    let store = ChainStore {
        store: Arc::new(RocksdbStore::new(config.path.to_str().unwrap())),
    };

    if store.tip().expect("store should be OK").is_none() {
        let resource = ckb_resource::Resource::bundled(format!("specs/{}.toml", "mainnet"));
        let spec = ckb_chain_spec::ChainSpec::load_from(&resource).expect("load spec by name");
        let genesis = spec.build_genesis().expect("build genesis");
        store
            .insert_header(genesis.header())
            .expect("store should be OK");
    }

    let (sender, receiver) = unbounded();

    let _server = Service::new(store.clone(), sender, &rpc_listen_address).start();

    let genesis_hash = store
        .get_block_hash(0)
        .expect("store should be OK")
        .unwrap();

    let network_state =
        Arc::new(NetworkState::from_config(config).expect("Init network state failed"));
    let exit_condvar = Arc::new((Mutex::new(()), Condvar::new()));
    let version = get_version();
    let required_protocol_ids = vec![100usize.into(), 200usize.into()];

    let mut blocking_recv_flag = BlockingFlag::default();
    blocking_recv_flag.disable_connected();
    blocking_recv_flag.disable_disconnected();
    blocking_recv_flag.disable_notify();

    let sync_protocol = Box::new(SyncProtocol {
        store: store.clone(),
    });

    let filter_protocol = Box::new(FilterProtocol {
        store,
        control_receiver: receiver,
        peer_filter_hash_seed: None,
        pending_get_filtered_blocks: HashSet::new(),
    });

    // sender.send(ControlMessage::AddFilter());

    let protocols = vec![
        CKBProtocol::new(
            "syn".to_string(),
            100usize.into(),
            &["1".to_string()][..],
            MAX_FRAME_LENGTH_SYNC,
            sync_protocol,
            Arc::clone(&network_state),
            blocking_recv_flag,
        ),
        CKBProtocol::new(
            "fil".to_string(),
            200usize.into(),
            &["1".to_string()][..],
            MAX_FRAME_LENGTH_FILTER,
            filter_protocol,
            Arc::clone(&network_state),
            blocking_recv_flag,
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
        Arc::<(Mutex<()>, Condvar)>::clone(&exit_condvar),
    )
    .start(version, Some("NetworkService"))
    .expect("Start network service failed");

    wait_for_exit(exit_condvar);
}

fn get_version() -> Version {
    let major = env!("CARGO_PKG_VERSION_MAJOR")
        .parse::<u8>()
        .expect("CARGO_PKG_VERSION_MAJOR parse success");
    let minor = env!("CARGO_PKG_VERSION_MINOR")
        .parse::<u8>()
        .expect("CARGO_PKG_VERSION_MINOR parse success");
    let patch = env!("CARGO_PKG_VERSION_PATCH")
        .parse::<u16>()
        .expect("CARGO_PKG_VERSION_PATCH parse success");
    let dash_pre = {
        let pre = env!("CARGO_PKG_VERSION_PRE");
        if pre == "" {
            pre.to_string()
        } else {
            "-".to_string() + pre
        }
    };

    let commit_describe = option_env!("COMMIT_DESCRIBE").map(ToString::to_string);
    #[cfg(docker)]
    let commit_describe = commit_describe.map(|s| s.replace("-dirty", ""));
    let commit_date = option_env!("COMMIT_DATE").map(ToString::to_string);
    let code_name = None;
    Version {
        major,
        minor,
        patch,
        dash_pre,
        code_name,
        commit_describe,
        commit_date,
    }
}

fn wait_for_exit(exit: Arc<(Mutex<()>, Condvar)>) {
    // Handle possible exits
    let e = Arc::<(Mutex<()>, Condvar)>::clone(&exit);
    let _ = ctrlc::set_handler(move || {
        e.1.notify_all();
    });

    // Wait for signal
    let mut l = exit.0.lock();
    exit.1.wait(&mut l);
}
