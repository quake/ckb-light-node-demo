use crate::protocols::{ChainStore, ControlMessage};
use crate::store::Store;
use bech32::{Bech32, ToBase32};
use ckb_crypto::secp::Privkey;
use ckb_jsonrpc_types::{
    BlockNumber, Capacity as JsonCapacity, CellOutput, JsonBytes, OutPoint, Script, Transaction,
};
use ckb_types::{
    bytes::Bytes,
    core::{Capacity, ScriptHashType},
    h256, packed,
    prelude::*,
    H256,
};
use crossbeam_channel::Sender;
use jsonrpc_core::{Error, ErrorCode, IoHandler, Result};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{Server, ServerBuilder};
use jsonrpc_server_utils::cors::AccessControlAllowOrigin;
use jsonrpc_server_utils::hosts::DomainsValidation;
use rand::{thread_rng, Rng};
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::sync::{Arc, RwLock};

pub struct RpcService<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
    listen_address: String,
    private_keys_store_path: String,
    chain_name: String,
}

impl<S: Store + Send + Sync + 'static> RpcService<S> {
    pub fn new(
        chain_store: ChainStore<S>,
        sender: Sender<ControlMessage>,
        listen_address: &str,
        private_keys_store_path: &str,
        chain_name: &str,
    ) -> Self {
        Self {
            chain_store,
            sender,
            listen_address: listen_address.to_owned(),
            private_keys_store_path: private_keys_store_path.to_owned(),
            chain_name: chain_name.to_owned(),
        }
    }

    pub fn start(self) -> Server {
        let mut io_handler = IoHandler::new();
        let RpcService {
            chain_store,
            sender,
            listen_address,
            private_keys_store_path,
            chain_name,
        } = self;

        let private_keys: Vec<Privkey> =
            if let Ok(mut private_keys_store) = File::open(&private_keys_store_path) {
                let mut buf = Vec::new();
                private_keys_store.read_to_end(&mut buf).unwrap();
                buf.chunks(32)
                    .map(|slice| Privkey::from_slice(slice))
                    .collect()
            } else {
                Vec::new()
            };

        let rpc_impl = RpcImpl {
            chain_store,
            sender,
            private_keys: Arc::new(RwLock::new(private_keys)),
            private_keys_store_path,
            chain_name,
        };
        io_handler.extend_with(rpc_impl.to_delegate());

        ServerBuilder::new(io_handler)
            .cors(DomainsValidation::AllowOnly(vec![
                AccessControlAllowOrigin::Null,
                AccessControlAllowOrigin::Any,
            ]))
            .health_api(("/ping", "ping"))
            .start_http(
                &listen_address
                    .to_socket_addrs()
                    .expect("config listen_address parsed")
                    .next()
                    .expect("config listen_address parsed"),
            )
            .expect("Start Jsonrpc HTTP service")
    }
}

#[derive(Serialize)]
pub struct Cell {
    output: CellOutput,
    output_data: JsonBytes,
    out_point: OutPoint,
}

#[derive(Serialize)]
pub struct Account {
    address: String,
    balance: JsonCapacity,
    indexed_block_number: BlockNumber,
}

const SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");

#[rpc(server)]
pub trait Rpc {
    #[rpc(name = "add_filter")]
    fn add_filter(&self, script: Script) -> Result<()>;

    #[rpc(name = "get_cells")]
    fn get_cells(&self, script: Script) -> Result<Vec<Cell>>;

    #[rpc(name = "send_transaction")]
    fn send_transaction(&self, transaction: Transaction) -> Result<H256>;

    #[rpc(name = "start")]
    fn start(&self) -> Result<()>;

    #[rpc(name = "stop")]
    fn stop(&self) -> Result<()>;

    #[rpc(name = "generate_account")]
    fn generate_account(&self) -> Result<()>;

    #[rpc(name = "accounts")]
    fn accounts(&self) -> Result<Vec<Account>>;
}

struct RpcImpl<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
    private_keys: Arc<RwLock<Vec<Privkey>>>,
    private_keys_store_path: String,
    chain_name: String,
}

impl<S> RpcImpl<S> {
    fn send_control_message(&self, msg: ControlMessage) -> Result<()> {
        self.sender.send(msg).map_err(|err| Error {
            code: ErrorCode::InternalError,
            message: err.to_string(),
            data: None,
        })
    }
}

impl<S: Store + Send + Sync + 'static> Rpc for RpcImpl<S> {
    fn add_filter(&self, script: Script) -> Result<()> {
        self.send_control_message(ControlMessage::AddFilter(script.into()))
    }

    fn get_cells(&self, script: Script) -> Result<Vec<Cell>> {
        self.chain_store
            .get_cells(script.into())
            .map_err(|err| Error {
                code: ErrorCode::InternalError,
                message: err.to_string(),
                data: None,
            })
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(output, output_data, out_point)| Cell {
                        output: output.into(),
                        output_data: output_data.into(),
                        out_point: out_point.into(),
                    })
                    .collect()
            })
    }

    fn send_transaction(&self, transaction: Transaction) -> Result<H256> {
        Ok(H256::default())
    }

    fn start(&self) -> Result<()> {
        self.send_control_message(ControlMessage::Start)
    }

    fn stop(&self) -> Result<()> {
        self.send_control_message(ControlMessage::Stop)
    }

    fn generate_account(&self) -> Result<()> {
        let mut rng = thread_rng();
        let mut raw = [0; 32];
        loop {
            rng.fill(&mut raw);
            let privkey = Privkey::from_slice(&raw[..]);
            if privkey.pubkey().is_ok() {
                self.private_keys.write().unwrap().push(privkey);
                let mut file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&self.private_keys_store_path)
                    .unwrap();
                file.write_all(&raw).unwrap();

                let script = packed::Script::new_builder()
                    .code_hash(SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH.pack())
                    .args(Bytes::from(raw.to_vec()).pack())
                    .hash_type(ScriptHashType::Type.into())
                    .build();

                self.chain_store.insert_script(script, 0).unwrap();
                return Ok(());
            }
        }
    }

    fn accounts(&self) -> Result<Vec<Account>> {
        let mut result = Vec::new();
        for (script, block_number) in self.chain_store.get_scripts().unwrap() {
            let address = script_to_address(&script, &self.chain_name);
            let total_capacity = self
                .chain_store
                .get_cells(script.into())
                .unwrap()
                .iter()
                .map(|(output, _output_data, _out_point)| {
                    let capacity: Capacity = output.capacity().unpack();
                    capacity
                })
                .try_fold(Capacity::zero(), Capacity::safe_add)
                .unwrap();

            result.push(Account {
                address,
                balance: total_capacity.into(),
                indexed_block_number: block_number.into(),
            });
        }

        Ok(result)
    }
}

fn script_to_address(script: &packed::Script, chain_name: &str) -> String {
    let hrp = if chain_name == "mainnet" {
        "ckb"
    } else {
        "ckt"
    };
    let mut data = vec![0; 22];
    data[0] = 1;
    data[2..].copy_from_slice(&script.args().raw_data());
    format!(
        "{}",
        Bech32::new(hrp.to_string(), data.to_base32()).unwrap()
    )
}
