use crate::protocols::{ChainStore, ControlMessage};
use crate::store::Store;
use bech32::{convert_bits, Bech32, ToBase32};
use ckb_chain_spec::consensus::Consensus;
use ckb_crypto::secp::Privkey;
use ckb_hash::{blake2b_256, new_blake2b};
use ckb_jsonrpc_types::{
    BlockNumber, Capacity as JsonCapacity, CellOutput, JsonBytes, OutPoint, Script, Transaction,
};
use ckb_types::{
    bytes::Bytes,
    core::{Capacity, DepType, ScriptHashType, TransactionBuilder},
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
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::ToSocketAddrs;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub struct RpcService<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
    listen_address: String,
    private_keys_store_path: String,
    consensus: Consensus,
}

impl<S: Store + Send + Sync + 'static> RpcService<S> {
    pub fn new(
        chain_store: ChainStore<S>,
        sender: Sender<ControlMessage>,
        listen_address: &str,
        private_keys_store_path: &str,
        consensus: &Consensus,
    ) -> Self {
        Self {
            chain_store,
            sender,
            listen_address: listen_address.to_owned(),
            private_keys_store_path: private_keys_store_path.to_owned(),
            consensus: consensus.clone(),
        }
    }

    pub fn start(self) -> Server {
        let mut io_handler = IoHandler::new();
        let RpcService {
            chain_store,
            sender,
            listen_address,
            private_keys_store_path,
            consensus,
        } = self;

        let scripts = chain_store.get_scripts().unwrap();
        let private_keys: Vec<Privkey> =
            if let Ok(mut private_keys_store) = File::open(&private_keys_store_path) {
                let mut buf = Vec::new();
                private_keys_store.read_to_end(&mut buf).unwrap();
                buf.chunks(32)
                    .map(|slice| {
                        let private_key = Privkey::from_slice(slice);
                        let args =
                            blake2b_256(private_key.pubkey().unwrap().serialize())[..20].to_vec();
                        if !scripts
                            .iter()
                            .any(|(script, _)| args == script.args().raw_data())
                        {
                            let script = packed::Script::new_builder()
                                .code_hash(SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH.pack())
                                .args(Bytes::from(args).pack())
                                .hash_type(ScriptHashType::Type.into())
                                .build();

                            chain_store.insert_script(script, 0).unwrap();
                        }
                        private_key
                    })
                    .collect()
            } else {
                Vec::new()
            };

        let rpc_impl = RpcImpl {
            chain_store,
            sender,
            private_keys: Arc::new(RwLock::new(private_keys)),
            private_keys_store_path,
            consensus,
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
    out_point: OutPoint,
    output: CellOutput,
    output_data: JsonBytes,
}

#[derive(Serialize)]
pub struct Account {
    address: String,
    balance: JsonCapacity,
    indexed_block_number: BlockNumber,
}

#[derive(Serialize)]
pub struct AccountTransaction {
    address: String,
    tx_hash: H256,
    balance_change: i64,
    block_number: BlockNumber,
}

const SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH: H256 =
    h256!("0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8");
const DEFAULT_FEE_RATE: usize = 1;

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

    #[rpc(name = "account_transactions")]
    fn account_transactions(&self) -> Result<Vec<AccountTransaction>>;

    #[rpc(name = "transfer")]
    fn transfer(
        &self,
        from_account_index: usize,
        to_address: String,
        capacity: JsonCapacity,
    ) -> Result<H256>;
}

struct RpcImpl<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
    private_keys: Arc<RwLock<Vec<Privkey>>>,
    private_keys_store_path: String,
    consensus: Consensus,
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
            .get_cells(&script.into())
            .map_err(|err| Error {
                code: ErrorCode::InternalError,
                message: err.to_string(),
                data: None,
            })
            .map(|cells| {
                cells
                    .into_iter()
                    .map(|(out_point, output, output_data, _created_by_block_number)| Cell {
                        out_point: out_point.into(),
                        output: output.into(),
                        output_data: output_data.into(),
                    })
                    .collect()
            })
    }

    fn send_transaction(&self, transaction: Transaction) -> Result<H256> {
        let tx: packed::Transaction = transaction.into();
        let tx_hash = tx.calc_tx_hash();
        self.send_control_message(ControlMessage::SendTransaction(tx))
            .map(|_| tx_hash.unpack())
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
            if let Ok(pubkey) = privkey.pubkey() {
                self.private_keys.write().unwrap().push(privkey);
                let mut file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&self.private_keys_store_path)
                    .unwrap();
                file.write_all(&raw).unwrap();

                let script = packed::Script::new_builder()
                    .code_hash(SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH.pack())
                    .args(Bytes::from(blake2b_256(pubkey.serialize())[..20].to_vec()).pack())
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
            let address = script_to_address(&script, &self.consensus.id);
            let total_capacity = self
                .chain_store
                .get_cells(&script.into())
                .unwrap()
                .iter()
                .map(|(_out_point, output, _output_data, _created_by_block_number)| {
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

    fn account_transactions(&self) -> Result<Vec<AccountTransaction>> {
        let mut account_changes: HashMap<_, i64> = HashMap::new();

        for (script, _) in self.chain_store.get_scripts().unwrap() {
            for (out_point, cell_output, _output_data, created_by_block_number, consumed_by_tx_hash, consumed_by_block_number) in
                self.chain_store.get_consumed_cells(&script).unwrap()
            {
                let capacity: Capacity = cell_output.capacity().unpack();
                account_changes
                    .entry((script.clone(), out_point.tx_hash(), created_by_block_number))
                    .and_modify(|balance_change| {
                        *balance_change += capacity.as_u64() as i64;
                    })
                    .or_insert_with(|| capacity.as_u64() as i64);

                account_changes
                    .entry((script.clone(), consumed_by_tx_hash, consumed_by_block_number))
                    .and_modify(|balance_change| {
                        *balance_change -= capacity.as_u64() as i64;
                    })
                    .or_insert_with(|| -(capacity.as_u64() as i64));
            }

            for (out_point, cell_output, _output_data, created_by_block_number) in
                self.chain_store.get_cells(&script).unwrap()
            {
                let capacity: Capacity = cell_output.capacity().unpack();
                account_changes
                    .entry((script.clone(), out_point.tx_hash(), created_by_block_number))
                    .and_modify(|balance_change| {
                        *balance_change += capacity.as_u64() as i64;
                    })
                    .or_insert_with(|| capacity.as_u64() as i64);
            }
        }

        let result: Vec<_> = account_changes.into_iter().map(|((script, tx_hash, block_number), balance_change)| {
            AccountTransaction {
                address: script_to_address(&script, &self.consensus.id),
                tx_hash: tx_hash.unpack(),
                balance_change,
                block_number: block_number.into(),
            }
        }).collect();

        Ok(result)
    }

    fn transfer(
        &self,
        from_account_index: usize,
        to_address: String,
        to_capacity: JsonCapacity,
    ) -> Result<H256> {
        let to_script = address_to_script(&to_address).map_err(|err| Error {
            code: ErrorCode::InternalError,
            message: format!("parse script error: {}", err),
            data: None,
        })?;

        if let Some((from_script, _)) = self
            .chain_store
            .get_scripts()
            .unwrap()
            .get(from_account_index)
        {
            let cell_dep = packed::CellDep::new_builder()
                .out_point(packed::OutPoint::new(
                    self.consensus.genesis_block().transactions()[1].hash(),
                    0,
                ))
                .dep_type(DepType::DepGroup.into())
                .build();

            let to_address_output = packed::CellOutput::new_builder()
                .capacity(to_capacity.pack())
                .lock(to_script)
                .build();

            let change_address_output_placeholder = packed::CellOutput::new_builder()
                .capacity(0.pack())
                .lock(from_script.clone())
                .build();
            let min_change_capacity = change_address_output_placeholder
                .occupied_capacity(Capacity::zero())
                .unwrap();

            let witness_placeholder = packed::WitnessArgs::new_builder()
                .lock(Some(Bytes::from(vec![0u8; 65])).pack())
                .build();

            let mut tx_builder = TransactionBuilder::default()
                .cell_dep(cell_dep)
                .output(to_address_output.clone())
                .output_data(Default::default())
                .output(change_address_output_placeholder)
                .output_data(Default::default())
                .witness(witness_placeholder.as_bytes().pack());

            let mut inputs_capacity = Capacity::zero();

            for (out_point, output, _output_data, _created_by_block_number) in
                self.chain_store.get_cells(from_script).unwrap()
            {
                inputs_capacity = inputs_capacity
                    .safe_add::<Capacity>(output.capacity().unpack())
                    .unwrap();

                let input = packed::CellInput::new(out_point, 0);
                tx_builder = tx_builder.input(input);

                if inputs_capacity > to_capacity.into() {
                    let tx = tx_builder.clone().build();
                    let fee = Capacity::shannons(
                        (tx.data().serialized_size_in_block() * DEFAULT_FEE_RATE) as u64,
                    );
                    let transfer_capacity: Capacity = to_capacity.into();
                    if inputs_capacity
                        >= transfer_capacity
                            .safe_add(fee)
                            .unwrap()
                            .safe_add(min_change_capacity)
                            .unwrap()
                    {
                        let change_address_output = packed::CellOutput::new_builder()
                            .capacity(
                                (inputs_capacity
                                    .safe_sub(fee)
                                    .unwrap()
                                    .safe_sub(transfer_capacity)
                                    .unwrap())
                                .pack(),
                            )
                            .lock(from_script.clone())
                            .build();

                        let unsigned_tx_builder = tx_builder
                            .clone()
                            .set_outputs(vec![to_address_output.clone(), change_address_output]);
                        let unsigned_tx = unsigned_tx_builder.clone().build();
                        let tx_hash = unsigned_tx.hash();

                        let witness_len = witness_placeholder.as_slice().len() as u64;
                        let message = {
                            let mut hasher = new_blake2b();
                            hasher.update(tx_hash.as_slice());
                            hasher.update(&witness_len.to_le_bytes());
                            hasher.update(witness_placeholder.as_slice());
                            let mut buf = [0u8; 32];
                            hasher.finalize(&mut buf);
                            H256::from(buf)
                        };

                        // this demo use a dirty and insecure fn to find the corresponding private key and sign the message
                        let private_key = self
                            .private_keys
                            .read()
                            .unwrap()
                            .iter()
                            .find(|private_key| {
                                blake2b_256(private_key.pubkey().unwrap().serialize())[..20]
                                    .to_vec()
                                    == from_script.args().raw_data()
                            })
                            .cloned()
                            .unwrap();

                        let signature = private_key.sign_recoverable(&message).expect("sign");
                        let witness = packed::WitnessArgs::new_builder()
                            .lock(Some(Bytes::from(signature.serialize())).pack())
                            .build();

                        let signed_tx = unsigned_tx_builder
                            .set_witnesses(vec![witness.as_bytes().pack()])
                            .build()
                            .data();

                        return self
                            .send_control_message(ControlMessage::SendTransaction(signed_tx))
                            .map(|_| tx_hash.unpack());
                    }
                }
            }
        }
        Err(Error {
            code: ErrorCode::InternalError,
            message: "cannot get enough input cells".to_owned(),
            data: None,
        })
    }
}

fn script_to_address(script: &packed::Script, chain_id: &str) -> String {
    let hrp = if chain_id == "ckb" { "ckb" } else { "ckt" };
    let mut data = vec![0; 22];
    data[0] = 1;
    data[2..].copy_from_slice(&script.args().raw_data());
    format!(
        "{}",
        Bech32::new(hrp.to_string(), data.to_base32()).unwrap()
    )
}

fn address_to_script(address: &str) -> std::result::Result<packed::Script, String> {
    let value = Bech32::from_str(address).map_err(|err| err.to_string())?;
    let data = convert_bits(value.data(), 5, 8, false).unwrap();
    if data[0] != 1 {
        Err("this demo only supports short address".to_owned())
    } else {
        if data.len() != 22 {
            Err("Invalid address length".to_owned())
        } else {
            Ok(packed::Script::new_builder()
                .code_hash(SECP256K1_BLAKE160_SIGHASH_ALL_TYPE_HASH.pack())
                .args(Bytes::from(data[2..22].to_vec()).pack())
                .hash_type(ScriptHashType::Type.into())
                .build())
        }
    }
}
