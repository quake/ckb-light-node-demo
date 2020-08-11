use crate::protocols::{ChainStore, ControlMessage};
use crate::store::Store;
use ckb_jsonrpc_types::{CellOutput, JsonBytes, OutPoint, Script, Transaction};
use ckb_types::H256;
use crossbeam_channel::Sender;
use jsonrpc_core::{Error, ErrorCode, IoHandler, Result};
use jsonrpc_derive::rpc;
use jsonrpc_http_server::{Server, ServerBuilder};
use jsonrpc_server_utils::cors::AccessControlAllowOrigin;
use jsonrpc_server_utils::hosts::DomainsValidation;
use serde::Serialize;
use std::net::ToSocketAddrs;

pub struct RpcService<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
    listen_address: String,
}

impl<S: Store + Send + Sync + 'static> RpcService<S> {
    pub fn new(
        chain_store: ChainStore<S>,
        sender: Sender<ControlMessage>,
        listen_address: &str,
    ) -> Self {
        Self {
            chain_store,
            sender,
            listen_address: listen_address.to_string(),
        }
    }

    pub fn start(self) -> Server {
        let mut io_handler = IoHandler::new();
        let RpcService {
            chain_store,
            sender,
            listen_address,
        } = self;

        let rpc_impl = RpcImpl {
            chain_store,
            sender,
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
}

struct RpcImpl<S> {
    chain_store: ChainStore<S>,
    sender: Sender<ControlMessage>,
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
}
