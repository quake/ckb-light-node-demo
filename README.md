## Usage

```bash
cargo build --release
mkdir /tmp/ckb-light-node-demo
cp config.toml /tmp/ckb-light-node-demo
CKB_LOG=info ./target/release/ckb-light-node-demo -d /tmp/ckb-light-node-demo -c mainnet
```

## RPC

### `add_script_to_filter`


#### Parameters
    script: Script

#### Returns

    live cells

#### Examples

Add a script to filter:

```bash
echo '{
    "id": 2,
    "jsonrpc": "2.0",
    "method": "add_filter",
    "params": [
        {
            "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
            "hash_type": "type",
            "args": "0xcdafc491453d8e2b4209d7f05072a7f1e24b22fd"
        }
    ]
}' \
| tr -d '\n' \
| curl -H 'content-type: application/json' -d @- \
http://localhost:8121
```

Start filter protocol:

```bash
echo '{
    "id": 2,
    "jsonrpc": "2.0",
    "method": "start",
    "params": []
}' \
| tr -d '\n' \
| curl -H 'content-type: application/json' -d @- \
http://localhost:8121
```

Get live cells:

```bash
echo '{
    "id": 2,
    "jsonrpc": "2.0",
    "method": "get_cells",
    "params": [
        {
            "code_hash": "0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8",
            "hash_type": "type",
            "args": "0xcdafc491453d8e2b4209d7f05072a7f1e24b22fd"
        }
    ]
}' \
| tr -d '\n' \
| curl -H 'content-type: application/json' -d @- \
http://localhost:8121
```