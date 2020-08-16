var rpc_uri = "http://localhost:8121";

function render_index() {
    fetch('templates/index.mustache')
    .then((response) => response.text())
    .then((template) => {
        fetch(rpc_uri, {
            method: 'POST',
            headers: new Headers({
                'Content-Type': 'application/json'
            }),
            body: '{"id": 2, "jsonrpc": "2.0", "method": "accounts", "params": []}'
        }).then((rpc_res) => rpc_res.json())
        .then((json) => {
            var rendered = Mustache.render(template, json);
            document.getElementById('main').innerHTML = rendered;
        });
    });
}

function generate_address() {
    fetch(rpc_uri, {
        method: 'POST',
        headers: new Headers({
            'Content-Type': 'application/json'
        }),
        body: '{"id": 2, "jsonrpc": "2.0", "method": "generate_account", "params": []}'
    }).then((rpc_res) => window.location.href = "/")
}