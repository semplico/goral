# Installation

You can download Goral binary with 

```sh
curl --proto '=https' --tlsv1.2 -sSf https://maksimryndin.github.io/goral/download.sh | sh
```

<details>
  <summary>or by downloading a prebuilt binary from <a href="https://github.com/maksimryndin/goral/releases" target="_blank">the releases page</a> manually
</summary>

```sh
wget https://github.com/maksimryndin/goral/releases/download/0.1.15/goral-0.1.15-x86_64-unknown-linux-gnu.tar.gz
tar -xzf goral-0.1.15-x86_64-unknown-linux-gnu.tar.gz
cd goral-0.1.15-x86_64-unknown-linux-gnu/
shasum -a 256 -c sha256_checksum.txt 
```
</details>

<details>
  <summary>or from source</summary>

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
git clone --depth 1 --branch 0.1.15 https://github.com/maksimryndin/goral
cd goral
RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target <target triple>
```
</details>

To run a binary
```sh
goral -c config.toml --id myhost
```

where an `myhost` is an identifier of the machine (no more than 8 chars) and an example `config.toml` can be

```toml
[general]
service_account_credentials_path = "/etc/goral_service_account.json"
messenger.specific.chat_id = "-000000000000"
messenger.url = "https://api.telegram.org/bot12345678:XXXXXxxxxx-XXXddxxss-XXX/sendMessage"

[healthcheck]
spreadsheet_id = "<spreadsheet_id_1>"
messenger.specific.chat_id = "-111111111111"
messenger.url = "https://api.telegram.org/bot12345678:XXXXXxxxxx-XXXddxxss-XXX/sendMessage"
messenger.send_google_append_error = false
autotruncate_at_usage_percent = 90
[[healthcheck.liveness]]
name = "backend"
type = "Http"
endpoint = "http://127.0.0.1:8080"

[logs]
spreadsheet_id = "<spreadsheet_id_2>"
messenger.url = "https://discord.com/api/webhooks/123456789/xxxxx-XXXXX"
autotruncate_at_usage_percent = 90

[metrics]
spreadsheet_id = "<spreadsheet_id_3>"
messenger.specific.token = "xoxb-123-456-XXXXXX"
messenger.specific.channel = "XXXXXXXX"
messenger.url = "https://slack.com/api/chat.postMessage"
autotruncate_at_usage_percent = 90
[[target]]
endpoint = "http://127.0.0.1:8080/metrics"
name = "backend"

[system]
spreadsheet_id = "<spreadsheet_id_4>"
messenger.url = "https://discord.com/api/webhooks/101010101/xxxxx-XXXXX"
autotruncate_at_usage_percent = 90
mounts = ["/", "/var"]
names = ["goral", "mybackend"]
```

See also [Services](./services.md) and [Recommended deployment](./recommended-deployment.md).