# Healthcheck

Healthcheck service with a configuration

<details open>
  <summary>Basic configuration</summary>

```toml
[healthcheck]
spreadsheet_id = "<spreadsheet_id>"
[[healthcheck.liveness]]
type = "Http"
endpoint = "http://127.0.0.1:9898"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[healthcheck]
spreadsheet_id = "<spreadsheet_id>"
# messenger.url = "<messenger api url for sending messages>"
# push_interval_secs = 20
# autotruncate_at_usage_percent = 10
[[healthcheck.liveness]]
# name = "http://127.0.0.1:9898" # by default the endpoint itself is used as a name
# initial_delay_secs = 0
# period_secs = 5
type = "Http"
endpoint = "http://127.0.0.1:9898"
# timeout_ms = 3000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "ls -lha" # by default the command itself is used as a name
# initial_delay_secs = 0
# period_secs = 5
type = "Command"
command = ["ls", "-lha"]
# timeout_ms = 3000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "[::1]:9898" # by default the tcp socket addr itself is used as a name
# initial_delay_secs = 0
# period_secs = 5
type = "Tcp"
endpoint = "[::1]:9898"
# timeout_ms = 3000 # should be less than or equal period_secs
[[healthcheck.liveness]]
# name = "http://[::1]:50050" # by default the tcp socket addr itself is used as a name
# initial_delay_secs = 0
# period_secs = 5
type = "Grpc"
endpoint = "http://[::1]:50050"
# timeout_ms = 3000 # should be less than or equal period_secs
```
</details>

will create a sheet for every liveness probe.

Liveness probes follow the same rules as for [k8s](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/). Namely:
* you should choose among HTTP GET (any status `>=200` and `<400`), gRPC, TCP (Goral can open socket) and command (successful exit)
* you can specify an initial delay, a period of a probe and a timeout on a probe
* misconfiguration of a liveness probe is considered a failure
* for gRPC Health service should be configured on the app side (see also [gRPC health checking protocol](https://github.com/grpc/grpc/blob/v1.59.1/doc/health-checking.md)). Only `http` scheme is supported at the moment. If you need a tls check, you can use a command probe with a [grpc health probe](https://github.com/grpc-ecosystem/grpc-health-probe) and specify proper certificates.

Goral saves probe time, a status (true for alive) and a text output (for HTTP GET - a response text, for a command - a stdout output, for all probes - an error text). Each probe is saved at a separate sheet.
In case an output is larger than 1024 bytes, it is truncated and you get permanent warnings in logs of Goral. So configure the output size of your healthcheck reasonably (healthcheck responses shouldn't be heavy).
For command healthchecks it is recommended to assign a name to a liveness probe or wrap your command in some script so that in case of small changes in command arguments preserve the same sheet for data (otherwise Goral will create a new sheet since the title has changed).

If a messenger is configured, then any healthcheck change (healthy -> unhealthy and vice versa) is sent via the messenger. In case of many endpoints with short liveness periods there is a risk to hit a rate limit of a messenger.