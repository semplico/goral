# Goral

> Save your time with Goral to setup the whole observability/monitoring stack when you start

A lean observability toolkit. Easy-to-use and compatible with industry standards.

```sh
curl --proto '=https' --tlsv1.2 -sSf https://semplico.github.io/goral/download.sh | sh
```

Also check out [setup](https://semplico.github.io/goral/setup.html) and [recommended deployment](https://semplico.github.io/goral/recommended-deployment.html).

## Overview

Goral is a lean observability daemon developed with the following idea in mind: most applications have small to moderate number of users so a full-blown observability toolkit (which requires much more setup, maintenance and resources) is not required as the amount of data is small.

So Goral provides the following features being deployed next to your app(s):
* [Periodic healthchecks](https://semplico.github.io/goral/healthcheck.html) (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* [Metrics collection](https://semplico.github.io/goral/metrics.html) (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* [Logs](https://semplico.github.io/goral/logs.html) collection (importing logs from stdout/stderr of the target process)
* [System telemetry](https://semplico.github.io/goral/system.html) (CPU, Memory, Free/Busy storage space, ssh access log etc)
* A general key-value appendable log storage (see [the user case](https://semplico.github.io/goral/kv-log.html))
* Features are modular - all [services](https://semplico.github.io/goral/services.html) are switched on/off in the configuration.
* You can observe several instances of the same app or different apps on the same host with a single Goral daemon (except logs as logs are collected via stdin of Goral - see [Logs](https://semplico.github.io/goral/logs.html))
* You can configure different messengers and/or channels for every [service](https://semplico.github.io/goral/services.html) to get notifications on errors, liveness updates, system resources overlimit etc
* All the data collected is stored in Google Sheets with an automatic quota and limits checks and automatic data rotation - old data is deleted with a preliminary notification via configured messenger. That way you don't have to buy a separate storage or overload your app VPS with Prometheus, ELK etc. Google Sheets allow you to build your own diagrams over the metrics and analyse them, analyse liveness statistics and calculate uptime etc.
* You can configure different spreadsheets and messengers for every service.
* You can configure [rules](https://semplico.github.io/goral/rules.html) for notifications by messengers for any data.

## Licence

Apache 2.0 licence is also applied to all commits in this repository before this licence was specified.
