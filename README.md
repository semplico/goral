# Goral

> Save your time with Goral to setup the whole observability/monitoring stack when you start

Observability toolkit for small projects. Easy-to-use and compatible with industry standards.

```sh
curl --proto '=https' --tlsv1.2 -sSf https://maksimryndin.github.io/goral/install.sh | sh
```

Also check out [setup](https://maksimryndin.github.io/goral/setup.html) and [recommended deployment](https://maksimryndin.github.io/goral/recommended-deployment.html).

## Overview

Goral is a simple observability daemon developed with the following idea in mind: when you have your favorite application in its infantry, you usually don't need a full-blown observability toolkit (which requires much more setup, maintenance and resources) around as the amount of data is not so huge. It is especially true for pet projects when you just want to test an idea and deploy the app at some free-tier commodity VPS.

So Goral provides the following features being deployed next to your app(s):
* [Periodic healthchecks](https://maksimryndin.github.io/goral/healthcheck.html) (aka [liveness probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/))
* [Metrics collection](https://maksimryndin.github.io/goral/metrics.html) (fully compatible with Prometheus to be easily replaced with more advanced stack as your project grows)
* [Logs](https://maksimryndin.github.io/goral/logs.html) collection (importing logs from stdout/stderr of the target process)
* [System telemetry](https://maksimryndin.github.io/goral/system.html) (CPU, Memory, Free/Busy storage space, ssh access log etc)
* A general key-value appendable log storage (see [the user case](https://maksimryndin.github.io/goral/kv-log.html))
* Features are modular - all [services](https://maksimryndin.github.io/goral/services.html) are switched on/off in the configuration.
* You can observe several instances of the same app or different apps on the same host with a single Goral daemon (except logs as logs are collected via stdin of Goral - see [Logs](https://maksimryndin.github.io/goral/logs.html))
* You can configure different messengers and/or channels for every [service](https://maksimryndin.github.io/goral/services.html) to get notifications on errors, liveness updates, system resources overlimit etc
* All the data collected is stored in Google Sheet with an automatic quota and limits checks and automatic data rotation - old data is deleted with a preliminary notification via configured messenger. That way you don't have to buy a separate storage or overload your app VPS with Prometheus, ELK etc. Just a lean process next to your brilliant one which just sends app data in batches to Google Sheets for your ease of use. Google Sheets allow you to build your own diagrams over the metrics and analyse them, analyse liveness statistics and calculate uptime etc.
* You can configure different spreadsheets and messengers for every service
* You can configure [rules](https://maksimryndin.github.io/goral/rules.html) for notifications by messengers for any data.

## Licence

Apache 2.0 licence is also applied to all commits in this repository before this licence was specified.