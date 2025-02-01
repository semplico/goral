# System

System service configuration:

<details open>
  <summary>Basic configuration</summary>

```toml
[system]
spreadsheet_id = "<spreadsheet_id>"
```
</details>

<details>
  <summary>Full configuration</summary>

```toml
[system]
spreadsheet_id = "<spreadsheet_id>"
# push_interval_secs = 20
# autotruncate_at_usage_percent = 20 # approx 2 days of data under default settings
# scrape_interval_secs = 10
# scrape_timeout_ms = 3000
# messenger.url = "<messenger api url for sending messages>"
# mounts = ["/"]
# names = ["goral"]
```
</details>

With this configuration System service will create the following sheets:
* `basic`: with general information about the system (boot time, memory, cpus, swap, a number of processes)
* `network`: for every network interface a number of bytes read/written _since the previous measurement_, total read/written
* `top_disk_read` - process which has read the most from disk _during the last second_
* `top_disk_write` - process which has written the most to disk _during the last second_
* `top_cpu` - process with the most cpu time during the last second
* `top_memory` - process with the most memory usage _during the last second_
* `top_open_files` (for Linux only) - among the processes with the same user as Goral (!) - process with the most opened files (files in a Linux sense - includes also sockets, pipes etc)
* for every process with name containing one of the substrings in `names` - a sheet with process info. Note: the first match (_case sensitive_) is used so plan accordingly a unique name for your binary.
* for every mount in `mounts` - disk usage and free space.
* `ssh` - for Linux systems ssh access log is monitored. There is a `status` field with the following values:
  * `rejected` - ssh user is correct but the key or password is wrong. Also a catchall reason for other unsuccessful connections.
  * `invalid_user` - an invalid ssh user (unexisting) was used.
  * `timeout` - a timeout on ssh connection happened.
  * `wrong_params` - no matching key exchange method found or an invalid format of the key
  * `connected` - a successful ssh connection is established (by default there is a rule with a warning notification for this event)
  * `terminated` - an ssh session (established earlier with `connected`) is terminated

System service doesn't require root privileges to collect the telemetry.
For a process a cpu usage percent may be [more than 100%](https://blog.guillaume-gomez.fr/articles/2021-09-06+sysinfo%3A+how+to+extract+systems%27+information) in a case of a multi-threaded process on several cores. `memory_used` by process is a [resident-set size](https://www.baeldung.com/linux/resident-set-vs-virtual-memory-size).

If there is an error while collecting system info, it is sent via a configured messenger or via a default messenger of General service.