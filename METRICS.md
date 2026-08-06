# observant-agent metrics

The agent writes InfluxDB line protocol. Every measurement name starts with
`obs_`. One collection cycle uses one timestamp for every point in the batch.

## Value types

- A **gauge** is the value at the moment of the sample.
- A **counter** is the cumulative total since boot or since container start.
  The agent never sends a rate. The ingest layer computes rates.

Integer fields carry the `i` suffix. Float fields carry no suffix. A value
above `int64` maximum is sent as a float.

## Tags on every point

| Tag    | Source                                        |
| ------ | --------------------------------------------- |
| `host` | `-hostname`, `OBSERVANT_HOSTNAME`, or the system hostname |
| `role` | `-role` or `OBSERVANT_ROLE`. The tag is absent when unset. |

The agent trims the whitespace from both values. A value of only whitespace
counts as unset, so `-hostname " "` falls back to the system hostname.

## Metric names in VictoriaMetrics

VictoriaMetrics stores line protocol as flat metric names. It joins the
measurement and the field key with an underscore. Every tag becomes a label.

One input line:

```
obs_cpu,host=web-1,role=builder,cores=4 usage_percent=12.5,idle_percent=87.5 1700000000000000000
```

becomes two time series:

```
obs_cpu_usage_percent{host="web-1",role="builder",cores="4"}  12.5
obs_cpu_idle_percent{host="web-1",role="builder",cores="4"}   87.5
```

So the query name is always `<measurement>_<field>`. Example queries:

```promql
# Host CPU busy percent, per host.
obs_cpu_usage_percent

# Disk space left on the root filesystem of every host.
100 - obs_disk_used_percent{mount="/"}

# Network receive rate in bytes per second. The agent sends the counter,
# so the rate is computed at query time.
rate(obs_net_rx_bytes[5m])

# Memory of one container across redeploys. The container ID is a field,
# not a label, so the series survives a redeploy.
obs_docker_mem_used_bytes{container="web"}

# Which image a container runs now. A string field becomes its own series
# with the text in a "value" label.
obs_docker_image{container="web"}

# Agents that stopped reporting in the last 5 minutes.
absent_over_time(obs_agent_up[5m])
```

A string field, for example `obs_docker_image`, is not a number. Query it for
identity, not for math.

## obs_cpu

One point per cycle. The percent fields cover the time since the previous
cycle.

| Tag     | Meaning                    |
| ------- | -------------------------- |
| `cores` | Logical core count as text |

| Field             | Type    | Meaning                                    |
| ----------------- | ------- | ------------------------------------------ |
| `usage_percent`   | gauge   | Non-idle time, 0 to 100                    |
| `user_percent`    | gauge   | User plus nice time                        |
| `system_percent`  | gauge   | System plus irq plus softirq time          |
| `iowait_percent`  | gauge   | I/O wait time                              |
| `steal_percent`   | gauge   | Time the hypervisor took                   |
| `idle_percent`    | gauge   | Idle time                                  |
| `user_seconds`    | counter | Cumulative user CPU seconds                |
| `system_seconds`  | counter | Cumulative system CPU seconds              |
| `idle_seconds`    | counter | Cumulative idle CPU seconds                |
| `nice_seconds`    | counter | Cumulative nice CPU seconds                |
| `iowait_seconds`  | counter | Cumulative I/O wait seconds                |
| `irq_seconds`     | counter | Cumulative hard interrupt seconds          |
| `softirq_seconds` | counter | Cumulative soft interrupt seconds          |
| `steal_seconds`   | counter | Cumulative stolen seconds                  |

The agent does not send a series per core. The core count is a tag.

## obs_load

One point per cycle. No extra tags.

| Field    | Type  | Meaning                    |
| -------- | ----- | -------------------------- |
| `load1`  | gauge | 1-minute run-queue average |
| `load5`  | gauge | 5-minute average           |
| `load15` | gauge | 15-minute average          |

## obs_mem

One point per cycle. No extra tags. Every byte field is a gauge.

| Field               | Meaning                                     |
| ------------------- | ------------------------------------------- |
| `total_bytes`       | Physical memory                             |
| `used_bytes`        | Memory in use                               |
| `available_bytes`   | Memory a new process can take without swap  |
| `free_bytes`        | Memory never touched                        |
| `cached_bytes`      | Page cache. 0 on darwin.                    |
| `buffers_bytes`     | Block buffers. 0 on darwin.                 |
| `used_percent`      | `used_bytes` share of total, 2 decimals     |
| `swap_total_bytes`  | Swap size                                   |
| `swap_used_bytes`   | Swap in use                                 |
| `swap_free_bytes`   | Swap free                                   |
| `swap_used_percent` | Swap share in use, 2 decimals               |

## obs_disk

One point per real mountpoint per cycle.

The agent skips pseudo filesystems (`tmpfs`, `devtmpfs`, `sysfs`, `proc`,
`overlay`, `squashfs`, `cgroup2`, and others) and the mount trees `/dev`,
`/proc`, `/sys`, `/run`, `/var/lib/docker`, `/var/lib/containers`,
`/var/lib/kubelet`, `/snap`, `/var/snap`, and several `/System/Volumes` paths
on darwin. It also skips a mount with a total size of 0, and it keeps only the
first mount of a repeated device and size pair.

Every mount read has a 2 second deadline. A dead network mount blocks the
statfs syscall and cannot be cancelled, so the agent reports the other mounts
and leaves that mount out of the cycle. After 3 timeouts in a row the agent
stops reading the mount. The mount comes back when it leaves the mount table
and returns. The agent logs the state one time, not one time per cycle.

| Tag      | Meaning                                  |
| -------- | ---------------------------------------- |
| `mount`  | Mountpoint path                          |
| `device` | Block device without the `/dev/` prefix  |
| `fstype` | Filesystem type                          |

| Field                 | Type  | Meaning                        |
| --------------------- | ----- | ------------------------------ |
| `total_bytes`         | gauge | Filesystem size                |
| `used_bytes`          | gauge | Bytes in use                   |
| `free_bytes`          | gauge | Bytes free                     |
| `used_percent`        | gauge | Share in use, 2 decimals       |
| `inodes_total`        | gauge | Inode count                    |
| `inodes_used`         | gauge | Inodes in use                  |
| `inodes_free`         | gauge | Inodes free                    |
| `inodes_used_percent` | gauge | Inode share in use, 2 decimals |

## obs_diskio

One point per block device per cycle. Every field is a counter.

The agent skips the device prefixes `loop`, `ram`, `zram`, `fd`, `sr`, and
`dm-`, and any device with no I/O since boot.

| Tag      | Meaning     |
| -------- | ----------- |
| `device` | Device name |

| Field           | Meaning                              |
| --------------- | ------------------------------------ |
| `read_bytes`    | Bytes read since boot                |
| `write_bytes`   | Bytes written since boot             |
| `read_ops`      | Read operations since boot           |
| `write_ops`     | Write operations since boot          |
| `read_time_ms`  | Milliseconds spent reading           |
| `write_time_ms` | Milliseconds spent writing           |
| `io_time_ms`    | Milliseconds with I/O in flight      |
| `in_progress`   | Operations in flight now (gauge)     |

## obs_net

One point per network interface per cycle. Every field is a counter.

The agent skips loopback and the interface prefixes `lo`, `gif`, `stf`,
`anpi`, `awdl`, `llw`, `utun`, and `ap` when a number follows the prefix.

The agent also skips every interface that starts with `veth`, `docker`, `br-`,
`podman`, `cni`, `virbr`, `flannel`, `cali`, `tap`, or `dummy`. Those are the
container and virtual machine interfaces. They mirror the traffic of the real
interface, so a fleet chart would count the same bytes several times. A host
bridge with a plain name, for example `br0` or `bond0`, keeps its series.

The agent skips any interface with no traffic since boot.

| Tag         | Meaning        |
| ----------- | -------------- |
| `interface` | Interface name |

| Field        | Meaning                       |
| ------------ | ----------------------------- |
| `rx_bytes`   | Bytes received since boot     |
| `tx_bytes`   | Bytes sent since boot         |
| `rx_packets` | Packets received since boot   |
| `tx_packets` | Packets sent since boot       |
| `rx_errs`    | Receive errors since boot     |
| `tx_errs`    | Send errors since boot        |
| `rx_drops`   | Received packets dropped      |
| `tx_drops`   | Sent packets dropped          |

## obs_host

One point per cycle. The tags change only after a kernel or an operating
system upgrade.

| Tag                | Meaning                                    |
| ------------------ | ------------------------------------------ |
| `os`               | Operating system, for example `linux`      |
| `platform`         | Distribution, for example `debian`         |
| `platform_version` | Distribution version                       |
| `kernel`           | Kernel version                             |
| `arch`             | Kernel architecture                        |
| `virt`             | Virtualization system, absent when unknown |

| Field            | Type    | Meaning                              |
| ---------------- | ------- | ------------------------------------ |
| `uptime_seconds` | gauge   | Seconds since boot                   |
| `boot_time`      | gauge   | Boot time as a unix timestamp        |
| `procs`          | gauge   | Process count                        |

A change in `boot_time` means a reboot.

## obs_agent

One point per cycle. The measurement reports that the agent itself runs.

| Tag | Meaning |
| --- | ------- |
| none, beyond `host` and `role` | |

| Field        | Type   | Meaning                                       |
| ------------ | ------ | --------------------------------------------- |
| `version`    | string | Agent version, from the build `-ldflags`      |
| `start_time` | gauge  | Process start as a unix timestamp in seconds  |
| `up`         | gauge  | 1 on a normal cycle, 0 on the final cycle     |

The agent sends `up=1` on every cycle. On SIGTERM or SIGINT the agent sends
one last batch with `up=0`. So:

- `up=0` means a clean stop. Somebody stopped the service.
- No point at all, and no `up=0` before it, means the host or the agent
  vanished. Alert on that case.
- A `start_time` that moves forward means the agent restarted.

### obs_agent up is not the liveness signal

The ingest server derives `obs_host_up` from the moment of the last push of
each host. That server-derived series is the authoritative liveness signal.
Alert on `obs_host_up`, not on `obs_agent_up`. The `up` field of `obs_agent`
is a heartbeat input only. A host that dies sends no `obs_agent` point at all,
so the last `obs_agent_up` value stays 1 forever and hides the outage. The
server writes `obs_host_up=0` for that host on the next cycle.

## obs_docker

One point per container per cycle. The measurement covers Docker and Podman.
The agent emits nothing when no container socket exists.

| Tag         | Meaning                        |
| ----------- | ------------------------------ |
| `container` | Container name without the `/` |

The container name is the only tag. The container ID and the image are string
fields. Both change on every redeploy. As tags they would start a new series
on every redeploy and grow the series count without limit.

| Field             | Type    | Running | Stopped | Meaning                    |
| ----------------- | ------- | ------- | ------- | -------------------------- |
| `container_id`    | string  | yes     | yes     | First 12 characters of the ID |
| `image`           | string  | yes     | yes     | Image reference            |
| `running`         | gauge   | `1`     | `0`     | Runtime state              |
| `restart_count`   | counter | yes     | yes     | Restarts since creation    |
| `cpu_percent`     | gauge   | yes     | no      | CPU share since the previous cycle. 100 means one full core. The ceiling is the online core count times 100. |
| `cpu_total_nanos` | counter | yes     | no      | Cumulative CPU nanoseconds since container start |
| `mem_used_bytes`  | gauge   | yes     | no      | Memory in use, page cache removed |
| `mem_limit_bytes` | gauge   | yes     | no      | Memory limit. 0 means no limit. |
| `mem_percent`     | gauge   | yes     | no      | Share of the limit in use. 0 when no limit. |
| `rx_bytes`        | counter | yes     | no      | Bytes received across every interface |
| `tx_bytes`        | counter | yes     | no      | Bytes sent across every interface |
| `read_bytes`      | counter | yes     | no      | Block bytes read           |
| `write_bytes`     | counter | yes     | no      | Block bytes written        |
| `pids`            | gauge   | yes     | no      | Process count in the container |

A stopped container carries no stats fields. The agent does not send zeros for
values it did not read.

Notes on the container source:

- The agent calls `/containers/json?all=true` and
  `/containers/{id}/stats?stream=0&one-shot=1` over the unix socket.
- The agent calls `/containers/{id}/json` for a stopped container only. The
  inspect call gives `RestartCount` and `State.FinishedAt`. A running
  container skips the call, which keeps the cost of a cycle low.
- The agent drops a stopped container that finished more than 1 hour ago. It
  reads the age from `State.FinishedAt`, and from the list `Created` time when
  the runtime reports no finish time.
- The agent inspects at most 50 stopped containers per cycle, newest first. A
  host with a large graveyard of old containers stays cheap.
- The agent inspects a running container every 10 cycles to read its restart
  count. The flag `-inspect-every` sets the number of cycles. Every cycle
  reports the last known count, so the series holds one point per cycle and an
  increase query over any window measures the restarts in that window.
- A container in a restart loop never reaches the stopped list. The rising
  `restart_count` of a running container is the signal for that state.
- The one-shot endpoint returns an empty `precpu_stats` block. The agent keeps
  the previous CPU sample per container and does the delta math itself. The
  first cycle after start reports `cpu_percent=0`.
- The agent drops the kept CPU sample when a container stops.
- On a Docker server below version 25.0 the agent sends one stats request at a
  time. Concurrent stats requests are slow on those versions.
- The memory reading subtracts `inactive_file` (cgroup v2), or
  `total_inactive_file` or `cache` (cgroup v1).
- A memory limit above 1 TiB is the cgroup "no limit" sentinel. The agent
  reports `mem_limit_bytes=0` for it.
- A memory usage above 1 TiB is garbage. The agent drops `mem_used_bytes`,
  `mem_limit_bytes`, and `mem_percent` from that point. It does not send a 0,
  because a 0 would read as an idle container.
- When `system_cpu_usage` does not advance, which happens on some Podman
  builds, the agent computes the CPU percent from wall-clock time.

## Cardinality

Per host, per cycle:

- 5 fixed points: `obs_cpu`, `obs_load`, `obs_mem`, `obs_host`, `obs_agent`.
- 1 point per mount, per block device, per interface, and per container.

The ingest rate depends on the field count, not the point count. This is the
field count of each measurement:

| Measurement          | Fields per point |
| -------------------- | ---------------- |
| `obs_cpu`            | 14               |
| `obs_load`           | 3                |
| `obs_mem`            | 11               |
| `obs_host`           | 3                |
| `obs_agent`          | 3                |
| `obs_disk`           | 8                |
| `obs_diskio`         | 8                |
| `obs_net`            | 8                |
| `obs_docker` running | 13               |
| `obs_docker` stopped | 4                |

### Worked example: a small VPS

One mount, one block device, one interface, three running containers.

| Part                        | Points | Fields          |
| --------------------------- | ------ | --------------- |
| `obs_cpu` + `obs_load` + `obs_mem` + `obs_host` + `obs_agent` | 5 | 14+3+11+3+3 = 34 |
| `obs_disk`                  | 1      | 8               |
| `obs_diskio`                | 1      | 8               |
| `obs_net`                   | 1      | 8               |
| `obs_docker`                | 3      | 3 x 13 = 39     |
| **Total**                   | **11** | **97**          |

97 fields every 15 s is **6.5 samples per second per host**.

### Worked example: a busy host

Three mounts, two block devices, three interfaces, twenty running containers,
two containers that stopped in the last hour.

| Part            | Points | Fields        |
| --------------- | ------ | ------------- |
| Fixed five      | 5      | 34            |
| `obs_disk`      | 3      | 24            |
| `obs_diskio`    | 2      | 16            |
| `obs_net`       | 3      | 24            |
| `obs_docker`    | 22     | 20x13 + 2x4 = 268 |
| **Total**       | **35** | **366**       |

366 fields every 15 s is **24.4 samples per second per host**.

At a 60 s interval both figures divide by four: 1.6 and 6.1 samples per
second.

An earlier version of this file claimed 40 to 100 samples per second for a
small VPS. That figure counted the fields of one cycle and forgot to divide by
the interval. The correct small-VPS figure is 6.5 per second.

### Series count

The series count is what a time series database charges for, and it does not
grow with time. One host produces one series per measurement, per tag set, per
field:

- 34 host-wide series (`obs_cpu` 14, `obs_load` 3, `obs_mem` 11, `obs_host` 3,
  `obs_agent` 3).
- 8 series per mount, per block device, and per interface.
- 13 series per container name.

A redeploy does not add a series, because the container name is the only tag
on `obs_docker`.
