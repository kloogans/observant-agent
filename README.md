# observant-agent

`observant-agent` is the monitoring agent of [observant.computer](https://observant.computer),
a hosted observability service for small and ephemeral server fleets. This
repository is public and MIT-licensed. The hosted service itself, its
server, and its dashboard are not open source; they live in a private
repository.

## What it does

The agent is one static Go binary, `CGO_ENABLED=0`, about 6 MB resident. It
runs as a systemd service under its own `observant` user. On each collection
cycle it reads:

- Host metrics through [gopsutil](https://github.com/shirou/gopsutil):
  CPU usage, memory, disk usage, load average, and network counters.
- Container metrics from the Docker socket, when present: which containers
  run, and their image references.

It writes every point as InfluxDB line protocol to the observant.computer
API over HTTPS, using an ingest token scoped to write only. It needs no
inbound port, so a NAT'd machine or a short-lived build agent works the same
as a long-running server. See [METRICS.md](METRICS.md) for the full metric
and tag reference.

## Install

```sh
OBSERVANT_URL=https://observant.computer OBSERVANT_TOKEN=<your-ingest-token> \
  sh install.sh /path/to/observant-agent
```

The installer must run as root. It creates the `observant` system user,
installs the binary to `/usr/local/bin/observant-agent`, writes
`/etc/observant/agent.env`, and starts a systemd service. If a Docker socket
is present, it adds `observant` to the `docker` group so the agent can read
container stats; set `OBSERVANT_DOCKER=off` before installing to skip that
step, since docker group membership is equivalent to root on that host.

## Build

```sh
make build      # cross-compiles linux/amd64, linux/arm64, darwin/arm64 into dist/
make dev        # builds a binary for the current OS/arch into dist/
make check      # gofmt, go vet, go test
```

## License

MIT. See [LICENSE](LICENSE).
