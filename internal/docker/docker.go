// Package docker reads container stats over the Docker or Podman unix socket.
//
// The package speaks raw HTTP to the socket. It does not use the Docker SDK.
// The one-shot stats endpoint returns an empty precpu block, so the Client
// keeps the previous CPU sample per container and does the delta math itself.
package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jamesobrien/observant/agent/internal/lineproto"
)

// maxPlausibleMemory caps a garbage memory reading. No container on a VPS
// fleet uses more than 1 TiB, and the cgroup "no limit" value is far larger.
const maxPlausibleMemory = 1 << 40

// maxStoppedAge drops a stopped container that nobody looks at any more.
const maxStoppedAge = time.Hour

// maxStoppedInspect bounds the inspect calls of one cycle. A host with a
// large graveyard of old containers must not slow the cycle down.
const maxStoppedInspect = 50

// DefaultInspectEvery is the number of cycles between two inspect calls of the
// same running container. The inspect call reads the restart count, which is
// the only way to see a container that restarts again and again.
const DefaultInspectEvery = 10

// ErrNoSocket reports that no container socket exists on this machine.
var ErrNoSocket = errors.New("no container socket found")

// Container holds the identity of one container.
type Container struct {
	ID     string
	Name   string
	Image  string
	State  string
	Status string
	// Created is the creation time as a unix timestamp in seconds.
	Created int64
	// Running reports whether the runtime state is "running".
	Running bool
}

// Stats holds one container sample.
// A stopped container carries the identity, Running, and RestartCount only.
type Stats struct {
	Container
	CPUPercent  float64
	MemUsed     uint64
	MemLimit    uint64
	MemPercent  float64
	NetRxBytes  uint64
	NetTxBytes  uint64
	BlkReadByte uint64
	BlkWriteByt uint64
	Pids        uint64
	// Cumulative CPU nanoseconds, taken straight from the runtime.
	CPUTotalNanos uint64
	// MemUsedOK is false when the runtime reported a garbage memory usage.
	// The encoder then drops the memory fields instead of writing a 0.
	MemUsedOK bool
	// RestartCount is valid only when HasRestartCount is true. The agent
	// inspects a stopped container on every cycle and a running container on
	// every inspectEvery cycle, so a running container carries the count of
	// the last inspect call.
	RestartCount    int64
	HasRestartCount bool
}

// cpuSample is the previous CPU reading of one container.
type cpuSample struct {
	total  uint64
	system uint64
	seen   time.Time
}

// restartSample is the last restart count read for a running container.
// The client reports the cached count on every cycle, so the series holds a
// point per cycle and a server query can measure the increase over any window.
type restartSample struct {
	count int64
	// cycle is the collection cycle of the last inspect call.
	cycle int64
	// have is false while no inspect call ever returned a count.
	have bool
}

// Client reads container stats from one socket.
type Client struct {
	http    *http.Client
	socket  string
	runtime string
	// batch limits concurrent stats calls. Docker below 25.0 serves
	// concurrent stats calls slowly, so the client drops to one at a time.
	batch int
	// inspectEvery is the cycle gap between two inspect calls of the same
	// running container.
	inspectEvery int

	mu sync.Mutex
	// cycle counts the Collect calls since start.
	cycle   int64
	prev    map[string]cpuSample
	restart map[string]restartSample
}

// candidateSockets lists the sockets the agent probes, in order.
func candidateSockets() []string {
	var out []string
	if h := os.Getenv("DOCKER_HOST"); strings.HasPrefix(h, "unix://") {
		out = append(out, strings.TrimPrefix(h, "unix://"))
	}
	out = append(out,
		"/var/run/docker.sock",
		"/run/docker.sock",
		"/run/podman/podman.sock",
		"/var/run/podman/podman.sock",
	)
	if x := os.Getenv("XDG_RUNTIME_DIR"); x != "" {
		out = append(out,
			filepath.Join(x, "podman", "podman.sock"),
			filepath.Join(x, "docker.sock"),
		)
	}
	return out
}

// Detect opens the first usable socket.
// A non-empty socket argument forces that path.
// Detect returns ErrNoSocket when no socket answers.
func Detect(ctx context.Context, socket string) (*Client, error) {
	paths := candidateSockets()
	if socket != "" {
		paths = []string{socket}
	}
	for _, p := range paths {
		if fi, err := os.Stat(p); err != nil || fi.Mode()&os.ModeSocket == 0 {
			continue
		}
		c := newClient(p)
		if err := c.probe(ctx); err != nil {
			continue
		}
		return c, nil
	}
	return nil, ErrNoSocket
}

func newClient(socket string) *Client {
	d := &net.Dialer{Timeout: 2 * time.Second}
	return &Client{
		socket:       socket,
		batch:        5,
		inspectEvery: DefaultInspectEvery,
		prev:         map[string]cpuSample{},
		restart:      map[string]restartSample{},
		http: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return d.DialContext(ctx, "unix", socket)
				},
				DisableCompression:  true,
				MaxIdleConns:        8,
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     60 * time.Second,
			},
		},
	}
}

// SetInspectEvery sets the cycle gap between two inspect calls of the same
// running container. A value below 1 becomes 1, which inspects every cycle.
func (c *Client) SetInspectEvery(n int) {
	if n < 1 {
		n = 1
	}
	c.inspectEvery = n
}

// Socket returns the socket path in use.
func (c *Client) Socket() string { return c.socket }

// Runtime returns the runtime name and version string.
func (c *Client) Runtime() string { return c.runtime }

type versionResponse struct {
	Version    string `json:"Version"`
	APIVersion string `json:"ApiVersion"`
	Components []struct {
		Name string `json:"Name"`
	} `json:"Components"`
}

// probe reads /version and sets the batch size for the server version.
func (c *Client) probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var v versionResponse
	if err := c.getJSON(ctx, "/version", &v); err != nil {
		return err
	}
	name := "docker"
	for _, comp := range v.Components {
		if strings.Contains(strings.ToLower(comp.Name), "podman") {
			name = "podman"
		}
	}
	c.runtime = name + " " + v.Version
	if name == "docker" && majorVersion(v.Version) > 0 && majorVersion(v.Version) < 25 {
		// Docker below 25.0 has a slow concurrent stats path. Serialize.
		c.batch = 1
	}
	return nil
}

func majorVersion(s string) int {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		i = len(s)
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil {
		return 0
	}
	return n
}

type containerJSON struct {
	ID      string   `json:"Id"`
	Names   []string `json:"Names"`
	Image   string   `json:"Image"`
	State   string   `json:"State"`
	Status  string   `json:"Status"`
	Created int64    `json:"Created"`
}

// inspectJSON mirrors the fields the agent needs from the inspect endpoint.
type inspectJSON struct {
	RestartCount int64 `json:"RestartCount"`
	State        struct {
		Running    bool   `json:"Running"`
		FinishedAt string `json:"FinishedAt"`
	} `json:"State"`
}

// Containers lists every container, running or not.
func (c *Client) Containers(ctx context.Context) ([]Container, error) {
	var raw []containerJSON
	if err := c.getJSON(ctx, "/containers/json?all=true", &raw); err != nil {
		return nil, err
	}
	out := make([]Container, 0, len(raw))
	for _, r := range raw {
		name := ""
		if len(r.Names) > 0 {
			name = strings.TrimPrefix(r.Names[0], "/")
		}
		if name == "" {
			name = shortID(r.ID)
		}
		out = append(out, Container{
			ID:      r.ID,
			Name:    name,
			Image:   r.Image,
			State:   r.State,
			Status:  r.Status,
			Created: r.Created,
			Running: strings.EqualFold(r.State, "running"),
		})
	}
	return out, nil
}

// statsJSON mirrors the fields the agent needs from the stats endpoint.
type statsJSON struct {
	CPUStats struct {
		CPUUsage struct {
			TotalUsage  uint64   `json:"total_usage"`
			PercpuUsage []uint64 `json:"percpu_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     uint64 `json:"online_cpus"`
	} `json:"cpu_stats"`
	MemoryStats struct {
		Usage uint64            `json:"usage"`
		Limit uint64            `json:"limit"`
		Stats map[string]uint64 `json:"stats"`
	} `json:"memory_stats"`
	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`
	BlkioStats struct {
		IoServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`
	PidsStats struct {
		Current uint64 `json:"current"`
	} `json:"pids_stats"`
}

// Collect reads the stats of every running container and the restart count of
// every recently stopped container.
//
// Collect also inspects a running container every inspectEvery cycles to read
// its restart count. A container that restarts again and again never reaches
// the stopped list, so the count of a running container is the only signal of
// a restart loop.
//
// Collect returns the samples it managed to read and joins any errors.
func (c *Client) Collect(ctx context.Context) ([]Stats, error) {
	containers, err := c.Containers(ctx)
	if err != nil {
		return nil, err
	}
	c.pruneDead(containers)
	if len(containers) == 0 {
		return nil, nil
	}

	var running, stopped []Container
	for _, ct := range containers {
		if ct.Running {
			running = append(running, ct)
			continue
		}
		stopped = append(stopped, ct)
	}
	stopped = recentFirst(stopped)
	if len(stopped) > maxStoppedInspect {
		stopped = stopped[:maxStoppedInspect]
	}
	cycle, inspect := c.plan(running)

	out := make([]Stats, len(running)+len(stopped))
	errs := make([]error, len(out))
	// A stats read of a running container is a stats call. A stopped
	// container needs one inspect call. Both share the concurrency limit.
	sem := make(chan struct{}, c.batch)
	var wg sync.WaitGroup
	for i, ct := range running {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s, err := c.stats(ctx, ct)
			if err != nil {
				errs[i] = fmt.Errorf("%s: %w", ct.Name, err)
				return
			}
			if inspect[i] {
				n, err := c.restartCount(ctx, ct.ID)
				// An inspect failure must not drop the stats sample. The
				// client keeps the previous count and tries again later.
				c.putRestart(ct.ID, n, err == nil, cycle)
			}
			if n, ok := c.getRestart(ct.ID); ok {
				s.RestartCount = n
				s.HasRestartCount = true
			}
			out[i] = s
		}()
	}
	for j, ct := range stopped {
		i := len(running) + j
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s, keep, err := c.stopped(ctx, ct)
			if err != nil {
				errs[i] = fmt.Errorf("%s: %w", ct.Name, err)
				return
			}
			if !keep {
				errs[i] = errSkip
				return
			}
			out[i] = s
		}()
	}
	wg.Wait()

	final := out[:0]
	var joined []error
	for i := range out {
		switch {
		case errs[i] == nil:
			final = append(final, out[i])
		case errors.Is(errs[i], errSkip):
			// An old stopped container. Not an error.
		default:
			joined = append(joined, errs[i])
		}
	}
	return final, errors.Join(joined...)
}

// errSkip marks a container the collector chose not to report.
var errSkip = errors.New("skipped")

// plan starts a cycle and reports which running containers need an inspect
// call. plan returns the cycle number and one flag per running container.
func (c *Client) plan(running []Container) (int64, []bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cycle++
	inspect := make([]bool, len(running))
	for i, ct := range running {
		inspect[i] = c.needsInspect(ct.ID, c.cycle)
	}
	return c.cycle, inspect
}

// needsInspect reports whether the running container needs an inspect call on
// this cycle. The caller holds the mutex.
func (c *Client) needsInspect(id string, cycle int64) bool {
	r, ok := c.restart[id]
	if !ok {
		// A container the client never inspected. Read the count at once so
		// that the first push of a new container carries it.
		return true
	}
	every := int64(c.inspectEvery)
	if every < 1 {
		every = 1
	}
	// The slot spreads the refresh over the cycles. Every container keeps its
	// own slot, so a 40-container host makes about 4 inspect calls per cycle
	// instead of 40 calls on one cycle in ten.
	return cycle-r.cycle >= every && (cycle+slot(id, every))%every == 0
}

// slot maps a container ID to a stable cycle offset in the range [0, every).
func slot(id string, every int64) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64() % uint64(every))
}

// putRestart records the result of an inspect call of a running container.
// A failed call keeps the previous count and moves the cycle stamp only, so
// one bad call does not blank the series.
func (c *Client) putRestart(id string, count int64, ok bool, cycle int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.restart[id]
	r.cycle = cycle
	if ok {
		r.count = count
		r.have = true
	}
	c.restart[id] = r
}

// getRestart returns the last known restart count of a running container.
func (c *Client) getRestart(id string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.restart[id]
	return r.count, ok && r.have
}

// restartCount reads the restart count of one container.
func (c *Client) restartCount(ctx context.Context, id string) (int64, error) {
	var raw inspectJSON
	if err := c.getJSON(ctx, "/containers/"+id+"/json", &raw); err != nil {
		return 0, err
	}
	return raw.RestartCount, nil
}

// recentFirst orders the containers by creation time, newest first.
func recentFirst(in []Container) []Container {
	sort.Slice(in, func(i, j int) bool { return in[i].Created > in[j].Created })
	return in
}

// stopped reads the restart count of one stopped container.
// stopped reports keep=false when the container stopped too long ago.
func (c *Client) stopped(ctx context.Context, ct Container) (Stats, bool, error) {
	var raw inspectJSON
	if err := c.getJSON(ctx, "/containers/"+ct.ID+"/json", &raw); err != nil {
		return Stats{}, false, err
	}
	if age, ok := stoppedAge(raw.State.FinishedAt, ct.Created, time.Now()); ok && age > maxStoppedAge {
		return Stats{}, false, nil
	}
	s := Stats{Container: ct}
	s.Running = raw.State.Running
	s.RestartCount = raw.RestartCount
	s.HasRestartCount = true
	return s, true, nil
}

// stoppedAge returns how long ago the container stopped.
// stoppedAge prefers the inspect FinishedAt time. It falls back to the
// creation time when the runtime reports no usable finish time, which is the
// case for a container that never started.
func stoppedAge(finishedAt string, created int64, now time.Time) (time.Duration, bool) {
	if t, err := time.Parse(time.RFC3339Nano, finishedAt); err == nil && t.Unix() > 0 {
		return now.Sub(t), true
	}
	if created > 0 {
		return now.Sub(time.Unix(created, 0)), true
	}
	return 0, false
}

func (c *Client) stats(ctx context.Context, ct Container) (Stats, error) {
	var raw statsJSON
	path := "/containers/" + ct.ID + "/stats?stream=0&one-shot=1"
	if err := c.getJSON(ctx, path, &raw); err != nil {
		return Stats{}, err
	}

	s := Stats{Container: ct}
	s.Running = true
	s.MemUsedOK = true
	s.CPUTotalNanos = raw.CPUStats.CPUUsage.TotalUsage
	s.Pids = raw.PidsStats.Current

	// The one-shot endpoint zeroes precpu_stats, so keep our own sample.
	cores := raw.CPUStats.OnlineCPUs
	if cores == 0 {
		cores = uint64(len(raw.CPUStats.CPUUsage.PercpuUsage))
	}
	if cores == 0 {
		cores = 1
	}
	now := cpuSample{
		total:  raw.CPUStats.CPUUsage.TotalUsage,
		system: raw.CPUStats.SystemCPUUsage,
		seen:   time.Now(),
	}
	c.mu.Lock()
	prev, had := c.prev[ct.ID]
	c.prev[ct.ID] = now
	c.mu.Unlock()

	if had && now.total >= prev.total {
		switch {
		case now.system > prev.system:
			// The Docker path: system_cpu_usage is host CPU nanoseconds
			// across every core.
			cpuDelta := float64(now.total - prev.total)
			sysDelta := float64(now.system - prev.system)
			s.CPUPercent = clampPercent(cpuDelta/sysDelta*float64(cores)*100, cores)
		case !prev.seen.IsZero():
			// The Podman path: system_cpu_usage can be absent. Fall back
			// to wall-clock time.
			wall := now.seen.Sub(prev.seen).Seconds()
			if wall > 0 {
				used := float64(now.total-prev.total) / 1e9
				s.CPUPercent = clampPercent(used/wall*100, cores)
			}
		}
	}

	// Memory: subtract the reclaimable page cache. The key differs between
	// cgroup v2 (inactive_file) and cgroup v1 (total_inactive_file, cache).
	used := raw.MemoryStats.Usage
	for _, key := range []string{"inactive_file", "total_inactive_file", "cache"} {
		if v, ok := raw.MemoryStats.Stats[key]; ok {
			if v <= used {
				used -= v
			}
			break
		}
	}
	limit := raw.MemoryStats.Limit
	if limit > maxPlausibleMemory {
		// A container with no memory limit reports a garbage limit.
		limit = 0
	}
	if used > maxPlausibleMemory {
		// The reading is garbage. Report no memory value at all. A 0 would
		// read as an idle container and hide the truth.
		used = 0
		s.MemUsedOK = false
	}
	if s.MemUsedOK && limit > 0 && used > limit {
		used = limit
	}
	s.MemUsed = used
	s.MemLimit = limit
	if s.MemUsedOK && limit > 0 {
		s.MemPercent = math.Round(float64(used)/float64(limit)*10000) / 100
	}

	for _, n := range raw.Networks {
		s.NetRxBytes += n.RxBytes
		s.NetTxBytes += n.TxBytes
	}
	for _, b := range raw.BlkioStats.IoServiceBytesRecursive {
		switch strings.ToLower(b.Op) {
		case "read":
			s.BlkReadByte += b.Value
		case "write":
			s.BlkWriteByt += b.Value
		}
	}
	return s, nil
}

// pruneDead drops the CPU samples and the restart counts of containers that
// no longer run.
func (c *Client) pruneDead(live []Container) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.prev) == 0 && len(c.restart) == 0 {
		return
	}
	keep := make(map[string]bool, len(live))
	for _, ct := range live {
		if ct.Running {
			keep[ct.ID] = true
		}
	}
	for id := range c.prev {
		if !keep[id] {
			delete(c.prev, id)
		}
	}
	for id := range c.restart {
		if !keep[id] {
			delete(c.restart, id)
		}
	}
}

func clampPercent(v float64, cores uint64) float64 {
	max := float64(cores) * 100
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	if v > max {
		return max
	}
	return math.Round(v*100) / 100
}

func (c *Client) getJSON(ctx context.Context, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix"+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: http %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(v)
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// Encode writes the container samples into the encoder.
//
// The container name is the only tag. The container ID and the image are
// string fields. An image tag changes on every redeploy, so a tag would start
// a new series on every redeploy and grow the cardinality without limit.
func Encode(e *lineproto.Encoder, stats []Stats, ts time.Time) {
	for _, s := range stats {
		tags := []lineproto.Tag{{Key: "container", Value: s.Name}}
		fields := []lineproto.Field{
			lineproto.S("container_id", shortID(s.ID)),
			lineproto.S("image", s.Image),
			lineproto.I("running", boolInt(s.Running)),
		}
		if s.HasRestartCount {
			fields = append(fields, lineproto.I("restart_count", s.RestartCount))
		}
		if s.Running {
			fields = append(fields,
				lineproto.F("cpu_percent", s.CPUPercent),
				lineproto.U("cpu_total_nanos", s.CPUTotalNanos),
			)
			if s.MemUsedOK {
				fields = append(fields,
					lineproto.U("mem_used_bytes", s.MemUsed),
					lineproto.U("mem_limit_bytes", s.MemLimit),
					lineproto.F("mem_percent", s.MemPercent),
				)
			}
			fields = append(fields,
				lineproto.U("rx_bytes", s.NetRxBytes),
				lineproto.U("tx_bytes", s.NetTxBytes),
				lineproto.U("read_bytes", s.BlkReadByte),
				lineproto.U("write_bytes", s.BlkWriteByt),
				lineproto.U("pids", s.Pids),
			)
		}
		e.Point("obs_docker", tags, fields, ts)
	}
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
