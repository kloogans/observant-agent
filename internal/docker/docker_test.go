package docker

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamesobrien/observant/agent/internal/lineproto"
)

// fakeSocket starts an HTTP server on a unix socket and returns its path.
func fakeSocket(t *testing.T, h http.Handler) string {
	t.Helper()
	// A unix socket path has a length limit, so keep it short.
	dir := t.TempDir()
	sock := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Skipf("cannot listen on a unix socket: %v", err)
	}
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: h}}
	srv.Start()
	t.Cleanup(srv.Close)
	return sock
}

type fakeStats struct {
	total  uint64
	system uint64
	cores  uint64
	memUse uint64
	memLim uint64
	memCch uint64
}

// handler serves the three endpoints the agent uses.
func handler(t *testing.T, version string, stats *atomic.Pointer[fakeStats], calls *atomic.Int32) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"Version": version, "ApiVersion": "1.44"})
	})
	mux.HandleFunc("/containers/json", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("all") != "true" {
			t.Errorf("list query = %q, want all=true", r.URL.RawQuery)
		}
		json.NewEncoder(w).Encode([]map[string]any{{
			"Id":      "abcdef0123456789",
			"Names":   []string{"/web"},
			"Image":   "nginx:1",
			"State":   "running",
			"Status":  "Up 3 minutes",
			"Created": time.Now().Unix(),
		}})
	})
	mux.HandleFunc("/containers/abcdef0123456789/stats", func(w http.ResponseWriter, r *http.Request) {
		if calls != nil {
			calls.Add(1)
		}
		if r.URL.Query().Get("one-shot") != "1" || r.URL.Query().Get("stream") != "0" {
			t.Errorf("bad query: %s", r.URL.RawQuery)
		}
		s := stats.Load()
		json.NewEncoder(w).Encode(map[string]any{
			"cpu_stats": map[string]any{
				"cpu_usage":        map[string]any{"total_usage": s.total},
				"system_cpu_usage": s.system,
				"online_cpus":      s.cores,
			},
			"precpu_stats": map[string]any{"cpu_usage": map[string]any{"total_usage": 0}},
			"memory_stats": map[string]any{
				"usage": s.memUse,
				"limit": s.memLim,
				"stats": map[string]uint64{"inactive_file": s.memCch},
			},
			"networks":    map[string]any{"eth0": map[string]any{"rx_bytes": 100, "tx_bytes": 200}},
			"blkio_stats": map[string]any{"io_service_bytes_recursive": []map[string]any{{"op": "read", "value": 10}, {"op": "write", "value": 20}}},
			"pids_stats":  map[string]any{"current": 7},
		})
	})
	return mux
}

func TestDetectAndCollect(t *testing.T) {
	var s atomic.Pointer[fakeStats]
	s.Store(&fakeStats{total: 1_000_000_000, system: 100_000_000_000, cores: 4, memUse: 200 << 20, memLim: 512 << 20, memCch: 50 << 20})
	sock := fakeSocket(t, handler(t, "24.0.7", &s, nil))

	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatalf("detect: %v", err)
	}
	if c.Socket() != sock {
		t.Errorf("socket = %q", c.Socket())
	}
	if !strings.HasPrefix(c.Runtime(), "docker 24.0.7") {
		t.Errorf("runtime = %q", c.Runtime())
	}
	if c.batch != 1 {
		t.Errorf("batch = %d, want 1 on docker below 25", c.batch)
	}

	// The first read has no previous sample, so the CPU percent is 0.
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("containers = %d", len(got))
	}
	if got[0].CPUPercent != 0 {
		t.Errorf("first cpu = %v, want 0", got[0].CPUPercent)
	}
	if got[0].Name != "web" || got[0].Image != "nginx:1" {
		t.Errorf("identity = %+v", got[0].Container)
	}
	// 200 MiB usage minus 50 MiB inactive_file = 150 MiB.
	if got[0].MemUsed != 150<<20 {
		t.Errorf("mem = %d want %d", got[0].MemUsed, 150<<20)
	}
	if got[0].MemPercent < 29 || got[0].MemPercent > 30 {
		t.Errorf("mem percent = %v want about 29.3", got[0].MemPercent)
	}
	if got[0].NetRxBytes != 100 || got[0].NetTxBytes != 200 {
		t.Errorf("net = %d/%d", got[0].NetRxBytes, got[0].NetTxBytes)
	}
	if got[0].BlkReadByte != 10 || got[0].BlkWriteByt != 20 {
		t.Errorf("blkio = %d/%d", got[0].BlkReadByte, got[0].BlkWriteByt)
	}
	if got[0].Pids != 7 {
		t.Errorf("pids = %d", got[0].Pids)
	}

	// The second read has a previous sample: 2 of 100 system units on
	// 4 cores is 8%.
	s.Store(&fakeStats{total: 3_000_000_000, system: 200_000_000_000, cores: 4, memUse: 200 << 20, memLim: 512 << 20})
	got, err = c.Collect(context.Background())
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if got[0].CPUPercent != 8 {
		t.Errorf("cpu = %v want 8", got[0].CPUPercent)
	}
}

func TestBatchSizeOnModernDocker(t *testing.T) {
	var s atomic.Pointer[fakeStats]
	s.Store(&fakeStats{cores: 1, memLim: 1 << 20})
	sock := fakeSocket(t, handler(t, "27.1.1", &s, nil))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	if c.batch != 5 {
		t.Errorf("batch = %d want 5", c.batch)
	}
}

func TestAbsurdMemoryIsCapped(t *testing.T) {
	var s atomic.Pointer[fakeStats]
	// The cgroup "no limit" sentinel and a garbage usage value.
	s.Store(&fakeStats{cores: 1, memUse: 1 << 62, memLim: 9223372036854771712})
	sock := fakeSocket(t, handler(t, "27.1.1", &s, nil))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].MemLimit != 0 {
		t.Errorf("limit = %d want 0", got[0].MemLimit)
	}
	// An absurd usage is dropped, not reported as 0. A 0 would read as an
	// idle container.
	if got[0].MemUsedOK {
		t.Error("MemUsedOK = true, want false for a garbage usage value")
	}
	out := lineproto.New()
	Encode(out, got, time.Unix(5, 0))
	for _, gone := range []string{"mem_used_bytes", "mem_limit_bytes", "mem_percent"} {
		if strings.Contains(out.String(), gone) {
			t.Errorf("field %s must be absent: %s", gone, out.String())
		}
	}
	if !strings.Contains(out.String(), "cpu_percent=") {
		t.Errorf("cpu field must survive: %s", out.String())
	}
}

func TestPodmanFallbackUsesWallClock(t *testing.T) {
	var s atomic.Pointer[fakeStats]
	// system_cpu_usage stays at 0, which is the Podman case.
	s.Store(&fakeStats{cores: 2, total: 0, memLim: 1 << 20})
	sock := fakeSocket(t, handler(t, "27.1.1", &s, nil))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	// 60 ms of CPU time over about 120 ms of wall time is about 50%.
	s.Store(&fakeStats{cores: 2, total: 60_000_000, memLim: 1 << 20})
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got[0].CPUPercent <= 0 || got[0].CPUPercent > 200 {
		t.Errorf("cpu = %v, want a plausible percent", got[0].CPUPercent)
	}
}

func TestPruneDeadDropsOldSamples(t *testing.T) {
	c := newClient("/dev/null")
	c.prev["gone"] = cpuSample{total: 1}
	c.prev["alive"] = cpuSample{total: 2}
	c.restart["gone"] = restartSample{count: 1, have: true}
	c.restart["alive"] = restartSample{count: 2, have: true}
	c.pruneDead([]Container{{ID: "alive", Running: true}, {ID: "gone", Running: false}})
	if _, ok := c.prev["gone"]; ok {
		t.Error("dead container sample kept")
	}
	if _, ok := c.prev["alive"]; !ok {
		t.Error("live container sample dropped")
	}
	if _, ok := c.restart["gone"]; ok {
		t.Error("dead container restart count kept")
	}
	if _, ok := c.restart["alive"]; !ok {
		t.Error("live container restart count dropped")
	}
}

func TestDetectNoSocket(t *testing.T) {
	_, err := Detect(context.Background(), filepath.Join(t.TempDir(), "missing.sock"))
	if err != ErrNoSocket {
		t.Fatalf("err = %v want ErrNoSocket", err)
	}
}

func TestEncode(t *testing.T) {
	e := lineproto.New(lineproto.Tag{Key: "host", Value: "h"})
	Encode(e, []Stats{{
		Container:  Container{ID: "abcdef0123456789", Name: "web", Image: "nginx:1", Running: true},
		CPUPercent: 12.5, MemUsed: 100, MemLimit: 200, MemPercent: 50, MemUsedOK: true,
	}}, time.Unix(5, 0))
	out := e.String()
	for _, want := range []string{
		"obs_docker,", "container=web", `container_id="abcdef012345"`, `image="nginx:1"`,
		"host=h", "running=1i", "cpu_percent=12.5", "mem_used_bytes=100i", "mem_percent=50", " 5000000000\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %s", want, out)
		}
	}
}

// The tag set decides the series identity. A redeploy changes the image and
// the container ID, so neither may be a tag.
func TestIdentityTagsAreNameOnly(t *testing.T) {
	line := func(id, image string) string {
		e := lineproto.New(lineproto.Tag{Key: "host", Value: "h"})
		Encode(e, []Stats{{
			Container: Container{ID: id, Name: "web", Image: image, Running: true},
			MemUsedOK: true,
		}}, time.Unix(5, 0))
		return e.String()
	}
	a := line("aaaaaaaaaaaa1111", "nginx:1")
	b := line("bbbbbbbbbbbb2222", "nginx:2")

	tagsOf := func(s string) string {
		return strings.SplitN(s, " ", 2)[0]
	}
	if tagsOf(a) != tagsOf(b) {
		t.Errorf("series identity changed on redeploy: %q vs %q", tagsOf(a), tagsOf(b))
	}
	if tagsOf(a) != "obs_docker,container=web,host=h" {
		t.Errorf("tags = %q", tagsOf(a))
	}
	for _, bad := range []string{"container_id=abc", ",image=nginx"} {
		if strings.Contains(tagsOf(a), bad) {
			t.Errorf("tag set still holds %q: %s", bad, tagsOf(a))
		}
	}
}

func TestEncodeStoppedContainer(t *testing.T) {
	e := lineproto.New()
	Encode(e, []Stats{{
		Container:       Container{ID: "abcdef0123456789", Name: "job", Image: "busybox:1"},
		RestartCount:    3,
		HasRestartCount: true,
	}}, time.Unix(5, 0))
	out := e.String()
	for _, want := range []string{"running=0i", "restart_count=3i", `container_id="abcdef012345"`} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in %s", want, out)
		}
	}
	// A stopped container has no stats. Do not fake them with zeros.
	for _, gone := range []string{"cpu_percent", "mem_used_bytes", "rx_bytes", "pids"} {
		if strings.Contains(out, gone) {
			t.Errorf("field %s must be absent for a stopped container: %s", gone, out)
		}
	}
}

// allHandler serves a list of the given containers plus inspect and stats.
func allHandler(t *testing.T, list []map[string]any, inspect map[string]any, statsCalls, inspectCalls *atomic.Int32) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"Version": "27.1.1"})
	})
	mux.HandleFunc("/containers/json", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(list)
	})
	mux.HandleFunc("/containers/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/stats"):
			if statsCalls != nil {
				statsCalls.Add(1)
			}
			json.NewEncoder(w).Encode(map[string]any{
				"cpu_stats":    map[string]any{"cpu_usage": map[string]any{"total_usage": 1}, "online_cpus": 1},
				"memory_stats": map[string]any{"usage": 1 << 20, "limit": 1 << 30},
				"pids_stats":   map[string]any{"current": 1},
			})
		case strings.HasSuffix(r.URL.Path, "/json"):
			if inspectCalls != nil {
				inspectCalls.Add(1)
			}
			json.NewEncoder(w).Encode(inspect)
		default:
			w.WriteHeader(404)
		}
	})
	return mux
}

func TestStoppedContainerGetsRestartCount(t *testing.T) {
	now := time.Now()
	list := []map[string]any{
		{"Id": "run1", "Names": []string{"/web"}, "Image": "nginx:1", "State": "running", "Created": now.Unix()},
		{"Id": "dead1", "Names": []string{"/job"}, "Image": "busybox:1", "State": "exited", "Created": now.Add(-30 * time.Minute).Unix()},
	}
	inspect := map[string]any{
		"RestartCount": 4,
		"State":        map[string]any{"Running": false, "FinishedAt": now.Add(-5 * time.Minute).Format(time.RFC3339Nano)},
	}
	var statsCalls, inspectCalls atomic.Int32
	sock := fakeSocket(t, allHandler(t, list, inspect, &statsCalls, &inspectCalls))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("samples = %d want 2: %+v", len(got), got)
	}
	byName := map[string]Stats{}
	for _, s := range got {
		byName[s.Name] = s
	}
	if !byName["web"].Running {
		t.Error("web must be running")
	}
	// A container the client never saw gets one inspect call at once, so the
	// first push already carries the restart count.
	if !byName["web"].HasRestartCount || byName["web"].RestartCount != 4 {
		t.Errorf("web = %+v", byName["web"])
	}
	if byName["job"].Running || byName["job"].RestartCount != 4 {
		t.Errorf("job = %+v", byName["job"])
	}
	if statsCalls.Load() != 1 {
		t.Errorf("stats calls = %d want 1", statsCalls.Load())
	}
	// One inspect call for the running container and one for the stopped one.
	if inspectCalls.Load() != 2 {
		t.Errorf("inspect calls = %d want 2", inspectCalls.Load())
	}
}

// A container that crashes and restarts again and again never reaches the
// stopped list. Its restart count is the only signal of the loop.
func TestRunningContainerCarriesTheRestartCount(t *testing.T) {
	list := []map[string]any{
		{"Id": "run1", "Names": []string{"/web"}, "Image": "nginx:1", "State": "running", "Created": time.Now().Unix()},
	}
	inspect := map[string]any{"RestartCount": 7, "State": map[string]any{"Running": true}}
	var inspectCalls atomic.Int32
	sock := fakeSocket(t, allHandler(t, list, inspect, nil, &inspectCalls))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	c.SetInspectEvery(5)

	const cycles = 12
	for i := 0; i < cycles; i++ {
		got, err := c.Collect(context.Background())
		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if len(got) != 1 {
			t.Fatalf("cycle %d: samples = %d", i, len(got))
		}
		// Every cycle reports the count, also the cycles that make no
		// inspect call. A sparse series would break an increase query.
		if !got[0].HasRestartCount || got[0].RestartCount != 7 {
			t.Fatalf("cycle %d: restart count = %+v", i, got[0])
		}
	}
	// One call on the first sight, then one call per five cycles.
	if n := inspectCalls.Load(); n < 2 || n > 4 {
		t.Errorf("inspect calls = %d over %d cycles, want about %d", n, cycles, 1+cycles/5)
	}
}

// The refresh of a big host must not land on one cycle.
func TestInspectLoadIsSpreadOverCycles(t *testing.T) {
	const containers = 40
	var list []map[string]any
	for i := 0; i < containers; i++ {
		list = append(list, map[string]any{
			"Id":    "c" + strconv.Itoa(i),
			"Names": []string{"/app" + strconv.Itoa(i)},
			"State": "running",
		})
	}
	inspect := map[string]any{"RestartCount": 0, "State": map[string]any{"Running": true}}
	var inspectCalls atomic.Int32
	sock := fakeSocket(t, allHandler(t, list, inspect, nil, &inspectCalls))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	c.SetInspectEvery(10)

	// The first cycle inspects every container one time.
	if _, err := c.Collect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := inspectCalls.Load(); n != containers {
		t.Fatalf("first cycle inspect calls = %d want %d", n, containers)
	}
	worst := int32(0)
	for i := 0; i < 20; i++ {
		inspectCalls.Store(0)
		if _, err := c.Collect(context.Background()); err != nil {
			t.Fatal(err)
		}
		if n := inspectCalls.Load(); n > worst {
			worst = n
		}
	}
	// 40 containers over 10 cycles is 4 per cycle on average. Allow slack
	// for the hash, but a burst of 40 must not happen.
	if worst > 12 {
		t.Errorf("worst cycle = %d inspect calls, want a spread load", worst)
	}
	if worst == 0 {
		t.Error("no refresh happened at all")
	}
}

func TestOldStoppedContainerIsSkipped(t *testing.T) {
	now := time.Now()
	list := []map[string]any{
		{"Id": "dead1", "Names": []string{"/old"}, "Image": "busybox:1", "State": "exited", "Created": now.Add(-48 * time.Hour).Unix()},
	}
	inspect := map[string]any{
		"RestartCount": 1,
		"State":        map[string]any{"Running": false, "FinishedAt": now.Add(-24 * time.Hour).Format(time.RFC3339Nano)},
	}
	sock := fakeSocket(t, allHandler(t, list, inspect, nil, nil))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatalf("an old container is not an error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("samples = %d want 0: %+v", len(got), got)
	}
}

func TestStoppedInspectIsCapped(t *testing.T) {
	now := time.Now()
	var list []map[string]any
	for i := 0; i < maxStoppedInspect+20; i++ {
		list = append(list, map[string]any{
			"Id":      "dead" + strconv.Itoa(i),
			"Names":   []string{"/j" + strconv.Itoa(i)},
			"State":   "exited",
			"Created": now.Add(-time.Duration(i) * time.Second).Unix(),
		})
	}
	inspect := map[string]any{
		"RestartCount": 0,
		"State":        map[string]any{"Running": false, "FinishedAt": now.Format(time.RFC3339Nano)},
	}
	var inspectCalls atomic.Int32
	sock := fakeSocket(t, allHandler(t, list, inspect, nil, &inspectCalls))
	c, err := Detect(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != maxStoppedInspect {
		t.Errorf("samples = %d want %d", len(got), maxStoppedInspect)
	}
	if n := inspectCalls.Load(); n != maxStoppedInspect {
		t.Errorf("inspect calls = %d want %d", n, maxStoppedInspect)
	}
	// The newest stopped container must survive the cap.
	if got[0].Name != "j0" {
		t.Errorf("first kept = %q want j0", got[0].Name)
	}
}

func TestStoppedAgeFallsBackToCreated(t *testing.T) {
	now := time.Unix(10_000, 0)
	if d, ok := stoppedAge("", 9_000, now); !ok || d != 1000*time.Second {
		t.Errorf("created fallback = %v %v", d, ok)
	}
	if d, ok := stoppedAge("0001-01-01T00:00:00Z", 9_000, now); !ok || d != 1000*time.Second {
		t.Errorf("zero finish time must fall back: %v %v", d, ok)
	}
	if _, ok := stoppedAge("", 0, now); ok {
		t.Error("no time source must report ok=false")
	}
	if d, ok := stoppedAge(time.Unix(9_500, 0).UTC().Format(time.RFC3339Nano), 9_000, now); !ok || d != 500*time.Second {
		t.Errorf("finish time = %v %v", d, ok)
	}
}
