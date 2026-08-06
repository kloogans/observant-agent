// Command observant-agent collects host and container metrics, then pushes
// them to the observant.computer ingest endpoint as InfluxDB line protocol.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jamesobrien/observant/agent/internal/collect"
	"github.com/jamesobrien/observant/agent/internal/config"
	"github.com/jamesobrien/observant/agent/internal/docker"
	"github.com/jamesobrien/observant/agent/internal/lineproto"
	"github.com/jamesobrien/observant/agent/internal/push"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const (
	// summaryEvery is the gap between the quiet summary log lines.
	summaryEvery = time.Minute
	// redetectEvery is the number of cycles between two socket detection
	// attempts after the first attempt failed.
	redetectEvery = 20
	// dockerLogEvery is the quiet period after the agent reports a broken
	// container socket. The agent logs the same failure once per period.
	dockerLogEvery = 10 * time.Minute
)

// startTime is the moment the process started. obs_agent reports it so that
// the server can tell a restart from an uninterrupted run.
var startTime = time.Now()

func main() {
	log.SetFlags(0)
	log.SetPrefix("")
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, config.ErrHelp) {
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, "observant-agent: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := config.Parse("observant-agent", args, os.Stderr)
	if err != nil {
		return err
	}
	if cfg.Version {
		fmt.Printf("observant-agent %s %s/%s %s\n", version, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return nil
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	a := newAgent(cfg)
	a.startDocker(ctx)

	switch {
	case cfg.SelfCheck:
		return a.selfCheck(ctx)
	case cfg.Once:
		return a.once(ctx)
	}

	if err := cfg.Validate(); err != nil {
		return err
	}
	return a.loop(ctx)
}

// agent holds the collectors, the encoder, and the pusher.
type agent struct {
	cfg    *config.Config
	sys    *collect.Collector
	docker *docker.Client
	enc    *lineproto.Encoder
	pusher *push.Pusher

	dockerErr error
	// cycles counts the collection cycles since start.
	cycles int
	// dockerLoggedAt is the time of the last container socket error log.
	dockerLoggedAt time.Time
	// dockerLastMsg is the text of the last container socket error. A new
	// text logs at once. The same text stays quiet for dockerLogEvery.
	dockerLastMsg string
}

func newAgent(cfg *config.Config) *agent {
	base := []lineproto.Tag{{Key: "host", Value: cfg.Hostname}}
	if cfg.Role != "" {
		base = append(base, lineproto.Tag{Key: "role", Value: cfg.Role})
	}
	return &agent{
		cfg:    cfg,
		sys:    collect.New(),
		enc:    lineproto.New(base...),
		pusher: push.New(cfg.URL, cfg.Token),
	}
}

// startDocker opens the container socket when the mode allows it.
func (a *agent) startDocker(ctx context.Context) {
	if a.cfg.Docker == config.DockerOff {
		return
	}
	c, err := docker.Detect(ctx, a.cfg.Socket)
	if err != nil {
		a.dockerErr = err
		return
	}
	c.SetInspectEvery(a.cfg.InspectEvery)
	a.docker = c
}

// redetectDocker retries the socket detection when the first attempt failed.
// redetectDocker runs once every redetectEvery cycles and stays silent.
func (a *agent) redetectDocker(ctx context.Context) {
	if a.docker != nil || a.cfg.Docker == config.DockerOff || a.dockerErr == nil {
		return
	}
	if a.cycles == 0 || a.cycles%redetectEvery != 0 {
		return
	}
	c, err := docker.Detect(ctx, a.cfg.Socket)
	if err != nil {
		a.dockerErr = err
		return
	}
	c.SetInspectEvery(a.cfg.InspectEvery)
	a.docker = c
	a.dockerErr = nil
	a.dockerLastMsg = ""
	log.Printf("containers: socket found at %s (%s)", c.Socket(), c.Runtime())
}

// logDockerFailure reports a container socket failure at most once per
// dockerLogEvery. A new message logs at once.
func (a *agent) logDockerFailure(msg string, now time.Time) bool {
	if msg == a.dockerLastMsg && now.Sub(a.dockerLoggedAt) < dockerLogEvery {
		return false
	}
	a.dockerLastMsg = msg
	a.dockerLoggedAt = now
	return true
}

// collectBatch fills the encoder with one cycle of points.
// collectBatch returns the non-fatal collector errors.
// up is 1 for a normal cycle and 0 for the final cycle of a clean stop.
func (a *agent) collectBatch(ctx context.Context, up int64) []error {
	a.enc.Reset()
	snap := a.sys.Collect(ctx)
	snap.Encode(a.enc)
	a.encodeAgent(snap.Time, up)
	errs := snap.Errs

	if a.docker != nil {
		stats, err := a.docker.Collect(ctx)
		docker.Encode(a.enc, stats, snap.Time)
		if err != nil {
			errs = append(errs, fmt.Errorf("docker: %w", err))
		}
	}
	return errs
}

// encodeAgent writes the obs_agent heartbeat point.
//
// up=1 marks a live agent. The shutdown path writes up=0 so that the server
// can tell a clean stop from a host that vanished.
func (a *agent) encodeAgent(ts time.Time, up int64) {
	a.enc.Point("obs_agent", nil, []lineproto.Field{
		lineproto.S("version", version),
		lineproto.I("start_time", startTime.Unix()),
		lineproto.I("up", up),
	}, ts)
}

// prime takes the first sample of every delta counter.
func (a *agent) prime(ctx context.Context) {
	a.sys.Prime(ctx)
	if a.docker != nil {
		a.docker.Collect(ctx) //nolint:errcheck // priming only
	}
}

// once collects one cycle and prints the line protocol.
func (a *agent) once(ctx context.Context) error {
	a.prime(ctx)
	if err := sleep(ctx, 300*time.Millisecond); err != nil {
		return nil
	}
	errs := a.collectBatch(ctx, 1)
	os.Stdout.Write(a.enc.Bytes())
	for _, e := range errs {
		fmt.Fprintln(os.Stderr, "warning: "+e.Error())
	}
	if a.dockerErr != nil && a.cfg.Docker == config.DockerOn {
		return fmt.Errorf("docker: %w", a.dockerErr)
	}
	return nil
}

// loop runs the collect and push cycle until the context ends.
func (a *agent) loop(ctx context.Context) error {
	// A jittered start keeps a fleet of agents from pushing in lockstep.
	jitter := time.Duration(rand.Int64N(int64(a.cfg.Interval)))
	log.Printf("observant-agent %s started: host=%s interval=%s jitter=%s docker=%s",
		version, a.cfg.Hostname, a.cfg.Interval, jitter.Round(time.Millisecond), a.dockerState())

	a.prime(ctx)
	if err := sleep(ctx, jitter); err != nil {
		return a.shutdown(context.WithoutCancel(ctx))
	}

	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()

	lastSummary := time.Now()
	for {
		a.cycle(ctx)
		if time.Since(lastSummary) >= summaryEvery {
			a.logSummary()
			lastSummary = time.Now()
		}
		select {
		case <-ctx.Done():
			return a.shutdown(context.WithoutCancel(ctx))
		case <-t.C:
		}
	}
}

// cycle collects one batch and pushes it.
func (a *agent) cycle(ctx context.Context) {
	a.redetectDocker(ctx)
	a.cycles++

	errs := a.collectBatch(ctx, 1)
	for _, e := range errs {
		msg := oneLine(e)
		if strings.HasPrefix(msg, "docker:") {
			// A dead socket repeats every cycle. Log it, then stay quiet.
			if a.logDockerFailure(msg, time.Now()) {
				log.Printf("collect: %s (further reports are suppressed for %s)", msg, dockerLogEvery)
			}
			continue
		}
		// A collector error is rare. Log it once per occurrence.
		log.Printf("collect: %s", msg)
	}
	if a.enc.Len() == 0 {
		return
	}
	if err := a.pusher.Send(ctx, a.enc.Bytes(), a.enc.Points()); err != nil {
		log.Printf("push failed: %s (batch dropped, %d buffered)", oneLine(err), a.pusher.Pending())
	}
}

// oneLine flattens a joined error so that one failure makes one log line.
func oneLine(err error) string {
	return strings.ReplaceAll(strings.TrimSpace(err.Error()), "\n", "; ")
}

// shutdown makes one final push attempt, then returns.
// The final batch carries obs_agent up=0, which marks a clean stop.
func (a *agent) shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, push.RequestTimeout+2*time.Second)
	defer cancel()

	a.pusher.Flush(ctx)
	a.collectBatch(ctx, 0)
	if a.enc.Len() > 0 {
		if err := a.pusher.SendOnce(ctx, a.enc.Bytes(), a.enc.Points()); err != nil {
			log.Printf("final push failed: %s", oneLine(err))
		}
	}
	s := a.pusher.Stats()
	log.Printf("stopped: batches=%d points=%d dropped=%d", s.Batches, s.Points, s.Dropped)
	return nil
}

func (a *agent) logSummary() {
	s := a.pusher.Stats()
	msg := fmt.Sprintf("summary: batches=%d points=%d bytes=%d failures=%d dropped=%d rejected=%d buffered=%d",
		s.Batches, s.Points, s.Bytes, s.Failures, s.Dropped, s.RejectedDropped, s.Buffered)
	if s.LastError != "" && (s.Failures > 0 || s.Buffered > 0) {
		msg += " last_error=" + strconv.Quote(s.LastError)
	}
	log.Print(msg)
}

func (a *agent) dockerState() string {
	switch {
	case a.cfg.Docker == config.DockerOff:
		return "off"
	case a.docker != nil:
		return a.docker.Runtime() + " via " + a.docker.Socket()
	default:
		return "none"
	}
}

// selfCheck runs every collector and prints what it can see.
func (a *agent) selfCheck(ctx context.Context) error {
	out := os.Stdout
	fmt.Fprintf(out, "observant-agent %s %s/%s\n", version, runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(out, "host tag:  %s\n", a.cfg.Hostname)
	if a.cfg.Role != "" {
		fmt.Fprintf(out, "role tag:  %s\n", a.cfg.Role)
	}
	fmt.Fprintf(out, "interval:  %s\n", a.cfg.Interval)
	fmt.Fprintf(out, "ingest:    %s\n", orNone(a.cfg.URL))
	fmt.Fprintf(out, "token:     %s\n", tokenState(a.cfg.Token))
	fmt.Fprintln(out)

	a.prime(ctx)
	if err := sleep(ctx, 300*time.Millisecond); err != nil {
		return nil
	}
	snap := a.sys.Collect(ctx)

	fail := 0
	if v := snap.CPU; v != nil {
		fmt.Fprintf(out, "CPU        %d cores, %.2f%% busy (user %.2f%%, sys %.2f%%, iowait %.2f%%, steal %.2f%%)\n",
			v.Cores, v.UsagePercent, v.UserPercent, v.SystemPercent, v.IowaitPercent, v.StealPercent)
	} else {
		fail++
		fmt.Fprintln(out, "CPU        FAILED")
	}
	if v := snap.Load; v != nil {
		fmt.Fprintf(out, "load       %.2f %.2f %.2f\n", v.Load1, v.Load5, v.Load15)
	} else {
		fail++
		fmt.Fprintln(out, "load       FAILED")
	}
	if v := snap.Mem; v != nil {
		fmt.Fprintf(out, "memory     %s used of %s (%.1f%%), available %s\n",
			human(v.Used), human(v.Total), v.UsedPercent, human(v.Available))
		if v.SwapTotal > 0 {
			fmt.Fprintf(out, "swap       %s used of %s (%.1f%%)\n",
				human(v.SwapUsed), human(v.SwapTotal), v.SwapUsedPercent)
		} else {
			fmt.Fprintln(out, "swap       none")
		}
	} else {
		fail++
		fmt.Fprintln(out, "memory     FAILED")
	}

	fmt.Fprintf(out, "disks      %d mounts\n", len(snap.Disks))
	for _, d := range snap.Disks {
		fmt.Fprintf(out, "           %-24s %-10s %s used of %s (%.1f%%)\n",
			d.Mount, d.Fstype, human(d.Used), human(d.Total), d.UsedPercent)
	}
	fmt.Fprintf(out, "disk i/o   %d devices\n", len(snap.DiskIO))
	for _, d := range snap.DiskIO {
		fmt.Fprintf(out, "           %-24s read %s, write %s\n", d.Device, human(d.ReadBytes), human(d.WriteBytes))
	}
	fmt.Fprintf(out, "network    %d interfaces\n", len(snap.Nets))
	for _, n := range snap.Nets {
		fmt.Fprintf(out, "           %-24s rx %s, tx %s\n", n.Interface, human(n.RxBytes), human(n.TxBytes))
	}
	if v := snap.Host; v != nil {
		fmt.Fprintf(out, "host       %s %s, kernel %s, arch %s\n", v.Platform, v.PlatformVersion, v.Kernel, v.Arch)
		fmt.Fprintf(out, "uptime     %s (booted %s)\n",
			(time.Duration(v.Uptime) * time.Second).String(),
			time.Unix(int64(v.BootTime), 0).Format(time.RFC3339))
	} else {
		fail++
		fmt.Fprintln(out, "host       FAILED")
	}

	fmt.Fprintln(out)
	switch {
	case a.cfg.Docker == config.DockerOff:
		fmt.Fprintln(out, "containers disabled by -docker=off")
	case a.docker == nil:
		fmt.Fprintf(out, "containers no socket found (%v)\n", a.dockerErr)
		if a.cfg.Docker == config.DockerOn {
			fail++
		}
	default:
		fmt.Fprintf(out, "containers %s at %s\n", a.docker.Runtime(), a.docker.Socket())
		stats, err := a.docker.Collect(ctx)
		if err != nil {
			fmt.Fprintf(out, "           partial read: %v\n", err)
		}
		live := 0
		for _, s := range stats {
			if s.Running {
				live++
			}
		}
		fmt.Fprintf(out, "           %d running, %d recently stopped\n", live, len(stats)-live)
		for _, s := range stats {
			if !s.Running {
				fmt.Fprintf(out, "           %-24s stopped, %d restarts, image %s\n",
					s.Name, s.RestartCount, s.Image)
				continue
			}
			restarts := ""
			if s.HasRestartCount {
				restarts = fmt.Sprintf("%d restarts, ", s.RestartCount)
			}
			fmt.Fprintf(out, "           %-24s cpu %.2f%%, mem %s, %simage %s\n",
				s.Name, s.CPUPercent, human(s.MemUsed), restarts, s.Image)
		}
	}

	for _, e := range snap.Errs {
		fmt.Fprintf(out, "\nwarning: %v\n", e)
	}

	a.enc.Reset()
	snap.Encode(a.enc)
	a.encodeAgent(snap.Time, 1)
	fmt.Fprintf(out, "\nbatch      %d points, %d bytes uncompressed\n", a.enc.Points(), a.enc.Len())

	if fail > 0 {
		return fmt.Errorf("%d collector(s) failed", fail)
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func human(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit && exp < 4; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTP"[exp])
}

func orNone(s string) string {
	if s == "" {
		return "(not set)"
	}
	return s
}

func tokenState(s string) string {
	if s == "" {
		return "(not set)"
	}
	return fmt.Sprintf("set, %d chars", len(strings.TrimSpace(s)))
}
