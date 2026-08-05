package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jamesobrien/observant/agent/internal/config"
)

func testAgent(t *testing.T) *agent {
	t.Helper()
	return newAgent(&config.Config{
		Hostname: "test-host",
		Interval: 15 * time.Second,
		Docker:   config.DockerOff,
	})
}

func TestAgentPointCarriesVersionAndUp(t *testing.T) {
	a := testAgent(t)
	a.encodeAgent(time.Unix(7, 0), 1)
	got := a.enc.String()
	for _, want := range []string{
		"obs_agent,host=test-host ",
		`version="` + version + `"`,
		"up=1i",
		"start_time=",
		" 7000000000\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %q", want, got)
		}
	}
}

func TestAgentPointStartTimeIsProcessStart(t *testing.T) {
	a := testAgent(t)
	a.encodeAgent(time.Unix(7, 0), 1)
	want := "start_time=" + strconv.FormatInt(startTime.Unix(), 10) + "i"
	if !strings.Contains(a.enc.String(), want) {
		t.Errorf("missing %q in %q", want, a.enc.String())
	}
	if time.Since(startTime) > time.Hour {
		t.Error("startTime is not the process start")
	}
}

func TestShutdownBatchReportsUpZero(t *testing.T) {
	a := testAgent(t)
	a.collectBatch(context.Background(), 0)
	if !strings.Contains(a.enc.String(), "up=0i") {
		t.Errorf("shutdown batch has no up=0i:\n%s", a.enc.String())
	}
	a.collectBatch(context.Background(), 1)
	if !strings.Contains(a.enc.String(), "up=1i") {
		t.Errorf("normal batch has no up=1i:\n%s", a.enc.String())
	}
}

func TestEveryCycleEmitsObsAgent(t *testing.T) {
	a := testAgent(t)
	for i := 0; i < 3; i++ {
		a.collectBatch(context.Background(), 1)
		if n := strings.Count(a.enc.String(), "obs_agent,"); n != 1 {
			t.Fatalf("cycle %d has %d obs_agent lines", i, n)
		}
	}
}

func TestDockerFailureLogIsSuppressed(t *testing.T) {
	a := testAgent(t)
	now := time.Unix(1_000_000, 0)
	if !a.logDockerFailure("docker: broken pipe", now) {
		t.Fatal("the first report must log")
	}
	if a.logDockerFailure("docker: broken pipe", now.Add(time.Minute)) {
		t.Error("a repeat inside the quiet period must not log")
	}
	if a.logDockerFailure("docker: broken pipe", now.Add(dockerLogEvery-time.Second)) {
		t.Error("a repeat just before the quiet period ends must not log")
	}
	if !a.logDockerFailure("docker: broken pipe", now.Add(dockerLogEvery)) {
		t.Error("a repeat after the quiet period must log")
	}
	// A different failure is news. Log it at once.
	if !a.logDockerFailure("docker: no such container", now.Add(dockerLogEvery+time.Second)) {
		t.Error("a new message must log at once")
	}
}

// fakeDockerSocket serves the /version endpoint on a unix socket.
func fakeDockerSocket(t *testing.T) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Skipf("cannot listen on a unix socket: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"Version":"27.1.1","ApiVersion":"1.46"}`))
	})
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: mux}}
	srv.Start()
	t.Cleanup(srv.Close)
	return sock
}

func TestRedetectDockerRetriesEveryTwentyCycles(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.sock")
	a := newAgent(&config.Config{Hostname: "h", Docker: config.DockerAuto, Socket: missing})
	a.startDocker(context.Background())
	if a.dockerErr == nil {
		t.Fatal("detection was supposed to fail")
	}

	// The socket appears. Nothing happens until the 20th cycle.
	a.cfg.Socket = fakeDockerSocket(t)
	for i := 0; i < redetectEvery-1; i++ {
		a.cycles++
		a.redetectDocker(context.Background())
		if a.docker != nil {
			t.Fatalf("redetected too early at cycle %d", a.cycles)
		}
	}
	a.cycles++
	a.redetectDocker(context.Background())
	if a.docker == nil {
		t.Fatalf("no redetection at cycle %d", a.cycles)
	}
	if a.dockerErr != nil {
		t.Errorf("dockerErr = %v, want nil after a success", a.dockerErr)
	}
}

func TestRedetectDockerStaysOffWhenDisabled(t *testing.T) {
	a := newAgent(&config.Config{Hostname: "h", Docker: config.DockerOff, Socket: fakeDockerSocket(t)})
	a.startDocker(context.Background())
	a.dockerErr = context.DeadlineExceeded
	a.cycles = redetectEvery
	a.redetectDocker(context.Background())
	if a.docker != nil {
		t.Error("-docker=off must never open a socket")
	}
}

func TestRedetectDockerDoesNothingWhenAlreadyOpen(t *testing.T) {
	a := newAgent(&config.Config{Hostname: "h", Docker: config.DockerAuto, Socket: fakeDockerSocket(t)})
	a.startDocker(context.Background())
	if a.docker == nil {
		t.Fatal("detection was supposed to work")
	}
	before := a.docker
	a.cycles = redetectEvery
	a.redetectDocker(context.Background())
	if a.docker != before {
		t.Error("an open client must not be replaced")
	}
}

func TestOneLineFlattensJoinedErrors(t *testing.T) {
	err := errors.Join(errors.New("a"), errors.New("b"))
	if got := oneLine(err); got != "a; b" {
		t.Errorf("oneLine = %q want %q", got, "a; b")
	}
}
