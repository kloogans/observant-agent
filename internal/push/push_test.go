package push

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// newTestPusher returns a Pusher whose backoff sleep is instant.
func newTestPusher(url string) *Pusher {
	p := New(url, "tok")
	p.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return p
}

func TestSendGzipsAndAuthorizes(t *testing.T) {
	var gotBody string
	var gotAuth, gotEnc string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotEnc = r.Header.Get("Content-Encoding")
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip reader: %v", err)
			w.WriteHeader(500)
			return
		}
		b, _ := io.ReadAll(zr)
		gotBody = string(b)
		w.WriteHeader(204)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.Send(context.Background(), []byte("obs_cpu v=1i 1\n"), 1); err != nil {
		t.Fatalf("send: %v", err)
	}
	if gotBody != "obs_cpu v=1i 1\n" {
		t.Fatalf("body = %q", gotBody)
	}
	if gotAuth != "Bearer tok" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if gotEnc != "gzip" {
		t.Fatalf("encoding = %q", gotEnc)
	}
	s := p.Stats()
	if s.Batches != 1 || s.Points != 1 || s.Failures != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestRetryThenSucceed(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) < 3 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.Send(context.Background(), []byte("m v=1i\n"), 1); err != nil {
		t.Fatalf("send: %v", err)
	}
	if n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if s := p.Stats(); s.Failures != 2 || s.Dropped != 0 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestFourFailuresDropAndBuffer(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(500)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.Send(context.Background(), []byte("m v=1i\n"), 1); err == nil {
		t.Fatal("expected an error")
	}
	if n != 4 {
		t.Fatalf("attempts = %d, want 4 (1 try + 3 retries)", n)
	}
	if p.Pending() != 1 {
		t.Fatalf("pending = %d", p.Pending())
	}
	if s := p.Stats(); s.Dropped != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestBufferCapsAtFour(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	for i := 0; i < 7; i++ {
		p.Send(context.Background(), []byte("m v=1i\n"), 1)
	}
	if p.Pending() != MaxBuffered {
		t.Fatalf("pending = %d want %d", p.Pending(), MaxBuffered)
	}
	if s := p.Stats(); s.Dropped != 7 {
		t.Fatalf("dropped = %d want 7", s.Dropped)
	}
}

func TestOldestBufferedGoesFirst(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var order []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(500)
			return
		}
		zr, _ := gzip.NewReader(r.Body)
		b, _ := io.ReadAll(zr)
		order = append(order, string(b))
		w.WriteHeader(204)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	p.Send(context.Background(), []byte("first\n"), 1)
	p.Send(context.Background(), []byte("second\n"), 1)
	if p.Pending() != 2 {
		t.Fatalf("pending = %d", p.Pending())
	}

	fail.Store(false)
	if err := p.Send(context.Background(), []byte("third\n"), 1); err != nil {
		t.Fatalf("send: %v", err)
	}
	want := []string{"first\n", "second\n", "third\n"}
	if len(order) != 3 {
		t.Fatalf("order = %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v want %v", order, want)
		}
	}
	if p.Pending() != 0 {
		t.Fatalf("pending = %d", p.Pending())
	}
	if s := p.Stats(); s.Retried != 2 {
		t.Fatalf("retried = %d want 2", s.Retried)
	}
}

func TestClientErrorNotRetried(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		http.Error(w, "bad token", 401)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	err := p.Send(context.Background(), []byte("m v=1i\n"), 1)
	if err == nil {
		t.Fatal("expected an error")
	}
	if n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

func TestRateLimitIsRetried(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.Send(context.Background(), []byte("m v=1i\n"), 1); err != nil {
		t.Fatalf("send: %v", err)
	}
	if n != 2 {
		t.Fatalf("attempts = %d want 2", n)
	}
}

func TestSendOnceRetriesTwiceAndDoesNotBuffer(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(500)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.SendOnce(context.Background(), []byte("m v=1i\n"), 1); err == nil {
		t.Fatal("expected an error")
	}
	if want := int32(1 + OnceRetries); n != want {
		t.Fatalf("attempts = %d want %d", n, want)
	}
	if p.Pending() != 0 {
		t.Fatalf("pending = %d want 0", p.Pending())
	}
}

func TestSendOnceStopsAfterASuccess(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&n, 1) < 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.SendOnce(context.Background(), []byte("m v=1i\n"), 3); err != nil {
		t.Fatalf("send once: %v", err)
	}
	if n != 2 {
		t.Fatalf("attempts = %d want 2", n)
	}
	if s := p.Stats(); s.Batches != 1 || s.Points != 3 || s.Failures != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestSendOnceDoesNotRetryAClientError(t *testing.T) {
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		http.Error(w, "bad token", 401)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.SendOnce(context.Background(), []byte("m v=1i\n"), 1); err == nil {
		t.Fatal("expected an error")
	}
	if n != 1 {
		t.Fatalf("attempts = %d want 1", n)
	}
}

// The real sleep runs, so this test proves the delay is quick.
func TestSendOnceRetryDelayIsQuick(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	p := New(srv.URL, "tok")
	start := time.Now()
	p.SendOnce(context.Background(), []byte("m v=1i\n"), 1)
	elapsed := time.Since(start)
	if elapsed < OnceRetryDelay*time.Duration(OnceRetries) {
		t.Fatalf("elapsed = %s, the retries did not sleep", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("elapsed = %s, the shutdown push is too slow", elapsed)
	}
}

// The retry ladder plus the attempts must fit inside SendDeadline.
func TestBackoffLadderFitsTheDeadline(t *testing.T) {
	var total time.Duration
	for _, d := range backoff {
		total += d
	}
	if total >= SendDeadline {
		t.Fatalf("backoff sum %s does not fit in %s", total, SendDeadline)
	}
	if total > SendDeadline/2 {
		t.Fatalf("backoff sum %s leaves too little of %s for the requests", total, SendDeadline)
	}
}

// Send must return inside SendDeadline even when the server never answers.
func TestSendHonoursTheDeadline(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(block)

	p := New(srv.URL, "tok")
	start := time.Now()
	if err := p.Send(context.Background(), []byte("m v=1i\n"), 1); err == nil {
		t.Fatal("expected an error")
	}
	elapsed := time.Since(start)
	if elapsed > SendDeadline+2*time.Second {
		t.Fatalf("Send took %s, want at most about %s", elapsed, SendDeadline)
	}
}

// A buffered batch the server rejects for good must leave the buffer.
func TestFlushDropsANonRetryableBatch(t *testing.T) {
	var mode atomic.Int32 // 0 = 500, 1 = 400, 2 = ok
	var seen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			w.WriteHeader(500)
		case 1:
			seen.Add(1)
			http.Error(w, "malformed line", 400)
		default:
			w.WriteHeader(204)
		}
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	p.Send(context.Background(), []byte("bad1\n"), 1)
	p.Send(context.Background(), []byte("bad2\n"), 1)
	if p.Pending() != 2 {
		t.Fatalf("pending = %d want 2", p.Pending())
	}

	// The server now rejects everything for good. The buffer must empty.
	mode.Store(1)
	p.Flush(context.Background())
	if p.Pending() != 0 {
		t.Fatalf("pending = %d want 0 after a permanent rejection", p.Pending())
	}
	if s := p.Stats(); s.RejectedDropped != 2 {
		t.Fatalf("RejectedDropped = %d want 2", s.RejectedDropped)
	}
	if seen.Load() != 2 {
		t.Fatalf("flush attempts = %d want 2", seen.Load())
	}
}

// A retryable failure must keep the buffered batch and stop the flush.
func TestFlushKeepsARetryableBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	p.Send(context.Background(), []byte("a\n"), 1)
	p.Send(context.Background(), []byte("b\n"), 1)
	before := p.Pending()
	p.Flush(context.Background())
	if p.Pending() != before {
		t.Fatalf("pending = %d want %d", p.Pending(), before)
	}
	if s := p.Stats(); s.RejectedDropped != 0 {
		t.Fatalf("RejectedDropped = %d want 0", s.RejectedDropped)
	}
}

func TestEmptyBodyIsNoOp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server must not be called")
	}))
	defer srv.Close()

	p := newTestPusher(srv.URL)
	if err := p.Send(context.Background(), nil, 0); err != nil {
		t.Fatalf("send: %v", err)
	}
}
