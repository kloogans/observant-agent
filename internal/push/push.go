// Package push sends line protocol batches to the ingest endpoint.
//
// The Pusher gzips the body, retries with backoff, and keeps a small buffer
// of dropped batches for a later attempt. One Pusher serves one goroutine.
package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Defaults for the transport.
const (
	// RequestTimeout bounds one HTTP attempt.
	RequestTimeout = 10 * time.Second
	// MaxBuffered is the number of dropped batches kept for a retry.
	MaxBuffered = 4
	// SendDeadline bounds the whole of one Send call, the buffered flush and
	// every retry together. It stays below the smallest useful collection
	// interval so that one bad cycle cannot delay the next one.
	SendDeadline = 12 * time.Second
	// OnceRetries is the number of extra attempts SendOnce makes.
	OnceRetries = 2
	// OnceRetryDelay is the gap between the SendOnce attempts.
	OnceRetryDelay = 500 * time.Millisecond
)

// backoff holds the delay before each retry after the first attempt.
// The sum is 3.5 s, which leaves room for four attempts inside SendDeadline.
var backoff = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}

// Stats counts the work of the Pusher since start.
type Stats struct {
	Batches  int64
	Points   int64
	Bytes    int64
	Failures int64
	Dropped  int64
	Retried  int64
	// RejectedDropped counts the buffered batches thrown away because the
	// server rejected them for good, for example with a 400 or a 401.
	RejectedDropped int64
	Buffered        int
	LastError       string
}

// Pusher posts batches to the ingest endpoint.
type Pusher struct {
	url    string
	token  string
	client *http.Client
	sleep  func(context.Context, time.Duration) error

	pending []batch
	stats   Stats
	gz      *gzip.Writer
	buf     bytes.Buffer
}

type batch struct {
	body   []byte
	points int
	at     time.Time
}

// New makes a Pusher for the endpoint and the token.
func New(url, token string) *Pusher {
	return &Pusher{
		url:   url,
		token: token,
		client: &http.Client{
			Timeout: RequestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        4,
				MaxIdleConnsPerHost: 4,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
			},
		},
		sleep: sleepCtx,
	}
}

// Stats returns a copy of the counters.
func (p *Pusher) Stats() Stats {
	s := p.stats
	s.Buffered = len(p.pending)
	return s
}

// Send delivers one batch.
//
// Send first retries the buffered batches, oldest first, with one attempt
// each. Send then posts the new batch with the backoff schedule. Send buffers
// the new batch and counts a drop when every attempt fails.
//
// SendDeadline bounds the flush and the retries together.
func (p *Pusher) Send(ctx context.Context, body []byte, points int) error {
	if len(body) == 0 && len(p.pending) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, SendDeadline)
	defer cancel()

	p.flushPending(ctx)
	if len(body) == 0 {
		return nil
	}

	cp := append([]byte(nil), body...)
	err := p.sendWithRetry(ctx, cp)
	if err == nil {
		p.stats.Batches++
		p.stats.Points += int64(points)
		p.stats.Bytes += int64(len(cp))
		return nil
	}

	p.stats.Dropped++
	p.stats.LastError = err.Error()
	p.buffer(batch{body: cp, points: points, at: time.Now()})
	return err
}

// SendOnce delivers one batch with up to OnceRetries extra quick attempts and
// no buffering. The shutdown path uses it for the final push.
// SendOnce stops early when the server rejects the batch for good.
func (p *Pusher) SendOnce(ctx context.Context, body []byte, points int) error {
	var last error
	for attempt := 0; attempt <= OnceRetries; attempt++ {
		err := p.post(ctx, body)
		if err == nil {
			p.stats.Batches++
			p.stats.Points += int64(points)
			p.stats.Bytes += int64(len(body))
			return nil
		}
		last = err
		p.stats.Failures++
		p.stats.LastError = err.Error()
		if attempt == OnceRetries || !retryable(err) {
			break
		}
		if err := p.sleep(ctx, OnceRetryDelay); err != nil {
			return errors.Join(last, err)
		}
	}
	return last
}

// Pending returns the number of buffered batches.
func (p *Pusher) Pending() int { return len(p.pending) }

// Flush tries every buffered batch one time, oldest first.
func (p *Pusher) Flush(ctx context.Context) { p.flushPending(ctx) }

// flushPending tries every buffered batch one time, oldest first.
// flushPending stops at the first retryable failure so that the new batch
// keeps its share of the deadline. It throws away a batch that the server
// rejected for good, because another attempt will fail the same way.
func (p *Pusher) flushPending(ctx context.Context) {
	for len(p.pending) > 0 {
		if ctx.Err() != nil {
			return
		}
		b := p.pending[0]
		if err := p.post(ctx, b.body); err != nil {
			p.stats.Failures++
			p.stats.LastError = err.Error()
			if !retryable(err) {
				p.pending = p.pending[1:]
				p.stats.RejectedDropped++
				continue
			}
			return
		}
		p.pending = p.pending[1:]
		p.stats.Batches++
		p.stats.Points += int64(b.points)
		p.stats.Bytes += int64(len(b.body))
		p.stats.Retried++
	}
}

func (p *Pusher) sendWithRetry(ctx context.Context, body []byte) error {
	var last error
	for attempt := 0; ; attempt++ {
		err := p.post(ctx, body)
		if err == nil {
			return nil
		}
		last = err
		p.stats.Failures++
		p.stats.LastError = err.Error()
		if attempt >= len(backoff) || !retryable(err) {
			return last
		}
		if err := p.sleep(ctx, backoff[attempt]); err != nil {
			return errors.Join(last, err)
		}
	}
}

// buffer keeps a dropped batch. The oldest batch leaves when the buffer is
// full.
func (p *Pusher) buffer(b batch) {
	p.pending = append(p.pending, b)
	for len(p.pending) > MaxBuffered {
		p.pending = p.pending[1:]
	}
}

func (p *Pusher) post(ctx context.Context, body []byte) error {
	if len(body) == 0 {
		return nil
	}
	p.buf.Reset()
	if p.gz == nil {
		p.gz = gzip.NewWriter(&p.buf)
	} else {
		p.gz.Reset(&p.buf)
	}
	if _, err := p.gz.Write(body); err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	if err := p.gz.Close(); err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	payload := append([]byte(nil), p.buf.Bytes()...)

	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, p.url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.token)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("Content-Encoding", "gzip")
	req.ContentLength = int64(len(payload))

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &httpError{Code: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
}

type httpError struct {
	Code int
	Body string
}

func (e *httpError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("http %d", e.Code)
	}
	return fmt.Sprintf("http %d: %s", e.Code, e.Body)
}

// retryable reports whether another attempt can succeed.
// A 4xx answer other than 408 and 429 means the request itself is wrong.
func retryable(err error) bool {
	var he *httpError
	if errors.As(err, &he) {
		if he.Code == http.StatusRequestTimeout || he.Code == http.StatusTooManyRequests {
			return true
		}
		return he.Code < 400 || he.Code >= 500
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
