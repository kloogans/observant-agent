// Package config parses the agent flags and the environment fallbacks.
//
// Every flag has an environment variable fallback. The flag wins when the
// caller sets it. The precedence is: flag, then environment, then default.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jamesobrien/observant/agent/internal/docker"
)

// DockerMode selects the container collector behaviour.
type DockerMode string

const (
	// DockerAuto probes the known sockets and stays quiet when none exists.
	DockerAuto DockerMode = "auto"
	// DockerOn requires a socket and reports an error when none exists.
	DockerOn DockerMode = "on"
	// DockerOff disables the container collector.
	DockerOff DockerMode = "off"
)

// Config holds the runtime settings of the agent.
type Config struct {
	URL      string
	Token    string
	Interval time.Duration
	Hostname string
	Role     string
	Docker   DockerMode
	Socket   string
	// InspectEvery is the number of cycles between two inspect calls of the
	// same running container. The inspect call reads the restart count.
	InspectEvery int

	Version   bool
	Once      bool
	SelfCheck bool
}

// ErrHelp reports that the caller asked for the usage text.
var ErrHelp = flag.ErrHelp

// Parse reads the arguments and the environment into a Config.
// Parse does not validate the URL or the token. Call Validate for that.
func Parse(name string, args []string, out io.Writer) (*Config, error) {
	c := &Config{}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out)

	var interval string
	var dockerMode string

	fs.StringVar(&c.URL, "url", env("OBSERVANT_URL", ""),
		"ingest endpoint URL (env OBSERVANT_URL)")
	fs.StringVar(&c.Token, "token", env("OBSERVANT_TOKEN", ""),
		"account token sent as a bearer token (env OBSERVANT_TOKEN)")
	fs.StringVar(&interval, "interval", env("OBSERVANT_INTERVAL", "15s"),
		"collection interval, for example 30s (env OBSERVANT_INTERVAL)")
	fs.StringVar(&c.Hostname, "hostname", env("OBSERVANT_HOSTNAME", ""),
		"host tag value, defaults to the system hostname (env OBSERVANT_HOSTNAME)")
	fs.StringVar(&c.Role, "role", env("OBSERVANT_ROLE", ""),
		"optional role tag, for example builder (env OBSERVANT_ROLE)")
	fs.StringVar(&dockerMode, "docker", env("OBSERVANT_DOCKER", "auto"),
		"container collector: on, off, or auto (env OBSERVANT_DOCKER)")
	fs.StringVar(&c.Socket, "socket", env("OBSERVANT_SOCKET", ""),
		"container socket path, defaults to auto-detection (env OBSERVANT_SOCKET)")
	inspectEvery, err := envInt("OBSERVANT_INSPECT_EVERY", docker.DefaultInspectEvery)
	if err != nil {
		return nil, err
	}
	fs.IntVar(&c.InspectEvery, "inspect-every", inspectEvery,
		"cycles between two restart-count reads of the same running container (env OBSERVANT_INSPECT_EVERY)")

	fs.BoolVar(&c.Version, "version", false, "print the version and exit")
	fs.BoolVar(&c.Once, "once", false, "collect one cycle, print the line protocol, exit")
	fs.BoolVar(&c.SelfCheck, "selfcheck", false, "test every collector, print what it sees, exit")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	d, err := time.ParseDuration(strings.TrimSpace(interval))
	if err != nil {
		return nil, fmt.Errorf("bad interval %q: %w", interval, err)
	}
	if d < time.Second {
		return nil, fmt.Errorf("interval %s is below the 1s minimum", d)
	}
	c.Interval = d

	if c.InspectEvery < 1 {
		return nil, fmt.Errorf("bad inspect-every %d: want 1 or more", c.InspectEvery)
	}

	switch DockerMode(strings.ToLower(strings.TrimSpace(dockerMode))) {
	case DockerAuto, "":
		c.Docker = DockerAuto
	case DockerOn, "true", "1", "yes":
		c.Docker = DockerOn
	case DockerOff, "false", "0", "no":
		c.Docker = DockerOff
	default:
		return nil, fmt.Errorf("bad docker mode %q: want on, off, or auto", dockerMode)
	}

	// Trim first. A whitespace-only value is the same as no value.
	c.Hostname = strings.TrimSpace(c.Hostname)
	if c.Hostname == "" {
		h := strings.TrimSpace(hostname())
		if h == "" {
			h = "unknown"
		}
		c.Hostname = h
	}
	c.Role = strings.TrimSpace(c.Role)

	return c, nil
}

// Validate checks the settings the push loop needs.
func (c *Config) Validate() error {
	var errs []error
	if c.URL == "" {
		errs = append(errs, errors.New("no ingest URL: set -url or OBSERVANT_URL"))
	} else {
		u, err := url.Parse(c.URL)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("bad ingest URL %q: %w", c.URL, err))
		case u.Scheme != "http" && u.Scheme != "https":
			errs = append(errs, fmt.Errorf("ingest URL %q needs the http or https scheme", c.URL))
		case u.Host == "":
			errs = append(errs, fmt.Errorf("ingest URL %q has no host", c.URL))
		}
	}
	if c.Token == "" {
		errs = append(errs, errors.New("no token: set -token or OBSERVANT_TOKEN"))
	}
	return errors.Join(errs...)
}

// hostname reads the system hostname. A test replaces it.
var hostname = func() string {
	h, err := os.Hostname()
	if err != nil {
		return ""
	}
	return h
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// envInt reads an integer environment variable.
// envInt reports an error when the value is not an integer.
func envInt(key string, def int) (int, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("bad %s %q: want an integer", key, v)
	}
	return n, nil
}
