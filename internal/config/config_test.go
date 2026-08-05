package config

import (
	"io"
	"strings"
	"testing"
	"time"
)

func parse(t *testing.T, envs map[string]string, args ...string) (*Config, error) {
	t.Helper()
	for _, k := range []string{"OBSERVANT_URL", "OBSERVANT_TOKEN", "OBSERVANT_INTERVAL",
		"OBSERVANT_HOSTNAME", "OBSERVANT_ROLE", "OBSERVANT_DOCKER", "OBSERVANT_SOCKET"} {
		t.Setenv(k, "")
	}
	for k, v := range envs {
		t.Setenv(k, v)
	}
	return Parse("test", args, io.Discard)
}

func TestDefaults(t *testing.T) {
	c, err := parse(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Interval != 15*time.Second {
		t.Errorf("interval = %s want 15s", c.Interval)
	}
	if c.Docker != DockerAuto {
		t.Errorf("docker = %q want auto", c.Docker)
	}
	if c.Hostname == "" {
		t.Error("hostname must default to the system hostname")
	}
}

func TestEnvFallback(t *testing.T) {
	c, err := parse(t, map[string]string{
		"OBSERVANT_URL":      "https://ingest.example/write",
		"OBSERVANT_TOKEN":    "abc",
		"OBSERVANT_INTERVAL": "30s",
		"OBSERVANT_HOSTNAME": "builder-3",
		"OBSERVANT_ROLE":     "builder",
		"OBSERVANT_DOCKER":   "off",
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.URL != "https://ingest.example/write" || c.Token != "abc" {
		t.Errorf("url/token = %q %q", c.URL, c.Token)
	}
	if c.Interval != 30*time.Second {
		t.Errorf("interval = %s", c.Interval)
	}
	if c.Hostname != "builder-3" || c.Role != "builder" {
		t.Errorf("hostname/role = %q %q", c.Hostname, c.Role)
	}
	if c.Docker != DockerOff {
		t.Errorf("docker = %q", c.Docker)
	}
}

func TestFlagBeatsEnv(t *testing.T) {
	c, err := parse(t, map[string]string{"OBSERVANT_HOSTNAME": "from-env"}, "-hostname", "from-flag")
	if err != nil {
		t.Fatal(err)
	}
	if c.Hostname != "from-flag" {
		t.Errorf("hostname = %q", c.Hostname)
	}
}

func TestBlankHostnameFallsBackToTheSystemName(t *testing.T) {
	old := hostname
	t.Cleanup(func() { hostname = old })
	hostname = func() string { return "sys-host" }

	for _, in := range []string{"", " ", "\t", "   \n  "} {
		c, err := parse(t, map[string]string{"OBSERVANT_HOSTNAME": in})
		if err != nil {
			t.Fatalf("env %q: %v", in, err)
		}
		if c.Hostname != "sys-host" {
			t.Errorf("env %q gave hostname %q, want sys-host", in, c.Hostname)
		}
		c, err = parse(t, nil, "-hostname", in)
		if err != nil {
			t.Fatalf("flag %q: %v", in, err)
		}
		if c.Hostname != "sys-host" {
			t.Errorf("flag %q gave hostname %q, want sys-host", in, c.Hostname)
		}
	}
}

func TestHostnameIsTrimmed(t *testing.T) {
	c, err := parse(t, nil, "-hostname", "  web-1  ")
	if err != nil {
		t.Fatal(err)
	}
	if c.Hostname != "web-1" {
		t.Errorf("hostname = %q want web-1", c.Hostname)
	}
	c, err = parse(t, nil, "-role", "  builder ")
	if err != nil {
		t.Fatal(err)
	}
	if c.Role != "builder" {
		t.Errorf("role = %q want builder", c.Role)
	}
}

func TestUnknownHostnameFallback(t *testing.T) {
	old := hostname
	t.Cleanup(func() { hostname = old })
	hostname = func() string { return "  " }
	c, err := parse(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Hostname != "unknown" {
		t.Errorf("hostname = %q want unknown", c.Hostname)
	}
}

func TestDockerModeAliases(t *testing.T) {
	cases := map[string]DockerMode{
		"on": DockerOn, "true": DockerOn, "1": DockerOn,
		"off": DockerOff, "false": DockerOff, "0": DockerOff,
		"auto": DockerAuto, "AUTO": DockerAuto,
	}
	for in, want := range cases {
		c, err := parse(t, nil, "-docker", in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if c.Docker != want {
			t.Errorf("docker %q = %q want %q", in, c.Docker, want)
		}
	}
	if _, err := parse(t, nil, "-docker", "maybe"); err == nil {
		t.Error("expected an error for an unknown docker mode")
	}
}

func TestBadInterval(t *testing.T) {
	if _, err := parse(t, nil, "-interval", "nope"); err == nil {
		t.Error("expected an error")
	}
	if _, err := parse(t, nil, "-interval", "100ms"); err == nil {
		t.Error("expected the minimum interval error")
	}
}

func TestUnexpectedArgument(t *testing.T) {
	if _, err := parse(t, nil, "start"); err == nil {
		t.Error("expected an error")
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"missing both", Config{}, "no ingest URL"},
		{"missing token", Config{URL: "https://a/b"}, "no token"},
		{"bad scheme", Config{URL: "ftp://a/b", Token: "t"}, "http or https"},
		{"no host", Config{URL: "https://", Token: "t"}, "no host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	ok := Config{URL: "https://ingest.example/write", Token: "t"}
	if err := ok.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

func TestMissingURLMessageIsActionable(t *testing.T) {
	err := (&Config{}).Validate()
	for _, want := range []string{"-url", "OBSERVANT_URL", "-token", "OBSERVANT_TOKEN"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message does not name %s: %v", want, err)
		}
	}
}
