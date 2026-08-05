package collect

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jamesobrien/observant/agent/internal/lineproto"
	"github.com/shirou/gopsutil/v4/cpu"
)

func TestSkipFstype(t *testing.T) {
	skip := []string{"tmpfs", "devtmpfs", "sysfs", "proc", "overlay", "squashfs", "cgroup2", "TMPFS", "", "  "}
	keep := []string{"ext4", "xfs", "btrfs", "apfs", "zfs", "vfat", "nfs4", "hfs"}
	for _, f := range skip {
		if !SkipFstype(f) {
			t.Errorf("SkipFstype(%q) = false, want true", f)
		}
	}
	for _, f := range keep {
		if SkipFstype(f) {
			t.Errorf("SkipFstype(%q) = true, want false", f)
		}
	}
}

func TestSkipMount(t *testing.T) {
	skip := []string{"/proc", "/sys/fs/cgroup", "/dev/shm", "/run/user/1000", "/var/lib/docker/overlay2/x", "/snap/core/1", ""}
	keep := []string{"/", "/home", "/mnt/data", "/var", "/var/lib", "/devel", "/systemd"}
	for _, m := range skip {
		if !SkipMount(m) {
			t.Errorf("SkipMount(%q) = false, want true", m)
		}
	}
	for _, m := range keep {
		if SkipMount(m) {
			t.Errorf("SkipMount(%q) = true, want false", m)
		}
	}
}

func TestSkipBlockDevice(t *testing.T) {
	skip := []string{"loop0", "ram3", "zram0", "sr0", "dm-1", ""}
	keep := []string{"sda", "sda1", "nvme0n1", "vda", "xvda", "disk0", "md0"}
	for _, d := range skip {
		if !SkipBlockDevice(d) {
			t.Errorf("SkipBlockDevice(%q) = false, want true", d)
		}
	}
	for _, d := range keep {
		if SkipBlockDevice(d) {
			t.Errorf("SkipBlockDevice(%q) = true, want false", d)
		}
	}
}

func TestSkipInterface(t *testing.T) {
	skip := []string{"lo", "lo0", "veth1a2b3c", "utun0", "awdl0", "gif0", "stf0", "ap1", ""}
	keep := []string{"eth0", "ens3", "enp0s3", "en0", "wlan0", "docker0", "br-abc123", "tailscale0", "bond0"}
	for _, n := range skip {
		if !SkipInterface(n) {
			t.Errorf("SkipInterface(%q) = false, want true", n)
		}
	}
	for _, n := range keep {
		if SkipInterface(n) {
			t.Errorf("SkipInterface(%q) = true, want false", n)
		}
	}
}

func TestClampPercent(t *testing.T) {
	cases := []struct{ in, want float64 }{
		{-1, 0}, {0, 0}, {50.005, 50.01}, {100, 100}, {101, 100},
	}
	for _, c := range cases {
		if got := clampPercent(c.in); got != c.want {
			t.Errorf("clampPercent(%v) = %v want %v", c.in, got, c.want)
		}
	}
}

func TestCPUPercentFromDelta(t *testing.T) {
	c := New()
	c.prevCPU = cpu.TimesStat{User: 100, System: 50, Idle: 850}
	c.haveCPU = true
	// Advance by 10 user seconds and 90 idle seconds: 10% busy.
	now := cpu.TimesStat{User: 110, System: 50, Idle: 940}

	prev := c.prevCPU
	busy := cpuBusy(now) - cpuBusy(prev)
	total := cpuTotal(now) - cpuTotal(prev)
	if got := clampPercent(busy / total * 100); got != 10 {
		t.Fatalf("usage = %v want 10", got)
	}
}

func TestEncodeProducesEveryMeasurement(t *testing.T) {
	s := &Snapshot{
		Time:   time.Unix(1, 0),
		CPU:    &CPU{Cores: 4, UsagePercent: 12},
		Load:   &Load{Load1: 1},
		Mem:    &Mem{Total: 100, Used: 50},
		Disks:  []Disk{{Mount: "/", Fstype: "ext4", Device: "sda1", Total: 10, Used: 5}},
		DiskIO: []DiskIO{{Device: "sda", ReadBytes: 1}},
		Nets:   []Net{{Interface: "eth0", RxBytes: 1}},
		Host:   &Host{Uptime: 60, BootTime: 1, Platform: "debian"},
	}
	e := lineproto.New(lineproto.Tag{Key: "host", Value: "h"})
	s.Encode(e)
	out := e.String()
	for _, m := range []string{"obs_cpu", "obs_load", "obs_mem", "obs_disk,", "obs_diskio", "obs_net", "obs_host"} {
		if !strings.Contains(out, m) {
			t.Errorf("missing %s in:\n%s", m, out)
		}
	}
	if !strings.Contains(out, "cores=4") {
		t.Error("cores must be a tag")
	}
	if e.Points() != 7 {
		t.Errorf("points = %d want 7", e.Points())
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if !strings.Contains(line, "host=h") {
			t.Errorf("line missing the host tag: %s", line)
		}
	}
}

func TestEncodeSkipsNilSections(t *testing.T) {
	s := &Snapshot{Time: time.Unix(1, 0)}
	e := lineproto.New()
	s.Encode(e)
	if e.Points() != 0 {
		t.Fatalf("points = %d want 0", e.Points())
	}
}

func TestSortByName(t *testing.T) {
	in := []Net{{Interface: "eth1"}, {Interface: "br0"}, {Interface: "eth0"}, {Interface: "a"}}
	sortByName(in, func(n Net) string { return n.Interface })
	want := []string{"a", "br0", "eth0", "eth1"}
	for i := range want {
		if in[i].Interface != want[i] {
			t.Fatalf("order = %+v want %v", in, want)
		}
	}
	// The empty and the single case must not panic.
	sortByName([]Net(nil), func(n Net) string { return n.Interface })
	sortByName([]Net{{Interface: "x"}}, func(n Net) string { return n.Interface })
}

// A mount that disappears must not leave an entry in mountErr.
func TestMountErrEvictsGoneMounts(t *testing.T) {
	c := New()
	c.mountErr["/gone"] = true
	c.mountErr["/also-gone"] = true
	if _, _, err := c.collectDisks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.mountErr) != 0 {
		t.Errorf("mountErr still holds %v", c.mountErr)
	}
}

func TestCollectOnThisMachine(t *testing.T) {
	c := New()
	c.Prime(context.Background())
	s := c.Collect(context.Background())
	if s.CPU == nil {
		t.Error("no cpu sample")
	}
	if s.Mem == nil || s.Mem.Total == 0 {
		t.Error("no memory sample")
	}
	if s.Host == nil {
		t.Error("no host sample")
	}
	if len(s.Disks) == 0 {
		t.Error("no mounts found")
	}
}
