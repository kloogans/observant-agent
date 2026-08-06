package collect

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jamesobrien/observant/agent/internal/lineproto"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
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
	// A container host holds one veth per container. Those series mirror the
	// traffic of the real interface, so a fleet chart would count it twice.
	skip := []string{
		"lo", "lo0", "utun0", "awdl0", "gif0", "stf0", "ap1", "",
		"veth1a2b3c", "docker0", "docker_gwbridge", "br-9f3c1a2b4d5e",
		"podman0", "cni-podman0", "virbr0", "flannel.1", "cali1a2b3c", "tap0", "dummy0",
	}
	keep := []string{"eth0", "ens3", "enp0s3", "en0", "wlan0", "tailscale0", "bond0", "br0", "wg0"}
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
	if _, _, _, err := c.collectDisks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.mountErr) != 0 {
		t.Errorf("mountErr still holds %v", c.mountErr)
	}
}

// A dead network mount blocks the statfs syscall. The collector must give up
// on that mount instead of blocking the whole agent on every cycle.
func TestStuckMountIsDroppedAfterRepeatedTimeouts(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls atomic.Int32

	oldParts, oldUsage := diskPartitions, diskUsage
	t.Cleanup(func() { diskPartitions, diskUsage = oldParts, oldUsage })
	diskPartitions = func(ctx context.Context, all bool) ([]disk.PartitionStat, error) {
		return []disk.PartitionStat{{Mountpoint: "/mnt/nfs", Device: "/dev/nfs", Fstype: "nfs4"}}, nil
	}
	diskUsage = func(ctx context.Context, mount string) (*disk.UsageStat, error) {
		calls.Add(1)
		<-release
		return nil, nil
	}

	c := New()
	c.mountTime = 20 * time.Millisecond
	stuckReports := 0
	for i := 0; i < 6; i++ {
		out, _, stuck, err := c.collectDisks(context.Background())
		if err != nil {
			t.Fatalf("cycle %d: %v", i, err)
		}
		if len(out) != 0 {
			t.Fatalf("cycle %d: a stuck mount must report no usage: %+v", i, out)
		}
		stuckReports += len(stuck)
		// The goroutine of the timed-out call still holds the mount, so the
		// next cycle must not start a second call.
		time.Sleep(30 * time.Millisecond)
	}
	// The first call never returns, so the in-flight guard blocks the second
	// call and the count of timeouts bans the mount. Six cycles cost one
	// blocked goroutine.
	if n := calls.Load(); n != 1 {
		t.Errorf("usage calls = %d want 1", n)
	}
	if stuckReports != 1 {
		t.Errorf("stuck reports = %d want 1", stuckReports)
	}
	if c.mountTimeouts["/mnt/nfs"] != maxMountTimeouts {
		t.Errorf("timeout count = %d want %d", c.mountTimeouts["/mnt/nfs"], maxMountTimeouts)
	}
}

// A timeout repeats on every cycle. The log must not repeat with it.
func TestTimeoutReportIsRateLimited(t *testing.T) {
	c := New()
	now := time.Unix(1_000_000, 0)
	first := &Snapshot{Time: now}
	c.note(first, "disk", ErrTimeout)
	if len(first.Errs) != 1 {
		t.Fatalf("first report = %v want one error", first.Errs)
	}
	next := &Snapshot{Time: now.Add(time.Minute)}
	c.note(next, "disk", ErrTimeout)
	if len(next.Errs) != 0 {
		t.Errorf("the same timeout must stay quiet: %v", next.Errs)
	}
	later := &Snapshot{Time: now.Add(timeoutLogEvery + time.Second)}
	c.note(later, "disk", ErrTimeout)
	if len(later.Errs) != 1 {
		t.Errorf("the timeout must report again after %s: %v", timeoutLogEvery, later.Errs)
	}
	// A collector that recovers must report at once when it fails again.
	c.note(&Snapshot{Time: later.Time}, "disk", nil)
	again := &Snapshot{Time: later.Time.Add(time.Second)}
	c.note(again, "disk", ErrTimeout)
	if len(again.Errs) != 1 {
		t.Errorf("a new failure after a good cycle must report: %v", again.Errs)
	}
	// A plain error is not a timeout and is never suppressed.
	plainA := &Snapshot{Time: now}
	plainB := &Snapshot{Time: now}
	c.note(plainA, "mem", errors.New("permission denied"))
	c.note(plainB, "mem", errors.New("permission denied"))
	if len(plainA.Errs) != 1 || len(plainB.Errs) != 1 {
		t.Errorf("plain errors = %v %v want one each", plainA.Errs, plainB.Errs)
	}
}

// One stuck syscall must cost one goroutine, not one goroutine per cycle.
func TestDeadlineStartsOneCallAtATime(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var calls atomic.Int32
	block := func(ctx context.Context) (int, error) {
		calls.Add(1)
		<-release
		return 1, nil
	}

	c := New()
	for i := 0; i < 3; i++ {
		v, err := deadline(context.Background(), c, "stuck", 10*time.Millisecond, block)
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("call %d: err = %v want ErrTimeout", i, err)
		}
		if v != 0 {
			t.Fatalf("call %d: value = %d want the zero value", i, v)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("started calls = %d want 1", n)
	}
}

// A collector that answers must return its value and clear the way for the
// next cycle.
func TestDeadlineReturnsTheValue(t *testing.T) {
	c := New()
	v, err := deadline(context.Background(), c, "quick", time.Second, func(ctx context.Context) (int, error) {
		return 42, nil
	})
	if err != nil || v != 42 {
		t.Fatalf("v, err = %d, %v want 42, nil", v, err)
	}
	if _, err := deadline(context.Background(), c, "quick", time.Second, func(ctx context.Context) (int, error) {
		return 7, nil
	}); err != nil {
		t.Fatalf("second call = %v", err)
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
