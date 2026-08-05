// Package collect reads host metrics with gopsutil.
//
// One Collector keeps the state that the delta math needs. Call Collect once
// per interval from one goroutine. The Collector is not safe for concurrent
// use.
package collect

import (
	"context"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"

	"github.com/jamesobrien/observant/agent/internal/lineproto"
)

// CPU holds the processor sample.
type CPU struct {
	Cores int
	// Percent values cover the time since the previous Collect call.
	// The first call reports the time since boot.
	UsagePercent  float64
	UserPercent   float64
	SystemPercent float64
	IowaitPercent float64
	StealPercent  float64
	IdlePercent   float64
	// Seconds values are cumulative since boot.
	Times cpu.TimesStat
}

// Load holds the run-queue averages.
type Load struct {
	Load1  float64
	Load5  float64
	Load15 float64
}

// Mem holds the memory and swap sample.
type Mem struct {
	Total       uint64
	Used        uint64
	Available   uint64
	Free        uint64
	Cached      uint64
	Buffers     uint64
	UsedPercent float64

	SwapTotal       uint64
	SwapUsed        uint64
	SwapFree        uint64
	SwapUsedPercent float64
}

// Disk holds one mounted filesystem.
type Disk struct {
	Mount             string
	Device            string
	Fstype            string
	Total             uint64
	Used              uint64
	Free              uint64
	UsedPercent       float64
	InodesTotal       uint64
	InodesUsed        uint64
	InodesFree        uint64
	InodesUsedPercent float64
}

// DiskIO holds one block device. Every counter is cumulative since boot.
type DiskIO struct {
	Device     string
	ReadBytes  uint64
	WriteBytes uint64
	ReadOps    uint64
	WriteOps   uint64
	ReadTime   uint64
	WriteTime  uint64
	IoTime     uint64
	InProgress uint64
}

// Net holds one network interface. Every counter is cumulative since boot.
type Net struct {
	Interface string
	RxBytes   uint64
	TxBytes   uint64
	RxPackets uint64
	TxPackets uint64
	RxErrs    uint64
	TxErrs    uint64
	RxDrops   uint64
	TxDrops   uint64
}

// Host holds the machine identity and the uptime.
type Host struct {
	Uptime          uint64
	BootTime        uint64
	Procs           uint64
	OS              string
	Platform        string
	PlatformVersion string
	Kernel          string
	Arch            string
	Virtualization  string
}

// Snapshot is the result of one collection cycle.
// A nil section means that the collector failed. Errs holds the reasons.
type Snapshot struct {
	Time   time.Time
	CPU    *CPU
	Load   *Load
	Mem    *Mem
	Disks  []Disk
	DiskIO []DiskIO
	Nets   []Net
	Host   *Host
	Errs   []error
}

// Collector reads the host metrics and keeps the delta state.
type Collector struct {
	prevCPU  cpu.TimesStat
	haveCPU  bool
	cores    int
	timeout  time.Duration
	mountErr map[string]bool
}

// New makes a Collector. The timeout bounds one collector call.
func New() *Collector {
	return &Collector{timeout: 5 * time.Second, mountErr: map[string]bool{}}
}

// Prime takes the first CPU sample so that the next Collect call reports the
// usage over the gap and not the usage since boot.
func (c *Collector) Prime(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if times, err := cpu.TimesWithContext(ctx, false); err == nil && len(times) > 0 {
		c.prevCPU = times[0]
		c.haveCPU = true
	}
}

// Collect reads every host metric once.
// Collect returns a Snapshot even when some collectors fail.
func (c *Collector) Collect(ctx context.Context) *Snapshot {
	s := &Snapshot{Time: time.Now()}

	if v, err := c.collectCPU(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("cpu: %w", err))
	} else {
		s.CPU = v
	}
	if v, err := c.collectLoad(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("load: %w", err))
	} else {
		s.Load = v
	}
	if v, err := c.collectMem(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("mem: %w", err))
	} else {
		s.Mem = v
	}
	if v, failed, err := c.collectDisks(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("disk: %w", err))
	} else {
		s.Disks = v
		if len(failed) > 0 {
			s.Errs = append(s.Errs, fmt.Errorf("disk: cannot read mounts: %s", strings.Join(failed, ", ")))
		}
	}
	if v, err := c.collectDiskIO(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("diskio: %w", err))
	} else {
		s.DiskIO = v
	}
	if v, err := c.collectNet(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("net: %w", err))
	} else {
		s.Nets = v
	}
	if v, err := c.collectHost(ctx); err != nil {
		s.Errs = append(s.Errs, fmt.Errorf("host: %w", err))
	} else {
		s.Host = v
	}
	return s
}

func (c *Collector) collectCPU(ctx context.Context) (*CPU, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	times, err := cpu.TimesWithContext(ctx, false)
	if err != nil {
		return nil, err
	}
	if len(times) == 0 {
		return nil, fmt.Errorf("no cpu times returned")
	}
	now := times[0]

	if c.cores == 0 {
		if n, err := cpu.CountsWithContext(ctx, true); err == nil && n > 0 {
			c.cores = n
		}
	}

	out := &CPU{Cores: c.cores, Times: now}
	prev := c.prevCPU
	if !c.haveCPU {
		prev = cpu.TimesStat{CPU: now.CPU}
	}
	c.prevCPU = now
	c.haveCPU = true

	busy := cpuBusy(now) - cpuBusy(prev)
	total := cpuTotal(now) - cpuTotal(prev)
	if total <= 0 {
		// A counter reset or a same-instant call. Report no movement.
		return out, nil
	}
	pct := func(a, b float64) float64 { return clampPercent((a - b) / total * 100) }
	out.UsagePercent = clampPercent(busy / total * 100)
	out.UserPercent = pct(now.User+now.Nice, prev.User+prev.Nice)
	out.SystemPercent = pct(now.System+now.Irq+now.Softirq, prev.System+prev.Irq+prev.Softirq)
	out.IowaitPercent = pct(now.Iowait, prev.Iowait)
	out.StealPercent = pct(now.Steal, prev.Steal)
	out.IdlePercent = pct(now.Idle, prev.Idle)
	return out, nil
}

func cpuBusy(t cpu.TimesStat) float64 {
	return t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal
}

func cpuTotal(t cpu.TimesStat) float64 {
	return cpuBusy(t) + t.Idle
}

func clampPercent(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return math.Round(v*100) / 100
}

func (c *Collector) collectLoad(ctx context.Context) (*Load, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	a, err := load.AvgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Load{Load1: a.Load1, Load5: a.Load5, Load15: a.Load15}, nil
}

func (c *Collector) collectMem(ctx context.Context) (*Mem, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	v, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, err
	}
	out := &Mem{
		Total:       v.Total,
		Used:        v.Used,
		Available:   v.Available,
		Free:        v.Free,
		Cached:      v.Cached,
		Buffers:     v.Buffers,
		UsedPercent: round2(v.UsedPercent),
	}
	if s, err := mem.SwapMemoryWithContext(ctx); err == nil {
		out.SwapTotal = s.Total
		out.SwapUsed = s.Used
		out.SwapFree = s.Free
		out.SwapUsedPercent = round2(s.UsedPercent)
	}
	return out, nil
}

// collectDisks returns the usable mounts and the mounts it could not read.
// It reports each unreadable mount one time only.
func (c *Collector) collectDisks(ctx context.Context) ([]Disk, []string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	parts, err := disk.PartitionsWithContext(ctx, false)
	if err != nil {
		return nil, nil, err
	}
	var failed []string
	out := make([]Disk, 0, len(parts))
	seen := make(map[string]bool, len(parts))
	// live holds the mounts of this cycle. A mount that disappears must lose
	// its mountErr entry, else the map grows without limit and the agent
	// stays quiet if the mount comes back and still fails.
	live := make(map[string]bool, len(parts))
	for _, p := range parts {
		if SkipFstype(p.Fstype) || SkipMount(p.Mountpoint) {
			continue
		}
		live[p.Mountpoint] = true
		u, err := disk.UsageWithContext(ctx, p.Mountpoint)
		if err != nil {
			// A permission error on one mount must not kill the cycle.
			// Report the mount one time, then stay quiet about it.
			if !c.mountErr[p.Mountpoint] {
				c.mountErr[p.Mountpoint] = true
				failed = append(failed, p.Mountpoint)
			}
			continue
		}
		if u.Total == 0 {
			continue
		}
		// The same device can carry several mounts. Keep the first one.
		key := p.Device + "\x00" + strconv.FormatUint(u.Total, 10)
		if p.Device != "" && seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, Disk{
			Mount:             p.Mountpoint,
			Device:            shortDevice(p.Device),
			Fstype:            p.Fstype,
			Total:             u.Total,
			Used:              u.Used,
			Free:              u.Free,
			UsedPercent:       round2(u.UsedPercent),
			InodesTotal:       u.InodesTotal,
			InodesUsed:        u.InodesUsed,
			InodesFree:        u.InodesFree,
			InodesUsedPercent: round2(u.InodesUsedPercent),
		})
	}
	for m := range c.mountErr {
		if !live[m] {
			delete(c.mountErr, m)
		}
	}
	sortByName(out, func(d Disk) string { return d.Mount })
	return out, failed, nil
}

func (c *Collector) collectDiskIO(ctx context.Context) ([]DiskIO, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	counters, err := disk.IOCountersWithContext(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]DiskIO, 0, len(counters))
	for name, v := range counters {
		if SkipBlockDevice(name) {
			continue
		}
		if v.ReadBytes == 0 && v.WriteBytes == 0 && v.ReadCount == 0 && v.WriteCount == 0 {
			continue
		}
		out = append(out, DiskIO{
			Device:     name,
			ReadBytes:  v.ReadBytes,
			WriteBytes: v.WriteBytes,
			ReadOps:    v.ReadCount,
			WriteOps:   v.WriteCount,
			ReadTime:   v.ReadTime,
			WriteTime:  v.WriteTime,
			IoTime:     v.IoTime,
			InProgress: v.IopsInProgress,
		})
	}
	sortByName(out, func(d DiskIO) string { return d.Device })
	return out, nil
}

func (c *Collector) collectNet(ctx context.Context) ([]Net, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	counters, err := net.IOCountersWithContext(ctx, true)
	if err != nil {
		return nil, err
	}
	out := make([]Net, 0, len(counters))
	for _, v := range counters {
		if SkipInterface(v.Name) {
			continue
		}
		if v.BytesRecv == 0 && v.BytesSent == 0 {
			continue
		}
		out = append(out, Net{
			Interface: v.Name,
			RxBytes:   v.BytesRecv,
			TxBytes:   v.BytesSent,
			RxPackets: v.PacketsRecv,
			TxPackets: v.PacketsSent,
			RxErrs:    v.Errin,
			TxErrs:    v.Errout,
			RxDrops:   v.Dropin,
			TxDrops:   v.Dropout,
		})
	}
	sortByName(out, func(n Net) string { return n.Interface })
	return out, nil
}

func (c *Collector) collectHost(ctx context.Context) (*Host, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	i, err := host.InfoWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Host{
		Uptime:          i.Uptime,
		BootTime:        i.BootTime,
		Procs:           i.Procs,
		OS:              i.OS,
		Platform:        i.Platform,
		PlatformVersion: i.PlatformVersion,
		Kernel:          i.KernelVersion,
		Arch:            i.KernelArch,
		Virtualization:  i.VirtualizationSystem,
	}, nil
}

// Encode writes the snapshot into the encoder as line protocol.
func (s *Snapshot) Encode(e *lineproto.Encoder) {
	ts := s.Time

	if v := s.CPU; v != nil {
		var tags []lineproto.Tag
		if v.Cores > 0 {
			tags = append(tags, lineproto.Tag{Key: "cores", Value: strconv.Itoa(v.Cores)})
		}
		e.Point("obs_cpu", tags, []lineproto.Field{
			lineproto.F("usage_percent", v.UsagePercent),
			lineproto.F("user_percent", v.UserPercent),
			lineproto.F("system_percent", v.SystemPercent),
			lineproto.F("iowait_percent", v.IowaitPercent),
			lineproto.F("steal_percent", v.StealPercent),
			lineproto.F("idle_percent", v.IdlePercent),
			lineproto.F("user_seconds", v.Times.User),
			lineproto.F("system_seconds", v.Times.System),
			lineproto.F("idle_seconds", v.Times.Idle),
			lineproto.F("nice_seconds", v.Times.Nice),
			lineproto.F("iowait_seconds", v.Times.Iowait),
			lineproto.F("irq_seconds", v.Times.Irq),
			lineproto.F("softirq_seconds", v.Times.Softirq),
			lineproto.F("steal_seconds", v.Times.Steal),
		}, ts)
	}

	if v := s.Load; v != nil {
		e.Point("obs_load", nil, []lineproto.Field{
			lineproto.F("load1", v.Load1),
			lineproto.F("load5", v.Load5),
			lineproto.F("load15", v.Load15),
		}, ts)
	}

	if v := s.Mem; v != nil {
		e.Point("obs_mem", nil, []lineproto.Field{
			lineproto.U("total_bytes", v.Total),
			lineproto.U("used_bytes", v.Used),
			lineproto.U("available_bytes", v.Available),
			lineproto.U("free_bytes", v.Free),
			lineproto.U("cached_bytes", v.Cached),
			lineproto.U("buffers_bytes", v.Buffers),
			lineproto.F("used_percent", v.UsedPercent),
			lineproto.U("swap_total_bytes", v.SwapTotal),
			lineproto.U("swap_used_bytes", v.SwapUsed),
			lineproto.U("swap_free_bytes", v.SwapFree),
			lineproto.F("swap_used_percent", v.SwapUsedPercent),
		}, ts)
	}

	for _, d := range s.Disks {
		e.Point("obs_disk", []lineproto.Tag{
			{Key: "mount", Value: d.Mount},
			{Key: "device", Value: d.Device},
			{Key: "fstype", Value: d.Fstype},
		}, []lineproto.Field{
			lineproto.U("total_bytes", d.Total),
			lineproto.U("used_bytes", d.Used),
			lineproto.U("free_bytes", d.Free),
			lineproto.F("used_percent", d.UsedPercent),
			lineproto.U("inodes_total", d.InodesTotal),
			lineproto.U("inodes_used", d.InodesUsed),
			lineproto.U("inodes_free", d.InodesFree),
			lineproto.F("inodes_used_percent", d.InodesUsedPercent),
		}, ts)
	}

	for _, d := range s.DiskIO {
		e.Point("obs_diskio", []lineproto.Tag{
			{Key: "device", Value: d.Device},
		}, []lineproto.Field{
			lineproto.U("read_bytes", d.ReadBytes),
			lineproto.U("write_bytes", d.WriteBytes),
			lineproto.U("read_ops", d.ReadOps),
			lineproto.U("write_ops", d.WriteOps),
			lineproto.U("read_time_ms", d.ReadTime),
			lineproto.U("write_time_ms", d.WriteTime),
			lineproto.U("io_time_ms", d.IoTime),
			lineproto.U("in_progress", d.InProgress),
		}, ts)
	}

	for _, n := range s.Nets {
		e.Point("obs_net", []lineproto.Tag{
			{Key: "interface", Value: n.Interface},
		}, []lineproto.Field{
			lineproto.U("rx_bytes", n.RxBytes),
			lineproto.U("tx_bytes", n.TxBytes),
			lineproto.U("rx_packets", n.RxPackets),
			lineproto.U("tx_packets", n.TxPackets),
			lineproto.U("rx_errs", n.RxErrs),
			lineproto.U("tx_errs", n.TxErrs),
			lineproto.U("rx_drops", n.RxDrops),
			lineproto.U("tx_drops", n.TxDrops),
		}, ts)
	}

	if v := s.Host; v != nil {
		e.Point("obs_host", []lineproto.Tag{
			{Key: "os", Value: v.OS},
			{Key: "platform", Value: v.Platform},
			{Key: "platform_version", Value: v.PlatformVersion},
			{Key: "kernel", Value: v.Kernel},
			{Key: "arch", Value: v.Arch},
			{Key: "virt", Value: v.Virtualization},
		}, []lineproto.Field{
			lineproto.U("uptime_seconds", v.Uptime),
			lineproto.U("boot_time", v.BootTime),
			lineproto.U("procs", v.Procs),
		}, ts)
	}
}

// pseudoFstypes lists the filesystem types that hold no real storage.
var pseudoFstypes = map[string]bool{
	"autofs": true, "binfmt_misc": true, "bpf": true, "cgroup": true,
	"cgroup2": true, "configfs": true, "debugfs": true, "devfs": true,
	"devpts": true, "devtmpfs": true, "efivarfs": true, "fuse.gvfsd-fuse": true,
	"fuse.lxcfs": true, "fuse.portal": true, "fuse.snapfuse": true,
	"fusectl": true, "hugetlbfs": true, "iso9660": true, "mqueue": true,
	"none": true, "nsfs": true, "overlay": true, "proc": true, "procfs": true,
	"pstore": true, "ramfs": true, "rpc_pipefs": true, "securityfs": true,
	"selinuxfs": true, "shm": true, "snapfuse": true, "squashfs": true,
	"sysfs": true, "tmpfs": true, "tracefs": true,
}

// SkipFstype reports whether the filesystem type holds no real storage.
func SkipFstype(fstype string) bool {
	f := strings.ToLower(strings.TrimSpace(fstype))
	if f == "" {
		return true
	}
	return pseudoFstypes[f]
}

// skipMountPrefixes lists the mount trees that carry no user data.
var skipMountPrefixes = []string{
	"/dev", "/proc", "/sys", "/run",
	"/var/lib/docker", "/var/lib/containers", "/var/lib/kubelet",
	"/snap", "/var/snap",
	"/System/Volumes/VM", "/System/Volumes/Preboot", "/System/Volumes/Update",
	"/System/Volumes/xarts", "/System/Volumes/iSCPreboot", "/System/Volumes/Hardware",
	"/private/var/vm",
}

// SkipMount reports whether the mountpoint carries no user data.
func SkipMount(mount string) bool {
	if mount == "" {
		return true
	}
	m := path.Clean(mount)
	for _, p := range skipMountPrefixes {
		if m == p || strings.HasPrefix(m, p+"/") {
			return true
		}
	}
	return false
}

// skipDevicePrefixes lists the block devices that mirror other devices.
var skipDevicePrefixes = []string{"loop", "ram", "zram", "fd", "sr", "dm-"}

// SkipBlockDevice reports whether the block device is not worth a series.
func SkipBlockDevice(name string) bool {
	if name == "" {
		return true
	}
	for _, p := range skipDevicePrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// skipIfacePrefixes lists the interfaces that add churn without value.
var skipIfacePrefixes = []string{"lo", "gif", "stf", "anpi", "awdl", "llw", "utun", "ap"}

// SkipInterface reports whether the interface is not worth a series.
func SkipInterface(name string) bool {
	if name == "" {
		return true
	}
	for _, p := range skipIfacePrefixes {
		if name == p || (strings.HasPrefix(name, p) && isDigits(name[len(p):])) {
			return true
		}
	}
	return strings.HasPrefix(name, "veth")
}

func isDigits(s string) bool {
	if s == "" {
		return true
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func shortDevice(dev string) string {
	if dev == "" {
		return ""
	}
	return strings.TrimPrefix(dev, "/dev/")
}

func round2(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return math.Round(v*100) / 100
}

func sortByName[T any](s []T, key func(T) string) {
	sort.Slice(s, func(i, j int) bool { return key(s[i]) < key(s[j]) })
}
