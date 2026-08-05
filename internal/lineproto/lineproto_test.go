package lineproto

import (
	"math"
	"strings"
	"testing"
	"time"
)

var ts = time.Unix(1700000000, 123456789)

// The constants defeat compile-time folding, so the float math is real.
var (
	pointOne = 0.1
	pointTwo = 0.2
)

func TestPointBasic(t *testing.T) {
	e := New(Tag{"host", "web-1"})
	e.Point("obs_cpu", []Tag{{"role", "api"}}, []Field{F("usage_percent", 12.5), I("cores", 4)}, ts)
	want := "obs_cpu,host=web-1,role=api usage_percent=12.5,cores=4i 1700000000123456789\n"
	if got := e.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if e.Points() != 1 {
		t.Fatalf("points = %d", e.Points())
	}
}

func TestTagsSorted(t *testing.T) {
	e := New(Tag{"zeta", "1"})
	e.Point("m", []Tag{{"beta", "2"}, {"alpha", "3"}}, []Field{I("v", 1)}, time.Time{})
	want := "m,alpha=3,beta=2,zeta=1 v=1i\n"
	if got := e.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestTagOverride(t *testing.T) {
	e := New(Tag{"host", "base"})
	e.Point("m", []Tag{{"host", "override"}}, []Field{I("v", 1)}, time.Time{})
	want := "m,host=override v=1i\n"
	if got := e.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestEmptyTagsDropped(t *testing.T) {
	e := New()
	e.Point("m", []Tag{{"a", ""}, {"", "b"}, {"c", "d"}}, []Field{I("v", 1)}, time.Time{})
	want := "m,c=d v=1i\n"
	if got := e.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestEscaping(t *testing.T) {
	cases := []struct {
		name  string
		meas  string
		tags  []Tag
		field Field
		want  string
	}{
		{
			name:  "measurement comma and space",
			meas:  "obs disk,x",
			field: I("v", 1),
			want:  `obs\ disk\,x v=1i` + "\n",
		},
		{
			name:  "tag comma space equals",
			meas:  "m",
			tags:  []Tag{{"mo unt", "a=b,c"}},
			field: I("v", 1),
			want:  `m,mo\ unt=a\=b\,c v=1i` + "\n",
		},
		{
			name:  "field key escaped like tag",
			meas:  "m",
			field: I("a b=c", 1),
			want:  `m a\ b\=c=1i` + "\n",
		},
		{
			name:  "string field quotes and backslash",
			meas:  "m",
			field: S("v", `he said "hi"\ok`),
			want:  `m v="he said \"hi\"\\ok"` + "\n",
		},
		{
			name:  "newline becomes space",
			meas:  "m",
			tags:  []Tag{{"k", "a\nb\rc"}},
			field: I("v", 1),
			want:  "m,k=a b c v=1i\n",
		},
		{
			name:  "equals not escaped in string field",
			meas:  "m",
			field: S("v", "a=b, c"),
			want:  `m v="a=b, c"` + "\n",
		},
		{
			name:  "trailing backslash in a tag value",
			meas:  "m",
			tags:  []Tag{{"k", `c:\`}},
			field: I("v", 1),
			want:  `m,k=c:\\ v=1i` + "\n",
		},
		{
			name:  "backslash in a tag key",
			meas:  "m",
			tags:  []Tag{{`a\b`, "c"}},
			field: I("v", 1),
			want:  `m,a\\b=c v=1i` + "\n",
		},
		{
			name:  "backslash in a measurement",
			meas:  `ob\s`,
			field: I("v", 1),
			want:  `ob\\s v=1i` + "\n",
		},
		{
			name:  "backslash in a field key",
			meas:  "m",
			field: I(`a\`, 1),
			want:  `m a\\=1i` + "\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := New()
			e.Point(tc.meas, tc.tags, []Field{tc.field}, time.Time{})
			if got := e.String(); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestNumberFormatting(t *testing.T) {
	cases := []struct {
		field Field
		want  string
	}{
		{F("v", 0), "v=0"},
		{F("v", 1), "v=1"},
		{F("v", 12.5), "v=12.5"},
		{F("v", -0.125), "v=-0.125"},
		{F("v", 1e21), "v=1e+21"},
		{F("v", pointOne+pointTwo), "v=0.30000000000000004"},
		{I("v", 0), "v=0i"},
		{I("v", -9223372036854775808), "v=-9223372036854775808i"},
		{I("v", 9223372036854775807), "v=9223372036854775807i"},
		{U("v", 42), "v=42i"},
		{U("v", math.MaxUint64), "v=1.8446744073709552e+19"},
		{B("v", true), "v=T"},
		{B("v", false), "v=F"},
	}
	for _, tc := range cases {
		e := New()
		e.Point("m", nil, []Field{tc.field}, time.Time{})
		want := "m " + tc.want + "\n"
		if got := e.String(); got != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
}

func TestInvalidFloatsDropped(t *testing.T) {
	e := New()
	e.Point("m", nil, []Field{F("bad", math.NaN()), F("worse", math.Inf(1)), I("good", 3)}, time.Time{})
	want := "m good=3i\n"
	if got := e.String(); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestPointDroppedWhenNoValidField(t *testing.T) {
	e := New()
	e.Point("m", nil, []Field{F("bad", math.NaN())}, ts)
	e.Point("", nil, []Field{I("v", 1)}, ts)
	e.Point("m2", nil, nil, ts)
	if e.String() != "" {
		t.Fatalf("expected empty, got %q", e.String())
	}
	if e.Points() != 0 {
		t.Fatalf("points = %d", e.Points())
	}
}

func TestMultiplePointsAndReset(t *testing.T) {
	e := New(Tag{"host", "h"})
	e.Point("a", nil, []Field{I("v", 1)}, time.Time{})
	e.Point("b", nil, []Field{I("v", 2)}, time.Time{})
	if n := strings.Count(e.String(), "\n"); n != 2 {
		t.Fatalf("lines = %d", n)
	}
	if e.Len() == 0 {
		t.Fatal("expected non-zero length")
	}
	e.Reset()
	if e.Len() != 0 || e.Points() != 0 {
		t.Fatalf("reset failed: len=%d points=%d", e.Len(), e.Points())
	}
	e.Point("c", nil, []Field{I("v", 3)}, time.Time{})
	if got := e.String(); got != "c,host=h v=3i\n" {
		t.Fatalf("got %q", got)
	}
}
