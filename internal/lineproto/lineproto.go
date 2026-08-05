// Package lineproto encodes metrics as InfluxDB line protocol.
//
// The encoder writes into one reusable buffer. The caller creates one encoder
// per collection cycle, adds points, then reads the bytes.
package lineproto

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"time"
)

// Tag is one key/value label on a point.
type Tag struct {
	Key   string
	Value string
}

// Field is one measured value on a point.
// Value holds float64, int64, uint64, bool, or string.
type Field struct {
	Key   string
	Value any
}

// F makes a float field.
func F(key string, v float64) Field { return Field{Key: key, Value: v} }

// I makes a signed integer field.
func I(key string, v int64) Field { return Field{Key: key, Value: v} }

// U makes an unsigned integer field.
func U(key string, v uint64) Field { return Field{Key: key, Value: v} }

// B makes a boolean field.
func B(key string, v bool) Field { return Field{Key: key, Value: v} }

// S makes a string field.
func S(key, v string) Field { return Field{Key: key, Value: v} }

// Encoder builds a line protocol batch.
type Encoder struct {
	buf     bytes.Buffer
	base    []Tag
	points  int
	scratch []Tag
}

// New makes an encoder. The base tags go on every point.
func New(base ...Tag) *Encoder {
	e := &Encoder{}
	e.base = append(e.base, base...)
	return e
}

// Point appends one measurement line.
// Point drops the line if the measurement is empty or if no field is valid.
// A NaN or infinite float field is not valid.
func (e *Encoder) Point(measurement string, tags []Tag, fields []Field, ts time.Time) {
	if measurement == "" {
		return
	}
	valid := 0
	for _, f := range fields {
		if fieldValid(f) {
			valid++
		}
	}
	if valid == 0 {
		return
	}

	e.scratch = e.scratch[:0]
	for _, t := range e.base {
		if t.Key != "" && t.Value != "" {
			e.scratch = append(e.scratch, t)
		}
	}
	for _, t := range tags {
		if t.Key != "" && t.Value != "" {
			e.scratch = append(e.scratch, t)
		}
	}
	sort.SliceStable(e.scratch, func(i, j int) bool { return e.scratch[i].Key < e.scratch[j].Key })

	writeEscaped(&e.buf, measurement, escMeasurement)
	for i, t := range e.scratch {
		// Drop a duplicate key. The sort is stable and base tags come first,
		// so the last entry for a key is the override. Keep that one.
		if i+1 < len(e.scratch) && e.scratch[i+1].Key == t.Key {
			continue
		}
		e.buf.WriteByte(',')
		writeEscaped(&e.buf, t.Key, escTag)
		e.buf.WriteByte('=')
		writeEscaped(&e.buf, t.Value, escTag)
	}

	e.buf.WriteByte(' ')
	first := true
	for _, f := range fields {
		if !fieldValid(f) {
			continue
		}
		if !first {
			e.buf.WriteByte(',')
		}
		first = false
		writeEscaped(&e.buf, f.Key, escTag)
		e.buf.WriteByte('=')
		writeValue(&e.buf, f.Value)
	}

	if !ts.IsZero() {
		e.buf.WriteByte(' ')
		e.buf.WriteString(strconv.FormatInt(ts.UnixNano(), 10))
	}
	e.buf.WriteByte('\n')
	e.points++
}

// Bytes returns the encoded batch. The slice is valid until the next write.
func (e *Encoder) Bytes() []byte { return e.buf.Bytes() }

// String returns the encoded batch as text.
func (e *Encoder) String() string { return e.buf.String() }

// Len returns the byte count of the batch.
func (e *Encoder) Len() int { return e.buf.Len() }

// Points returns the number of lines in the batch.
func (e *Encoder) Points() int { return e.points }

// Reset empties the batch and keeps the buffer.
func (e *Encoder) Reset() {
	e.buf.Reset()
	e.points = 0
}

func fieldValid(f Field) bool {
	if f.Key == "" {
		return false
	}
	if v, ok := f.Value.(float64); ok {
		return !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	return f.Value != nil
}

func writeValue(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case float64:
		buf.WriteString(strconv.FormatFloat(t, 'g', -1, 64))
	case int64:
		buf.WriteString(strconv.FormatInt(t, 10))
		buf.WriteByte('i')
	case uint64:
		// The unsigned "u" suffix is not portable across line protocol
		// parsers. Write a signed integer when the value fits, else a float.
		if t <= math.MaxInt64 {
			buf.WriteString(strconv.FormatUint(t, 10))
			buf.WriteByte('i')
		} else {
			buf.WriteString(strconv.FormatFloat(float64(t), 'g', -1, 64))
		}
	case bool:
		if t {
			buf.WriteByte('T')
		} else {
			buf.WriteByte('F')
		}
	case string:
		buf.WriteByte('"')
		writeEscaped(buf, t, escString)
		buf.WriteByte('"')
	}
}

type escapeClass int

const (
	escMeasurement escapeClass = iota
	escTag
	escString
)

func writeEscaped(buf *bytes.Buffer, s string, class escapeClass) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		// A newline or a carriage return ends a line. Replace it.
		if c == '\n' || c == '\r' {
			buf.WriteByte(' ')
			continue
		}
		switch class {
		case escMeasurement:
			// A backslash starts an escape sequence for the parser. A raw
			// trailing backslash swallows the delimiter that follows it.
			if c == ',' || c == ' ' || c == '\\' {
				buf.WriteByte('\\')
			}
		case escTag:
			if c == ',' || c == ' ' || c == '=' || c == '\\' {
				buf.WriteByte('\\')
			}
		case escString:
			if c == '"' || c == '\\' {
				buf.WriteByte('\\')
			}
		}
		buf.WriteByte(c)
	}
}
