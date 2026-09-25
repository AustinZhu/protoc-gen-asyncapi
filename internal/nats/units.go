package nats

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// parseDuration parses a Go duration that may additionally use a "d" (24h)
// unit, e.g. "7d", "1d12h" or "90s".
func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	var total time.Duration
	rest := s
	if i := strings.IndexByte(rest, 'd'); i > 0 {
		days, err := strconv.ParseFloat(rest[:i], 64)
		if err != nil || days < 0 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		total = time.Duration(days * float64(24*time.Hour))
		rest = rest[i+1:]
	}
	if rest != "" {
		d, err := time.ParseDuration(rest)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q (use Go syntax such as \"30s\", \"5m\", \"12h\" or \"7d\")", s)
		}
		total += d
	}
	if total < 0 {
		return 0, fmt.Errorf("invalid duration %q: must not be negative", s)
	}
	return total, nil
}

// sizeUnits follows the NATS configuration conventions where K, KB and KiB
// all denote 1024 bytes.
var sizeUnits = map[string]int64{
	"":  1,
	"b": 1,
	"k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10,
	"m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
	"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30,
	"t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40,
}

// parseSize parses a byte size such as "512", "64KiB", "10MB" or "1G".
// The special value "-1" means unlimited.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	if s == "-1" {
		return -1, nil
	}
	i := strings.IndexFunc(s, func(r rune) bool { return !unicode.IsDigit(r) && r != '.' })
	num, unit := s, ""
	if i >= 0 {
		num, unit = s[:i], strings.ToLower(strings.TrimSpace(s[i:]))
	}
	mult, ok := sizeUnits[unit]
	if !ok || num == "" {
		return 0, fmt.Errorf("invalid size %q (use e.g. \"512\", \"64KiB\", \"10MB\" or \"1GiB\")", s)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return int64(f * float64(mult)), nil
}
