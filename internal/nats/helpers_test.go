package nats

import (
	"testing"
	"time"
)

func TestParseSubject(t *testing.T) {
	s, err := parseSubject("orders.{region}.*.{id}.>")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := s.wildcard(), "orders.*.*.*.>"; got != want {
		t.Errorf("wildcard = %q, want %q", got, want)
	}
	if len(s.params) != 2 || s.params[0] != "region" || s.params[1] != "id" {
		t.Errorf("params = %v", s.params)
	}
	for _, bad := range []string{"", "a b", "a..b", ".a", "a.", "a.>.b", "a.b*", "a.{x}y", "a.{x}.{x}", "a.{x.y}", "a.{}"} {
		if _, err := parseSubject(bad); err == nil {
			t.Errorf("parseSubject(%q) succeeded, want error", bad)
		}
	}
}

func TestSubjectSubsetOf(t *testing.T) {
	cases := []struct {
		sub, filter string
		want        bool
	}{
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.*.c", true},
		{"a.*.c", "a.*.c", true},
		{"a.*.c", "a.b.c", false},
		{"a.b.c", "a.>", true},
		{"a.*.>", "a.>", true},
		{"a", "a.>", false},
		{"a.b", "a.b.c", false},
		{"a.b.c", "a.b", false},
		{"a.>", "a.*", false},
		{"a.>", "a.>", true},
		{"$KV.carts.*", "$KV.carts.>", true},
	}
	for _, c := range cases {
		if got := subjectSubsetOf(c.sub, c.filter); got != c.want {
			t.Errorf("subjectSubsetOf(%q, %q) = %v, want %v", c.sub, c.filter, got, c.want)
		}
	}
}

func TestChannelID(t *testing.T) {
	cases := map[string]string{
		"orders.{id}.created": "orders.id.created",
		"$KV.carts.>":         "KV.carts.all",
		"a.*.b":               "a.any.b",
		"$SRV.PING.x":         "SRV.PING.x",
	}
	for in, want := range cases {
		if got := channelID(in); got != want {
			t.Errorf("channelID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseDuration(t *testing.T) {
	cases := map[string]time.Duration{
		"":      0,
		"30s":   30 * time.Second,
		"1h30m": 90 * time.Minute,
		"7d":    7 * 24 * time.Hour,
		"1d12h": 36 * time.Hour,
		"1.5d":  36 * time.Hour,
	}
	for in, want := range cases {
		got, err := parseDuration(in)
		if err != nil || got != want {
			t.Errorf("parseDuration(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"5 minutes", "-1s", "d", "xd"} {
		if _, err := parseDuration(bad); err == nil {
			t.Errorf("parseDuration(%q) succeeded, want error", bad)
		}
	}
}

func TestParseSize(t *testing.T) {
	cases := map[string]int64{
		"":      0,
		"512":   512,
		"-1":    -1,
		"64KiB": 64 << 10,
		"1K":    1 << 10,
		"10MB":  10 << 20,
		"1 GiB": 1 << 30,
		"1.5G":  3 << 29,
		"2tb":   2 << 40,
	}
	for in, want := range cases {
		got, err := parseSize(in)
		if err != nil || got != want {
			t.Errorf("parseSize(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"lots", "1XB", "KB"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) succeeded, want error", bad)
		}
	}
}
