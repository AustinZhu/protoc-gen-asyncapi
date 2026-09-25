package redis

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)
	eventRE     = regexp.MustCompile(`^[a-z][a-z_-]*$`)
	streamIDRE  = regexp.MustCompile(`^(\$|0|[0-9]+(-[0-9]+)?)$`)
	unsafeIDRE  = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
)

// address is a parsed channel name or key template such as
// "orders:{region}:created".
type address struct {
	raw    string
	params []string
	// glob reports Redis glob characters ('*', '?', '[').
	glob bool
}

// parseAddress validates a channel name or key template.
func parseAddress(s string) (*address, error) {
	if s == "" {
		return nil, fmt.Errorf("channel must not be empty")
	}
	if strings.ContainsAny(s, " \t\r\n") {
		return nil, fmt.Errorf("channel %q must not contain whitespace", s)
	}
	out := &address{raw: s}
	seen := map[string]bool{}
	for rest := s; rest != ""; {
		open := strings.IndexAny(rest, "{}")
		if open < 0 {
			break
		}
		if rest[open] == '}' {
			return nil, fmt.Errorf("channel %q has an unmatched '}'", s)
		}
		end := strings.IndexAny(rest[open+1:], "{}")
		if end < 0 || rest[open+1+end] != '}' {
			return nil, fmt.Errorf("channel %q has an unmatched '{'", s)
		}
		name := rest[open+1 : open+1+end]
		if !paramNameRE.MatchString(name) {
			return nil, fmt.Errorf("channel %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", s, name)
		}
		if seen[name] {
			return nil, fmt.Errorf("channel %q: parameter %q is used twice", s, name)
		}
		seen[name] = true
		out.params = append(out.params, name)
		rest = rest[open+end+2:]
	}
	out.glob = strings.ContainsAny(paramRE.ReplaceAllString(s, ""), "*?[")
	return out, nil
}

var paramRE = regexp.MustCompile(`\{[^{}]*\}`)

// pattern returns the glob a subscriber listens on: parameters become "*".
func (a *address) pattern() string { return paramRE.ReplaceAllString(a.raw, "*") }

// joinKey joins non-empty key segments with ':'.
func joinKey(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, ":"); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ":")
}

// channelID derives a channel id from a channel name or key.
func channelID(s string) string {
	r := strings.NewReplacer("{", "", "}", "", ":", ".", "*", "any", "?", "one", "@", ".")
	id := unsafeIDRE.ReplaceAllString(r.Replace(s), "_")
	id = strings.Trim(strings.ReplaceAll(id, "..", "."), "._")
	if id == "" {
		id = "channel"
	}
	return id
}
