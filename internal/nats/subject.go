package nats

import (
	"fmt"
	"regexp"
	"strings"
)

var paramNameRE = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)

// subject is a parsed NATS subject template such as
// "orders.{region}.{order_id}.created".
type subject struct {
	raw    string
	tokens []string
	params []string
}

// parseSubject validates a subject template. Parameters must span a whole
// token so that the subject can be turned into a NATS wildcard subscription.
func parseSubject(s string) (*subject, error) {
	if s == "" {
		return nil, fmt.Errorf("subject must not be empty")
	}
	if strings.ContainsAny(s, " \t\r\n") {
		return nil, fmt.Errorf("subject %q must not contain whitespace", s)
	}
	out := &subject{raw: s, tokens: strings.Split(s, ".")}
	seen := map[string]bool{}
	for i, tok := range out.tokens {
		switch {
		case tok == "":
			return nil, fmt.Errorf("subject %q has an empty token", s)
		case tok == ">":
			if i != len(out.tokens)-1 {
				return nil, fmt.Errorf("subject %q: the '>' wildcard must be the last token", s)
			}
		case tok == "*":
		case strings.HasPrefix(tok, "{") && strings.HasSuffix(tok, "}"):
			name := tok[1 : len(tok)-1]
			if !paramNameRE.MatchString(name) {
				return nil, fmt.Errorf("subject %q: invalid parameter name %q (allowed: letters, digits, '_' and '-')", s, name)
			}
			if seen[name] {
				return nil, fmt.Errorf("subject %q: parameter %q is used twice", s, name)
			}
			seen[name] = true
			out.params = append(out.params, name)
		case strings.ContainsAny(tok, "{}"):
			return nil, fmt.Errorf("subject %q: parameter in token %q must span the whole token, e.g. \"orders.{id}\"", s, tok)
		case strings.ContainsAny(tok, "*>"):
			return nil, fmt.Errorf("subject %q: wildcard in token %q must span the whole token", s, tok)
		}
	}
	return out, nil
}

// wildcard returns the subject with parameters replaced by "*", i.e. the
// subject a subscriber would listen on.
func (s *subject) wildcard() string {
	toks := make([]string, len(s.tokens))
	for i, t := range s.tokens {
		if strings.HasPrefix(t, "{") {
			t = "*"
		}
		toks[i] = t
	}
	return strings.Join(toks, ".")
}

// hasWildcards reports whether the subject contains parameters or wildcards.
func (s *subject) hasWildcards() bool {
	return s.wildcard() != s.raw || strings.ContainsAny(s.raw, "*>")
}

// subjectSubsetOf reports whether every subject matched by sub is also
// matched by filter (both may contain NATS wildcards).
func subjectSubsetOf(sub, filter string) bool {
	st, ft := strings.Split(sub, "."), strings.Split(filter, ".")
	for i, f := range ft {
		if f == ">" {
			return len(st) > i
		}
		if i >= len(st) {
			return false
		}
		s := st[i]
		switch {
		case s == ">":
			return false
		case f == "*":
		case s == "*" || s != f:
			return false
		}
	}
	return len(st) == len(ft)
}

var nonIDChars = regexp.MustCompile(`[^A-Za-z0-9._\-]+`)

// channelID derives a readable identifier from a subject, e.g.
// "orders.{id}.created" -> "orders.id.created" and "$KV.users.>" -> "KV.users.all".
func channelID(s string) string {
	toks := strings.Split(s, ".")
	for i, t := range toks {
		switch {
		case t == "*":
			t = "any"
		case t == ">":
			t = "all"
		default:
			t = strings.Trim(t, "{}$")
		}
		toks[i] = nonIDChars.ReplaceAllString(t, "_")
	}
	return strings.Join(toks, ".")
}

// joinSubject joins non-empty subject parts with ".".
func joinSubject(parts ...string) string {
	var out []string
	for _, p := range parts {
		if p = strings.Trim(p, "."); p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, ".")
}

func containsString(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}
