package core

import (
	"strings"
	"unicode"

	"google.golang.org/protobuf/compiler/protogen"
)

// commentText turns a Protobuf comment into Markdown text: the leading space
// of each line is removed, lint directives are dropped and surrounding blank
// lines are trimmed.
func commentText(c protogen.Comments) string {
	lines := strings.Split(string(c), "\n")
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		l = strings.TrimPrefix(strings.TrimRight(l, " \t\r"), " ")
		trimmed := strings.TrimSpace(l)
		if strings.HasPrefix(trimmed, "buf:lint:") || strings.HasPrefix(trimmed, "@exclude") {
			continue
		}
		out = append(out, l)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// describe returns the comment attached to a declaration, combining leading
// and trailing comments.
func describe(loc protogen.CommentSet) string {
	lead, trail := commentText(loc.Leading), commentText(loc.Trailing)
	switch {
	case lead == "":
		return trail
	case trail == "":
		return lead
	default:
		return lead + "\n\n" + trail
	}
}

// summarize splits a description into a summary (first sentence) and the
// remaining description. A description consisting of a single sentence
// becomes the summary only.
func summarize(desc string) (summary, description string) {
	if desc == "" {
		return "", ""
	}
	para := desc
	if i := strings.Index(desc, "\n\n"); i >= 0 {
		para = desc[:i]
	}
	flat := strings.Join(strings.Fields(para), " ")
	first := flat
	for i := 0; i+1 < len(flat); i++ {
		if (flat[i] == '.' || flat[i] == '!' || flat[i] == '?') && flat[i+1] == ' ' {
			first = flat[:i+1]
			break
		}
	}
	if first == flat && para == desc {
		return flat, ""
	}
	return first, desc
}

// snakeCase converts CamelCase identifiers to snake_case.
func snakeCase(s string) string {
	var b strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1]) ||
				(i+1 < len(runes) && unicode.IsLower(runes[i+1]) && unicode.IsUpper(runes[i-1]))) {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// normalizeName folds a name for loose comparisons between subject
// parameters and field names ("orderId" == "order_id" == "order-id").
func normalizeName(s string) string {
	return strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(s))
}
