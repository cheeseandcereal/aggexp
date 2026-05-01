package admission

import (
	"fmt"
	"strings"
)

// mutatePath walks obj along a dotted path (e.g. "spec.title",
// "metadata.annotations.aggexp.io/stamped-at") and applies op with
// value. Parents are created as needed for set; default leaves the
// path alone if the tail exists and is non-nil.
//
// The path grammar is deliberately simple:
//
//   - segments separated by ".".
//   - no array indexing. (YAGNI for this experiment — record in README.)
//   - annotation keys containing "." are a known sharp edge: the
//     naïve splitter would misparse "metadata.annotations.aggexp.io/x".
//     We handle this by special-casing: once the walker enters
//     "metadata.annotations" or "metadata.labels", it treats the
//     entire remainder (joined) as a single map key. Decision
//     recorded in README.
func mutatePath(obj map[string]any, path, op string, value any) error {
	if path == "" {
		return fmt.Errorf("mutation: empty jsonPath")
	}
	parts := splitPath(path)
	return walkAndApply(obj, parts, op, value)
}

// splitPath splits a dotted path respecting the
// metadata.annotations/labels special case described above.
func splitPath(p string) []string {
	raw := strings.Split(p, ".")
	// Special case: paths that begin "metadata.annotations." or
	// "metadata.labels." — everything after that prefix is a
	// single key (annotation/label names may contain "/" and ".").
	if len(raw) >= 3 && raw[0] == "metadata" &&
		(raw[1] == "annotations" || raw[1] == "labels") {
		tail := strings.Join(raw[2:], ".")
		return []string{raw[0], raw[1], tail}
	}
	return raw
}

func walkAndApply(obj map[string]any, parts []string, op string, value any) error {
	if len(parts) == 0 {
		return fmt.Errorf("mutation: empty path parts")
	}
	cur := obj
	for i, part := range parts {
		if i == len(parts)-1 {
			return applyLeaf(cur, part, op, value)
		}
		next, ok := cur[part]
		if !ok || next == nil {
			// For set/default on a missing parent we must create
			// intermediate maps.
			nm := map[string]any{}
			cur[part] = nm
			cur = nm
			continue
		}
		nm, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("mutation: path %q: segment %q is not an object (got %T)", strings.Join(parts, "."), part, next)
		}
		cur = nm
	}
	return nil
}

func applyLeaf(parent map[string]any, key, op string, value any) error {
	switch op {
	case "set":
		parent[key] = value
		return nil
	case "default":
		existing, present := parent[key]
		if present && existing != nil {
			// Strings and maps: treat "" and empty map as absent
			// so `default` fills in more intuitive cases.
			switch v := existing.(type) {
			case string:
				if v != "" {
					return nil
				}
			case map[string]any:
				if len(v) > 0 {
					return nil
				}
			default:
				return nil
			}
		}
		parent[key] = value
		return nil
	default:
		return fmt.Errorf("mutation: unknown op %q (supported: set, default)", op)
	}
}
