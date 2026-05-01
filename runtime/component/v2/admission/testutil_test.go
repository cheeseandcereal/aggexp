package admission

import "os"

// writeFile is a tiny test helper. Kept in a separate file so the
// test file itself doesn't pull the os import.
func writeFile(path string, content []byte) error {
	return os.WriteFile(path, content, 0o600)
}
