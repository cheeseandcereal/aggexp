package library

import (
	"crypto/sha256"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	kubetypes "k8s.io/apimachinery/pkg/types"
)

// DeterministicUID produces a UUID-formatted UID from
// SHA256(group/resource/namespace/name). The result is stable across
// pod restarts, eliminating the phantom-reconcile storm that random
// UIDs cause with controller-runtime (FINDINGS/0012, FINDINGS/0035).
//
// Format: 8-4-4-4-12 hex digits (standard UUID layout).
//
// This is a deliberate convention violation (same UID on recreate)
// that is harmless for stateless-projection AAs where name IS identity.
func DeterministicUID(gr schema.GroupResource, namespace, name string) kubetypes.UID {
	input := gr.Group + "/" + gr.Resource + "/" + namespace + "/" + name
	hash := sha256.Sum256([]byte(input))
	return kubetypes.UID(fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16]))
}
