package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Repo is a cluster-scoped projection of a GitHub repository. State
// lives on GitHub; this apiserver is stateless except for a polling
// cache. Resource name is <owner>.<repo-name>.
type Repo struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RepoSpec   `json:"spec,omitempty"`
	Status RepoStatus `json:"status,omitempty"`
}

// RepoSpec mirrors the relevant fields of a GitHub repo. All fields
// are derived from the GitHub API; the AA does not accept writes to
// this spec (read-only for the MVP).
type RepoSpec struct {
	// Owner is the GitHub user or organization.
	Owner string `json:"owner,omitempty"`
	// Name is the repository name (without the owner prefix).
	Name string `json:"name,omitempty"`
	// Description is the repo's short description on GitHub.
	Description string `json:"description,omitempty"`
	// DefaultBranch is the default branch (e.g. "main").
	DefaultBranch string `json:"defaultBranch,omitempty"`
	// Private is true for private repos.
	Private bool `json:"private,omitempty"`
	// Language is the primary language as reported by GitHub.
	Language string `json:"language,omitempty"`
	// Stars is the number of stargazers.
	Stars int32 `json:"stars,omitempty"`
	// HTMLURL is the human-facing URL on GitHub.
	HTMLURL string `json:"htmlURL,omitempty"`
}

// RepoStatus captures the server's observation of the backing
// GitHub resource.
type RepoStatus struct {
	// ObservedAt is the wall-clock time this Repo was last
	// refreshed from GitHub.
	ObservedAt metav1.Time `json:"observedAt,omitempty"`
	// ETag is the GitHub response ETag at last observation, if any.
	ETag string `json:"etag,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RepoList is a list of Repo objects.
type RepoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []Repo `json:"items"`
}
