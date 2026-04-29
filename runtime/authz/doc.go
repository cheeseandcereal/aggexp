// Package authz provides a reusable external-HTTP-service-backed
// authorizer.Authorizer for aggregated apiservers. It is the
// substrate version of experiment 0003's in-tree authorizer.
//
// Scope: the authorizer opines only on resource requests whose
// APIGroup matches Options.Group (empty string means "all resource
// requests"). Anything else returns NoOpinion, so the library's
// union chain's other authorizers (privileged-groups, health-path
// allow, delegated SAR) continue to handle out-of-scope traffic.
//
// Wire compatibility: the JSON request/response shape is preserved
// from 0003 so existing policy services keep working. Do not change
// field names without a deliberate break and a corresponding
// experiment noting the migration.
package authz
