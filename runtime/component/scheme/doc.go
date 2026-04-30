// Package scheme builds a runtime.Scheme at runtime for a resource
// described by the backend's GetSchemaResponse.
//
// There are two registration modes:
//
//   - Unstructured (the 0013 default): the Scheme registers
//     *unstructured.Unstructured under the target GVK. CRUD, watch,
//     discovery, table rendering, and `kubectl explain` all work.
//     Server-Side Apply does NOT work: the library's SSA path calls
//     scheme.New(gvk) and then scheme.ObjectKinds() on the result,
//     which reads the GVK off the instance for Unstructured. A
//     zero-value *unstructured.Unstructured has an empty GVK, so SSA
//     fails with "unstructured object has no kind" before any
//     managed-fields / structured-merge-diff work happens.
//
//   - Typed wrapper (dyn.Object, introduced by 0017): the Scheme
//     registers *dyn.Object under the target GVK instead. Because
//     dyn.Object is a typed Go struct and does NOT satisfy
//     runtime.Unstructured, Scheme.ObjectKinds attributes the GVK
//     via the typeToGVK map populated by AddKnownTypeWithName, and
//     SSA's empty-object path works. Content stays untyped (Spec /
//     Status live in a map[string]interface{}), so the component
//     server still has no compile-time knowledge of the resource.
//
// Choose the typed-wrapper mode when the backend declares
// supports_server_side_apply=true; the unstructured mode is fine
// for read-only resources and avoids a reflect-walk of the Content
// bag during SSA.
//
// The internal APIVersion is registered too when the typed wrapper
// is selected: the generic apiserver's installer sets
// hubGroupVersion = {group, runtime.APIVersionInternal} and the
// SSA machinery calls toUnversioned against that hub; without an
// internal registration SSA fails with "no kind X is registered for
// the internal version of group Y".
package scheme
