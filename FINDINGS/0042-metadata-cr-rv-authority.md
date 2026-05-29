# FINDINGS: 0042-metadata-cr-rv-authority

## What we were trying to learn

The 0025 experiment surfaced a resourceVersion-authority *split* in the
stitched-store model: Get/List returned backend-supplied RVs while
Watch returned middleware-counter RVs, which makes reflector
relist-with-RV semantically inconsistent. 0034 later showed that the
host CRD's etcd RV is a sound single authority for cross-replica watch
— but only over a *whole-object* storage CRD, where the entire object
lives in the CRD. 0042 asks the open question those two left: in the
0024 *stitched* model (KRM metadata on a cluster-scoped CR, business
body on a separate backend), is the host etcd RV of the per-object
metadata CR a sound single RV authority for the whole stitched object
across Get, List, and Watch, in a multi-replica deployment?

## What we did

Built a 3-replica StatefulSet aggregated apiserver serving
`aggexp.io/v1` Widget. The KRM metadata for each Widget lives on a
cluster-scoped CRD `resourcemetadatas.widgetmeta.aggexp.io/v1`; each
replica runs a dynamic informer on it. A custom `rest.Storage`
(`pkg/widgetrest`) stitches the metadata Record onto the business body
and stamps every served object's `metadata.resourceVersion` with the
metadata CR's host etcd RV — never a backend RV, never a per-replica
counter. The metadata-CR informer drives the watch broadcaster; List
stamps `ListMeta.resourceVersion` from the informer high-water mark;
unknown resume RVs replay current list-state rather than 410 (the 0034
contract).

We ran four scenarios: RV identity (Get/List/Watch RV == metadata-CR
RV), cross-replica resume by RV, multi-replica read/list consistency,
and propagation latency / stitch overhead. We used per-pod Services and
`hack/pin-replica.sh` to route all kubectl traffic to a chosen replica.

## What we observed

**RV identity holds exactly.** A freshly created `w1` returned
`metadata.resourceVersion=1272`, identical to the metadata CR's own
`metadata.resourceVersion` and with a matching UID. The list-level
`metadata.resourceVersion` (read from the raw API) was also `1272`,
and the single item's RV was `1272`. No backend RVs and no counter
values appeared anywhere.

**Cross-replica read consistency is perfect** — *after a design
change* (see "What surprised us"). All three replicas, pinned in turn,
returned byte-identical objects: same RV, same UID, same body. A
cluster-wide list pinned to each replica returned an identical
high-water `listRV` (1451) and an identical set of per-object RVs.

**Cross-replica resume by RV works without 410.** We captured
`listRV=1272` from replica 0, wrote `w2` through replica 1, then
resumed a watch against replica 2 with `resourceVersion=1272`. No 410
Gone. Replica 2 replayed `w1` (rv 1272) as an ADDED prefix and *also*
delivered `w2` (rv 1322) — a write that had hit a different replica
than the one minting the resume RV and a different replica than the
one serving the resumed watch. This is the 0025 split closed for the
stitched model: every replica interprets the same etcd RV stream
identically.

**Cross-replica propagation latency is effectively zero.** Measured
from the writer replica's `metastore-create` log line (the dynamic
write returning) to a non-writer replica's `metastore-informer-event`
log line, across five objects: −0.06 ms to +0.14 ms. All replicas'
metadata informers (including the writer's own) receive the event from
the same etcd watch stream within tens of microseconds; one sample of
`p4` showed the non-writer observing *before* the writer's own create
log flushed.

**Stitch overhead is sub-millisecond and invisible end-to-end.**
Per-Get wall time through the aggregation layer was ~61 ms, but so was
a direct `kubectl get` of the metadata CR on the host (~64 ms) — both
are dominated by kubectl process startup + TLS, not the stitch. The
stitch itself is two informer-cache reads (metadata + body) and a
struct overlay; it does not register against the kubectl/TLS baseline.
This is consistent with 0024's ~3–5 ms estimate once the per-process
kubectl overhead is excluded.

## What surprised us

**A per-replica in-memory body backend does not compose with
cross-replica consistency, and this is the central tension of the
stitched model — not a detail.** The original sketch (and the README's
architecture diagram) put the Widget body in a per-replica in-memory
map. We confirmed empirically that this breaks: a `kubectl apply` of
`w1` landed on replica 0, whose in-memory map then held the body;
replicas 1 and 2 learned the *metadata* via their informers but had no
*body*, so `kubectl get w1` pinned to replica 1 or 2 returned 404 even
though the metadata CR (and its RV) was fully replicated. The
metadata-CR RV authority is sound, but RV authority alone does not make
an object *readable* on a replica that never saw the body.

We resolved this by moving the body to a second shared cluster-scoped
CRD (`widgetbodies.widgetbody.aggexp.io/v1`), read by every replica via
its own informer. Crucially the body store stays **RV-blind**: the body
CR's resourceVersion is read and discarded; only the metadata CR's RV
is surfaced. After the change, all three replicas serve identical
objects. This means the 0024 stitch composes with the 0034
cross-replica informer pattern only if *both* halves (metadata and
body) are reachable from every replica — i.e. both must be host-backed
(CRD/etcd) or otherwise replicated. A genuinely node-local body
backend (in-memory, local disk) is incompatible with multi-replica
read consistency regardless of how clean the RV authority is.

**The two RV streams must not be conflated.** With two CRDs there are
two independent etcd RV streams (the metadata stream and the body
stream). The discipline that makes the experiment work is that exactly
one of them — the metadata CR's — is ever surfaced. The body CR's RV is
strictly internal. If the body CR's RV had leaked into a served
object, the 0025 split would have reappeared in a new disguise (two
authorities again).

## Which fundamentals this touched

**Watch and consistency semantics (primary).** This closes the
`rv-authority-unification` candidate for the stitched model and
confirms the 0025 split is resolvable: a single host-CRD RV authority,
uniformly stamped on Get/List/Watch and carried unchanged through the
informer-driven broadcaster, gives consistent cross-replica
resume-by-RV. The 0034 result generalizes from whole-object storage to
the stitched model, with the added requirement that the body be
host-reachable. The "no 410, replay list-state" resume contract from
0034 carried over unchanged and is what makes cross-replica resume
succeed.

**Storage independence (secondary).** The stitch (metadata in one CR,
body in another) does compose with multi-replica deployment, but the
experiment refines the claim: storage independence for a multi-replica
AA requires the storage to be *shared*, not merely *separate*. A
separate-but-node-local body store is not storage-independent in the
multi-replica sense — it reintroduces a per-replica state dependency
that the aggregated-API architecture is supposed to avoid.

## Consequents

- The ~61 ms per-Get wall time is entirely kubectl process startup +
  TLS handshake; it is a measurement-harness artifact, not a property
  of the stitch. Tied to using `kubectl` as the client.
- `kubectl get -o jsonpath='{.metadata.resourceVersion}'` on a *list*
  returns empty; the list RV is present in the raw API response. Same
  kubectl formatter consequent 0034 noted.
- The 30 s informer resync fires MODIFIED events (with unchanged RV)
  for all cached objects; these are visible on a raw `kubectl get -w`
  as a periodic batch of MODIFIEDs. A SharedInformer client
  deduplicates them (same RV = no DeltaFIFO delta). Same consequent as
  0034; tied to the chosen 30 s resync.
- `insecureSkipTLSVerify: true` on the APIService is a lab convenience
  to avoid per-pod-SAN cert regeneration during replica pinning. Tied
  to the pinning test approach, not to the architecture. (0034 instead
  regenerated certs with per-pod SANs; either works.)
- Numeric string comparison of RVs (`strconv.ParseUint`) works because
  these are etcd integer RVs; the Kubernetes API treats RVs as opaque,
  so this is an environment-specific shortcut, not a general technique.

## What this changes for SYNTHESIS and EXPERIMENTS

For SYNTHESIS, the Watch and consistency semantics section should now
state that host-CRD RV authority generalizes from whole-object storage
(0034) to the stitched metadata/body split (0042), and that the 0025
RV-authority split is resolvable by designating the metadata CR's etcd
RV as the sole authority. The storage-independence section should
record the refinement surfaced here: multi-replica read consistency
requires *shared* storage for every stitched component, and a
separate-but-node-local backend reintroduces a per-replica state
dependency. The `rv-authority-unification` candidate can be marked
closed for the stitched model.

For EXPERIMENTS, 0043/0044/0045 inherit this skeleton (two shared CRDs
+ metadata-CR RV authority + per-replica informers). They should not
re-derive the per-replica-body failure; it is settled here. The open
question of whether the *substrate* could offer a pass-through-RV mode
(rather than each experiment writing a parallel `rest.Storage`) remains
from 0034 and is unaffected.

## Open questions

- With two independent etcd RV streams, is there any client-visible
  anomaly when the body CR and metadata CR for the same object are
  written in separate transactions (they are: body first, then
  metadata)? In this experiment the metadata write is last and its RV
  is the authority, so a watcher never sees a metadata RV for which the
  body is missing on the *writer* replica — but a *reader* replica
  could momentarily have the metadata informer ahead of the body
  informer. We mitigated with a direct-read fallback in
  `StitchForRef`/`Get`; whether that fallback is ever exercised under
  sustained load is uncharacterized.
- The body CR write adds a second host round-trip per Widget write
  (body CR + metadata CR = two etcd writes). 0047 (host-etcd-write
  ceiling) is the place to characterize whether the doubled write
  amplification matters at scale.
- SSA / field-manager behavior under the two-CRD split is untested
  here (managedFields are persisted on the metadata CR and stitched
  back, and a round-trip through `kubectl apply` preserved them, but
  concurrent multi-replica SSA with competing field managers is
  uncharacterized — same gap 0034 left).
