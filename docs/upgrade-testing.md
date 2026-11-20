# OLM upgrade testing

The `e2e-aws-upgrade` CI job and `make e2e-upgrade-local` check that an
OLM-managed upgrade of the operator keeps existing VPA configuration and
workloads working. Both run the same phases:

1. Install the previous operator bundle with `operator-sdk run bundle`.
2. `make e2e-upgrade-pre`: functional checks against the previous version.
3. Upgrade to the candidate bundle with `operator-sdk run bundle-upgrade`.
4. `make e2e-upgrade-post`: functional checks against the upgraded operator.
5. `make e2e-olm-ci`: the broader upstream VPA e2e suite.

The checks are in `test/e2e-upgrade`. Phases 2 and 4 run as separate
processes (and as separate CI steps). They share state only through the
cluster.

## What the pre-upgrade phase does

* It checks that exactly one CSV is `Succeeded` and that the operator runs
  that CSV's image. It records the CSV name and image.
* It creates the namespace `vpa-upgrade-retained` and fails if it already
  exists.
* It sets non-default tuning values in the `default`
  `VerticalPodAutoscalerController` and waits until the operand deployments
  roll out with the matching args.
* It runs `recreate-workload`, a 2-replica Deployment with a `Recreate`-mode
  VPA. Pods start with requests below the VPA bounds. The check passes when:
  * the recommender publishes the expected recommendation,
  * the updater evicts every original pod (`EvictedByVPA` events), and
  * the admission controller sets the recommended requests on the
    replacement pods (`vpaUpdates` annotation).
* It runs `initial-workload`, a Deployment with an `Initial`-mode VPA. After a
  recommendation exists, a new pod must be created with mutated requests.
* It stores UIDs, the original controller spec and the previous CSV and image
  in the ConfigMap `vpa-upgrade-retained/vpa-upgrade-state`.

Nothing is cleaned up. The post phase needs these resources as they are.

## What the post-upgrade phase does

* The Succeeded CSV must be different from the previous one, and the previous
  CSV must be removed. If `VPA_UPGRADE_TARGET_CSV` is set, the CSV must have
  that name.
* The operator deployment must be fully rolled out on the CSV's image, and
  that image must differ from the previous one. If
  `VPA_UPGRADE_EXPECTED_OPERATOR_IMAGE` is set, it must also match that image
  (compared by digest when one is present).
* All three operand deployments must be fully rolled out on the operator's
  `VPA_OPERAND_IMAGE` and still carry the configured args.
* The controller CR, the namespace, the VPAs and the Deployments must have
  the same UIDs and spec as before the upgrade. A fresh install would fail
  this check.
* The recommendations stored on the retained VPAs were written by the old
  recommender, so both retained VPAs get raised bounds, and the check waits
  for recommendations with the new values, which only the upgraded
  recommender can write.
* For the retained `Recreate` VPA, the updater must then evict every original
  pod (one `EvictedByVPA` event per pod) and the replacements must get the new
  requests.
* New pods of both retained workloads must be mutated to the new values by the
  admission controller.
* A namespace, workload and VPA created after the upgrade must go through the
  same recommend, evict and mutate cycle.

After the checks, diagnostics are collected, the controller spec is restored
(and the operands are waited on until they run with the restored args) for the
`e2e-olm-ci` step that follows, and the test namespaces are deleted. Every
cleanup step runs even if an earlier one fails; the phase fails if any did.

## Bounds and timeouts

The VPAs pin `minAllowed` equal to `maxAllowed`. That keeps the recommendation
target and bounds deterministic, but the recommender still has to process the
VPA and its pods' metrics before it publishes a recommendation. Waits that
depend on the recommender and updater loops are bounded at 15 minutes.
Rollouts and CSV transitions are bounded at 10 minutes. Pod replacement is
bounded at 5 minutes.

Each phase runs through `hack/e2e.sh` (suites `upgrade-pre` and
`upgrade-post`), like the other e2e suites. The Ginkgo timeout (45 minutes)
plus its grace period for cleanup (20 minutes) stays below the `go test`
timeout (75 minutes), so a slow run is stopped by Ginkgo, which still runs
cleanup and writes the JUnit report, rather than killed by `go test`.

Zero webhook downtime during the upgrade is out of scope, so no pod is
created to test admission until the webhook is proven to serve: the suite
repeats a server-side dry-run pod create (admission runs, nothing is stored)
until the returned pod is mutated. After that, a replacement pod admitted
without mutation fails the check immediately.

## Artifacts

All output goes under `$ARTIFACT_DIR` (default `/tmp/workdir`). Each phase
directory holds:

* `junit_upgrade_<phase>.xml`: test results.
* `identities.yaml`: the cluster version and payload image, the CSV, the
  operator and operand images, and the running pods' image IDs.
* `diagnostics/`: OLM objects (CSVs, Subscriptions, InstallPlans,
  CatalogSources, OperatorGroups), the controller CR, VPA webhook
  configurations, deployments, pods, events and VPAs in the operator and test
  namespaces, and operator and operand logs.

`hack/local-upgrade.sh` also writes `upgrade-images.txt` with the bundle and
operator images and the cluster payload.

## Running locally

```sh
PREVIOUS_BUNDLE=<previous bundle image> \
OO_BUNDLE=<candidate bundle image> \
KUBECONFIG=<path> \
make e2e-upgrade-local
```

See the header of `hack/local-upgrade.sh` for the optional variables, such as
`PREVIOUS_OPERATOR` (to repair a previous bundle whose operator image is gone),
`RUN_FULL_E2E` and `SKIP_CLEANUP`. The script stops without changing anything
if the VPA operator namespace already exists. On failure it deletes the test
namespaces and uninstalls the operator, unless `SKIP_CLEANUP=true`. With
`VPA_UPGRADE_SKIP_CLEANUP=true` it skips `make e2e-olm-ci`, because the
controller configuration is left modified.

You can also run the phases one at a time and do the upgrade by hand. Delete
`vpa-upgrade-retained` before re-running the pre phase. If a pre phase failed
after changing the controller configuration, also restore the
`VerticalPodAutoscalerController` spec: the pre phase refuses to start while
the controller still holds its test values, because it would otherwise save
them as the original spec.

```sh
make e2e-upgrade-pre
operator-sdk run bundle-upgrade -n openshift-vertical-pod-autoscaler --security-context-config restricted <bundle>
make e2e-upgrade-post   # set VPA_UPGRADE_SKIP_CLEANUP=true to keep resources for debugging
```

Pods use `registry.k8s.io/pause:3.10` by default. Set
`VPA_UPGRADE_WORKLOAD_IMAGE` on disconnected clusters.
