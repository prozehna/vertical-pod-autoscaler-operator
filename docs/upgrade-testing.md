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
* It sets non-default values in the `default` `VerticalPodAutoscalerController`
  and waits until the operand deployments roll out with the matching args.
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
* Recommendations must still be provided for the retained VPAs.
* New pods of both retained workloads must be mutated by the admission
  controller.
* After the retained `Recreate` VPA's bounds are raised, the updater must
  evict its pods and the replacements must get the new requests.
* A namespace, workload and VPA created after the upgrade must go through the
  same recommend, evict and mutate cycle.

After the checks, diagnostics are collected, the test namespaces are deleted,
and the controller spec is restored for the `e2e-olm-ci` step that follows.

## Bounds and timeouts

The VPAs pin `minAllowed` equal to `maxAllowed`. That keeps the recommendation
target and bounds deterministic, but the recommender still has to process the
VPA and its pods' metrics before it publishes a recommendation. Waits that
depend on the recommender and updater loops are bounded at 15 minutes.
Rollouts and CSV transitions are bounded at 10 minutes. Pod replacement is
bounded at 5 minutes.

Zero webhook downtime during the upgrade is out of scope. A replacement pod
admitted without mutation while the webhook is still converging is deleted and
retried within that bound.

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
`RUN_FULL_E2E` and `SKIP_CLEANUP`.

You can also run the phases one at a time and do the upgrade by hand. Delete
`vpa-upgrade-retained` before re-running the pre phase.

```sh
make e2e-upgrade-pre
operator-sdk run bundle-upgrade -n openshift-vertical-pod-autoscaler --security-context-config restricted <bundle>
make e2e-upgrade-post   # set VPA_UPGRADE_SKIP_CLEANUP=true to keep resources for debugging
```

Pods use `registry.k8s.io/pause:3.10` by default. Set
`VPA_UPGRADE_WORKLOAD_IMAGE` on disconnected clusters.
