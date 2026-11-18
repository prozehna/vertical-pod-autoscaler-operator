#!/bin/bash
#
# Runs the same phases as the e2e-aws-upgrade CI job against the cluster in
# KUBECONFIG:
#
#   install previous bundle -> make e2e-upgrade-pre -> OLM bundle upgrade
#   -> make e2e-upgrade-post -> make e2e-olm-ci
#
# Required environment variables:
#   PREVIOUS_BUNDLE   Bundle image of the previous operator version to install.
#   OO_BUNDLE         Bundle image of the candidate operator version.
#   KUBECONFIG        Kubeconfig with cluster-admin rights on an OpenShift
#                     cluster that does not run the VPA operator yet.
#
# Optional environment variables:
#   PREVIOUS_OPERATOR  Operator image to repair the previous CSV with when the
#                      previous bundle references an image that no longer
#                      exists (as CI does). Unset: the bundle must install as is.
#   VPA_UPGRADE_EXPECTED_OPERATOR_IMAGE
#                      Operator image the candidate CSV must run after upgrade.
#   VPA_UPGRADE_TARGET_CSV
#                      CSV name the upgrade must end on (default: any CSV other
#                      than the previous one).
#   RUN_FULL_E2E       Run make e2e-olm-ci after the post-check (default: true).
#   SKIP_CLEANUP       Leave the operator installed at the end (default: false).
#   OPERATOR_SDK       operator-sdk binary (default: operator-sdk in PATH).
#   ARTIFACT_DIR       Where test reports, identities and diagnostics are
#                      written (default: /tmp/workdir).

set -euo pipefail

: "${PREVIOUS_BUNDLE:?PREVIOUS_BUNDLE must be set}"
: "${OO_BUNDLE:?OO_BUNDLE must be set}"
: "${KUBECONFIG:?KUBECONFIG must be set}"

OPERATOR_SDK=${OPERATOR_SDK:-operator-sdk}
VPA_NS=openshift-vertical-pod-autoscaler
OPERATOR_DEPLOYMENT=vertical-pod-autoscaler-operator
ARTIFACT_DIR=${ARTIFACT_DIR:-/tmp/workdir}
export ARTIFACT_DIR KUBECONFIG
mkdir -p "${ARTIFACT_DIR}"

phase() {
  echo
  echo "=== $* ==="
}

cleanup() {
  local rc=$?
  if [[ "${SKIP_CLEANUP:-false}" == "true" ]]; then
    echo "SKIP_CLEANUP=true: leaving the operator installed"
    return
  fi
  phase "Cleanup"
  "${OPERATOR_SDK}" cleanup vertical-pod-autoscaler -n "${VPA_NS}" || true
  oc delete ns "${VPA_NS}" --ignore-not-found || true
  exit "${rc}"
}
trap cleanup EXIT

# The single CSV in the namespace, or empty.
current_csv() {
  oc get csv -n "${VPA_NS}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true
}

wait_for_csv_succeeded() {
  local csv=$1
  oc wait --timeout=10m --for=jsonpath='{.status.phase}'=Succeeded -n "${VPA_NS}" "csv/${csv}"
  oc wait --timeout=10m --for condition=Available -n "${VPA_NS}" "deployment/${OPERATOR_DEPLOYMENT}"
}

record() {
  echo "$1=$2" | tee -a "${ARTIFACT_DIR}/upgrade-images.txt"
}

phase "Recording inputs"
: > "${ARTIFACT_DIR}/upgrade-images.txt"
record PREVIOUS_BUNDLE "${PREVIOUS_BUNDLE}"
record PREVIOUS_OPERATOR "${PREVIOUS_OPERATOR:-}"
record OO_BUNDLE "${OO_BUNDLE}"
record CLUSTER_VERSION "$(oc get clusterversion version -o jsonpath='{.status.desired.version}')"
record CLUSTER_PAYLOAD "$(oc get clusterversion version -o jsonpath='{.status.desired.image}')"

phase "Installing the previous bundle"
oc create ns "${VPA_NS}"
install_rc=0
"${OPERATOR_SDK}" run bundle --timeout=10m -n "${VPA_NS}" --security-context-config restricted \
  "${PREVIOUS_BUNDLE}" || install_rc=$?

OLD_CSV=$(current_csv)
if [[ -z "${OLD_CSV}" ]]; then
  echo "ERROR: installing ${PREVIOUS_BUNDLE} did not create a CSV (operator-sdk exit code ${install_rc})"
  exit 1
fi
if [[ ${install_rc} -ne 0 ]]; then
  if [[ -z "${PREVIOUS_OPERATOR:-}" ]]; then
    echo "ERROR: installing ${PREVIOUS_BUNDLE} failed and PREVIOUS_OPERATOR is not set to repair it"
    exit 1
  fi
  echo "Install reported an error; repairing CSV ${OLD_CSV} with operator image ${PREVIOUS_OPERATOR}"
  oc patch csv -n "${VPA_NS}" "${OLD_CSV}" --type=json -p "[{\"op\": \"replace\", \
\"path\": \"/spec/install/spec/deployments/0/spec/template/spec/containers/0/image\", \"value\": \"${PREVIOUS_OPERATOR}\"}]"
  sleep 10
  oc delete deployment -n "${VPA_NS}" "${OPERATOR_DEPLOYMENT}" --ignore-not-found
  oc wait --timeout=10m --for=create -n "${VPA_NS}" "deployment/${OPERATOR_DEPLOYMENT}"
fi
wait_for_csv_succeeded "${OLD_CSV}"
record PREVIOUS_CSV "${OLD_CSV}"
record PREVIOUS_RUNNING_OPERATOR "$(oc get deployment -n "${VPA_NS}" "${OPERATOR_DEPLOYMENT}" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"

phase "Pre-upgrade functional checks"
make e2e-upgrade-pre

phase "Upgrading to the candidate bundle"
"${OPERATOR_SDK}" run bundle-upgrade --timeout=10m -n "${VPA_NS}" --security-context-config restricted "${OO_BUNDLE}"

phase "Post-upgrade functional checks"
make e2e-upgrade-post
record CANDIDATE_CSV "$(current_csv)"
record CANDIDATE_RUNNING_OPERATOR "$(oc get deployment -n "${VPA_NS}" "${OPERATOR_DEPLOYMENT}" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"

if [[ "${RUN_FULL_E2E:-true}" == "true" ]]; then
  phase "Broader post-upgrade e2e"
  make e2e-olm-ci
fi

phase "Upgrade test passed"
