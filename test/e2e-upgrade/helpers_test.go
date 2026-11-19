package upgrade

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	configv1 "github.com/openshift/api/config/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	autoscalingv1 "github.com/openshift/vertical-pod-autoscaler-operator/api/v1"
)

const (
	operatorDeployment    = "vertical-pod-autoscaler-operator"
	recommenderDeployment = "vpa-recommender-default"
	updaterDeployment     = "vpa-updater-default"
	admissionDeployment   = "vpa-admission-plugin-default"
	controllerName        = "default"
	operandImageEnv       = "VPA_OPERAND_IMAGE"

	stateConfigMap = "vpa-upgrade-state"
	stateKey       = "state.json"
	testLabelKey   = "vpa-upgrade-e2e"
	labelTrue      = "true"

	olmGroup      = "operators.coreos.com"
	olmV1Alpha1   = "v1alpha1"
	fieldCPU      = "cpu"
	fieldMemory   = "memory"
	fieldMinAllow = "minAllowed"
	fieldMaxAllow = "maxAllowed"

	// vpaUpdatesAnnotation is set by the VPA admission controller on every
	// pod whose resources it mutated.
	vpaUpdatesAnnotation = "vpaUpdates"
	// evictedByVPAReason is the event reason the VPA updater records when it
	// evicts a pod to apply a recommendation.
	evictedByVPAReason = "EvictedByVPA"

	pollInterval = 10 * time.Second
	// rolloutTimeout bounds operator/operand rollouts and CSV transitions.
	rolloutTimeout = 10 * time.Minute
	// vpaTimeout bounds waits that depend on the recommender and updater
	// loops (1 minute each by default) plus metrics collection latency.
	vpaTimeout = 15 * time.Minute
	// podTimeout bounds waits for workload pods to be replaced and ready.
	podTimeout = 5 * time.Minute
)

var (
	vpaGVK     = schema.GroupVersionKind{Group: "autoscaling.k8s.io", Version: "v1", Kind: "VerticalPodAutoscaler"}
	csvGVK     = schema.GroupVersionKind{Group: olmGroup, Version: olmV1Alpha1, Kind: "ClusterServiceVersion"}
	olmListGVK = []schema.GroupVersionKind{
		{Group: olmGroup, Version: olmV1Alpha1, Kind: "ClusterServiceVersionList"},
		{Group: olmGroup, Version: olmV1Alpha1, Kind: "SubscriptionList"},
		{Group: olmGroup, Version: olmV1Alpha1, Kind: "InstallPlanList"},
		{Group: olmGroup, Version: olmV1Alpha1, Kind: "CatalogSourceList"},
		{Group: olmGroup, Version: "v1", Kind: "OperatorGroupList"},
	}

	// upgradeControllerSpec is applied to the "default"
	// VerticalPodAutoscalerController before the upgrade. Every value differs
	// from the operator defaults so that a reset to defaults (for example by
	// recreating the CR) is detectable after the upgrade. Only fields that
	// exist in the previous operator version are used.
	upgradeControllerSpec = autoscalingv1.VerticalPodAutoscalerControllerSpec{
		SafetyMarginFraction: ptr.To(0.2),
		PodMinCPUMillicores:  ptr.To(30.0),
		PodMinMemoryMb:       ptr.To(260.0),
		MinReplicas:          ptr.To[int64](2),
		RecommendationOnly:   ptr.To(false),
	}
	expectedRecommenderArgs = []string{
		"--recommendation-margin-fraction=0.2",
		"--pod-recommendation-min-cpu-millicores=30",
		"--pod-recommendation-min-memory-mb=260",
	}
	expectedUpdaterArgs = []string{"--min-replicas=2"}

	// Workload pods start with requests well below the VPA bounds so the
	// updater must evict them and the admission controller must raise them.
	initialRequests = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("10m"),
		corev1.ResourceMemory: resource.MustParse("16Mi"),
	}
	// Pinning minAllowed == maxAllowed makes the recommendation target, lower
	// bound and upper bound deterministic regardless of observed usage, while
	// still requiring the recommender to process the VPA and its metrics.
	preUpgradeBounds  = vpaBounds{CPU: "100m", Memory: "300Mi"}
	postUpgradeBounds = vpaBounds{CPU: "150m", Memory: "350Mi"}
)

// testEnv holds the clients and settings shared by both phases.
type testEnv struct {
	cl   client.Client
	kube kubernetes.Interface

	operatorNamespace     string
	retainedNamespace     string
	workloadImage         string
	targetCSV             string
	expectedOperatorImage string
	artifactDir           string
	skipCleanup           bool
}

var env *testEnv

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func newTestEnv() (*testEnv, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(configv1.Install(scheme))
	utilruntime.Must(autoscalingv1.AddToScheme(scheme))

	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("get rest config: %w", err)
	}
	cl, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create clientset: %w", err)
	}

	return &testEnv{
		cl:                    cl,
		kube:                  kube,
		operatorNamespace:     getenv("VPA_NAMESPACE", "openshift-vertical-pod-autoscaler"),
		retainedNamespace:     getenv("VPA_UPGRADE_NAMESPACE", "vpa-upgrade-retained"),
		workloadImage:         getenv("VPA_UPGRADE_WORKLOAD_IMAGE", "registry.k8s.io/pause:3.10"),
		targetCSV:             os.Getenv("VPA_UPGRADE_TARGET_CSV"),
		expectedOperatorImage: os.Getenv("VPA_UPGRADE_EXPECTED_OPERATOR_IMAGE"),
		artifactDir:           getenv("ARTIFACT_DIR", "/tmp/workdir"),
		skipCleanup:           os.Getenv("VPA_UPGRADE_SKIP_CLEANUP") == labelTrue,
	}, nil
}

// upgradeState is what the pre phase hands to the post phase. It is stored
// in a ConfigMap inside the retained namespace, so it survives across CI
// steps and is lost (making the post phase fail) if the namespace is
// recreated.
type upgradeState struct {
	OldCSV                 string                                             `json:"oldCSV"`
	OldOperatorImage       string                                             `json:"oldOperatorImage"`
	OriginalControllerSpec *autoscalingv1.VerticalPodAutoscalerControllerSpec `json:"originalControllerSpec,omitempty"`
	ControllerUID          types.UID                                          `json:"controllerUID"`
	NamespaceUID           types.UID                                          `json:"namespaceUID"`
	VPAs                   map[string]types.UID                               `json:"vpas"`
	Workloads              map[string]types.UID                               `json:"workloads"`
}

func (e *testEnv) saveState(ctx context.Context, st *upgradeState) {
	GinkgoHelper()
	data, err := json.MarshalIndent(st, "", "  ")
	Expect(err).NotTo(HaveOccurred())

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: stateConfigMap, Namespace: e.retainedNamespace}}
	err = e.cl.Get(ctx, client.ObjectKeyFromObject(cm), cm)
	if apierrors.IsNotFound(err) {
		cm.Data = map[string]string{stateKey: string(data)}
		Expect(e.cl.Create(ctx, cm)).To(Succeed(), "create state ConfigMap")
		return
	}
	Expect(err).NotTo(HaveOccurred(), "get state ConfigMap")
	cm.Data = map[string]string{stateKey: string(data)}
	Expect(e.cl.Update(ctx, cm)).To(Succeed(), "update state ConfigMap")
}

func (e *testEnv) loadState(ctx context.Context) *upgradeState {
	GinkgoHelper()
	cm := &corev1.ConfigMap{}
	Expect(e.cl.Get(ctx, types.NamespacedName{Name: stateConfigMap, Namespace: e.retainedNamespace}, cm)).
		To(Succeed(), "the pre-upgrade state ConfigMap %s/%s must exist; was make e2e-upgrade-pre run before the upgrade?",
			e.retainedNamespace, stateConfigMap)
	st := &upgradeState{}
	Expect(json.Unmarshal([]byte(cm.Data[stateKey]), st)).To(Succeed(), "decode pre-upgrade state")
	return st
}

// --- OLM ---------------------------------------------------------------------

func (e *testEnv) listCSVs(ctx context.Context) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(csvGVK.GroupVersion().WithKind("ClusterServiceVersionList"))
	if err := e.cl.List(ctx, list, client.InNamespace(e.operatorNamespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func csvPhase(csv *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(csv.Object, "status", "phase")
	return phase
}

// csvOperatorImage returns the operator container image from the CSV's
// install strategy.
func csvOperatorImage(csv *unstructured.Unstructured) (string, error) {
	deployments, _, err := unstructured.NestedSlice(csv.Object, "spec", "install", "spec", "deployments")
	if err != nil {
		return "", err
	}
	for _, d := range deployments {
		dep, ok := d.(map[string]interface{})
		if !ok || dep["name"] != operatorDeployment {
			continue
		}
		containers, _, err := unstructured.NestedSlice(dep, "spec", "template", "spec", "containers")
		if err != nil || len(containers) == 0 {
			return "", fmt.Errorf("CSV deployment %s has no containers", operatorDeployment)
		}
		image, _, _ := unstructured.NestedString(containers[0].(map[string]interface{}), "image")
		return image, nil
	}
	return "", fmt.Errorf("CSV %s has no install deployment named %s", csv.GetName(), operatorDeployment)
}

// waitForSucceededCSV waits until exactly one CSV for the operator is in the
// Succeeded phase and returns it. If name is non-empty it must match. If
// notName is non-empty the Succeeded CSV must have a different name.
func (e *testEnv) waitForSucceededCSV(ctx context.Context, name, notName string) *unstructured.Unstructured {
	GinkgoHelper()
	var found *unstructured.Unstructured
	Eventually(func(g Gomega) {
		csvs, err := e.listCSVs(ctx)
		g.Expect(err).NotTo(HaveOccurred(), "list CSVs")
		var succeeded []string
		found = nil
		for i := range csvs {
			if csvPhase(&csvs[i]) == "Succeeded" {
				succeeded = append(succeeded, csvs[i].GetName())
				found = &csvs[i]
			}
		}
		g.Expect(succeeded).To(HaveLen(1), "exactly one Succeeded CSV expected in %s", e.operatorNamespace)
		if name != "" {
			g.Expect(found.GetName()).To(Equal(name), "Succeeded CSV")
		}
		if notName != "" {
			g.Expect(found.GetName()).NotTo(Equal(notName), "the previous CSV is still the Succeeded one")
		}
	}, rolloutTimeout, pollInterval).Should(Succeed())
	return found
}

// --- Deployments ------------------------------------------------------------

func (e *testEnv) getDeployment(ctx context.Context, ns, name string) (*appsv1.Deployment, error) {
	dep := &appsv1.Deployment{}
	err := e.cl.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, dep)
	return dep, err
}

// checkRolledOut fails g unless the deployment has observed its latest spec,
// every replica is updated, ready and available, and no old pods remain.
func checkRolledOut(g Gomega, dep *appsv1.Deployment) {
	want := ptr.Deref(dep.Spec.Replicas, 1)
	g.Expect(dep.Status.ObservedGeneration).To(BeNumerically(">=", dep.Generation),
		"%s: spec change not yet observed", dep.Name)
	g.Expect(dep.Status.UpdatedReplicas).To(Equal(want), "%s: updated replicas", dep.Name)
	g.Expect(dep.Status.ReadyReplicas).To(Equal(want), "%s: ready replicas", dep.Name)
	g.Expect(dep.Status.AvailableReplicas).To(Equal(want), "%s: available replicas", dep.Name)
	g.Expect(dep.Status.Replicas).To(Equal(want), "%s: total replicas (old pods still present)", dep.Name)
}

// waitForRollout waits until the deployment is fully rolled out and every
// running pod uses the deployment's current container image.
func (e *testEnv) waitForRollout(ctx context.Context, ns, name string) *appsv1.Deployment {
	GinkgoHelper()
	var dep *appsv1.Deployment
	Eventually(func(g Gomega) {
		var err error
		dep, err = e.getDeployment(ctx, ns, name)
		g.Expect(err).NotTo(HaveOccurred(), "get deployment %s/%s", ns, name)
		checkRolledOut(g, dep)

		pods, err := e.livePods(ctx, ns, dep.Spec.Selector.MatchLabels)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(int(ptr.Deref(dep.Spec.Replicas, 1))), "%s: live pods", name)
		for _, p := range pods {
			g.Expect(p.Spec.Containers[0].Image).To(Equal(dep.Spec.Template.Spec.Containers[0].Image),
				"%s: pod %s runs a stale image", name, p.Name)
		}
	}, rolloutTimeout, pollInterval).Should(Succeed())
	return dep
}

func operandImage(operator *appsv1.Deployment) string {
	for _, e := range operator.Spec.Template.Spec.Containers[0].Env {
		if e.Name == operandImageEnv {
			return e.Value
		}
	}
	return ""
}

// waitForOperands waits for all three operand deployments to be rolled out
// with the operand image configured on the operator, and for their args to
// reflect upgradeControllerSpec.
func (e *testEnv) waitForOperands(ctx context.Context) {
	GinkgoHelper()
	operator := e.waitForRollout(ctx, e.operatorNamespace, operatorDeployment)
	wantImage := operandImage(operator)
	Expect(wantImage).NotTo(BeEmpty(), "operator deployment has no %s env var", operandImageEnv)

	wantArgs := map[string][]string{
		recommenderDeployment: expectedRecommenderArgs,
		updaterDeployment:     expectedUpdaterArgs,
		admissionDeployment:   nil,
	}
	for name, args := range wantArgs {
		By(fmt.Sprintf("waiting for operand %s to roll out with the configured image and args", name))
		Eventually(func(g Gomega) {
			dep, err := e.getDeployment(ctx, e.operatorNamespace, name)
			g.Expect(err).NotTo(HaveOccurred(), "get operand %s", name)
			c := dep.Spec.Template.Spec.Containers[0]
			g.Expect(c.Image).To(Equal(wantImage), "%s image", name)
			for _, a := range args {
				g.Expect(c.Args).To(ContainElement(a), "%s args", name)
			}
		}, rolloutTimeout, pollInterval).Should(Succeed())
		e.waitForRollout(ctx, e.operatorNamespace, name)
	}
}

// --- Controller CR ----------------------------------------------------------

func (e *testEnv) getController(ctx context.Context) (*autoscalingv1.VerticalPodAutoscalerController, error) {
	vpac := &autoscalingv1.VerticalPodAutoscalerController{}
	err := e.cl.Get(ctx, types.NamespacedName{Name: controllerName, Namespace: e.operatorNamespace}, vpac)
	return vpac, err
}

// expectControllerSpec checks the fields set from upgradeControllerSpec.
func expectControllerSpec(g Gomega, spec autoscalingv1.VerticalPodAutoscalerControllerSpec) {
	want := upgradeControllerSpec
	g.Expect(spec.SafetyMarginFraction).To(HaveValue(Equal(*want.SafetyMarginFraction)), "safetyMarginFraction")
	g.Expect(spec.PodMinCPUMillicores).To(HaveValue(Equal(*want.PodMinCPUMillicores)), "podMinCPUMillicores")
	g.Expect(spec.PodMinMemoryMb).To(HaveValue(Equal(*want.PodMinMemoryMb)), "podMinMemoryMb")
	g.Expect(spec.MinReplicas).To(HaveValue(Equal(*want.MinReplicas)), "minReplicas")
	g.Expect(spec.RecommendationOnly).To(HaveValue(BeFalse()), "recommendationOnly")
}

// --- Workloads and VPAs -------------------------------------------------------

type vpaBounds struct {
	CPU    string
	Memory string
}

func (b vpaBounds) resources() corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(b.CPU),
		corev1.ResourceMemory: resource.MustParse(b.Memory),
	}
}

func newWorkload(ns, name string, replicas int32, image string) *appsv1.Deployment {
	labels := map[string]string{"app": name, testLabelKey: labelTrue}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr.To[int64](5),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:      "workload",
						Image:     image,
						Resources: corev1.ResourceRequirements{Requests: initialRequests.DeepCopy()},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}
}

func newVPA(ns, name, target, mode string, b vpaBounds) *unstructured.Unstructured {
	bounds := map[string]interface{}{fieldCPU: b.CPU, fieldMemory: b.Memory}
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": ns,
			"labels":    map[string]interface{}{testLabelKey: labelTrue},
		},
		"spec": map[string]interface{}{
			"targetRef": map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"name":       target,
			},
			"updatePolicy": map[string]interface{}{"updateMode": mode},
			"resourcePolicy": map[string]interface{}{
				"containerPolicies": []interface{}{map[string]interface{}{
					"containerName":       "*",
					"controlledResources": []interface{}{fieldCPU, fieldMemory},
					fieldMinAllow:         bounds,
					fieldMaxAllow:         runtime.DeepCopyJSONValue(bounds),
				}},
			},
		},
	}}
	u.SetGroupVersionKind(vpaGVK)
	return u
}

func (e *testEnv) getVPA(ctx context.Context, ns, name string) (*unstructured.Unstructured, error) {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(vpaGVK)
	err := e.cl.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, u)
	return u, err
}

// setVPABounds updates the pinned minAllowed/maxAllowed of an existing VPA.
func (e *testEnv) setVPABounds(ctx context.Context, ns, name string, b vpaBounds) {
	GinkgoHelper()
	bounds := map[string]interface{}{fieldCPU: b.CPU, fieldMemory: b.Memory}
	patch := map[string]interface{}{"spec": map[string]interface{}{"resourcePolicy": map[string]interface{}{
		"containerPolicies": []interface{}{map[string]interface{}{
			"containerName":       "*",
			"controlledResources": []interface{}{fieldCPU, fieldMemory},
			fieldMinAllow:         bounds,
			fieldMaxAllow:         runtime.DeepCopyJSONValue(bounds),
		}},
	}}}
	data, err := json.Marshal(patch)
	Expect(err).NotTo(HaveOccurred())
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(vpaGVK)
	vpa.SetNamespace(ns)
	vpa.SetName(name)
	Expect(e.cl.Patch(ctx, vpa, client.RawPatch(types.MergePatchType, data))).To(Succeed(), "patch VPA %s bounds", name)
}

// expectVPASpec checks the parts of a retained VPA's spec that the test set.
func expectVPASpec(g Gomega, vpa *unstructured.Unstructured, target, mode string, b vpaBounds) {
	ref, _, _ := unstructured.NestedString(vpa.Object, "spec", "targetRef", "name")
	g.Expect(ref).To(Equal(target), "%s targetRef", vpa.GetName())
	gotMode, _, _ := unstructured.NestedString(vpa.Object, "spec", "updatePolicy", "updateMode")
	g.Expect(gotMode).To(Equal(mode), "%s updateMode", vpa.GetName())
	policies, _, _ := unstructured.NestedSlice(vpa.Object, "spec", "resourcePolicy", "containerPolicies")
	g.Expect(policies).To(HaveLen(1), "%s containerPolicies", vpa.GetName())
	p, _ := policies[0].(map[string]interface{})
	for _, field := range []string{fieldMinAllow, fieldMaxAllow} {
		got, _, _ := unstructured.NestedStringMap(p, field)
		gotCPU, gotMem := resource.MustParse(got[fieldCPU]), resource.MustParse(got[fieldMemory])
		g.Expect(gotCPU.Cmp(resource.MustParse(b.CPU))).To(BeZero(), "%s %s.cpu", vpa.GetName(), field)
		g.Expect(gotMem.Cmp(resource.MustParse(b.Memory))).To(BeZero(),
			"%s %s.memory", vpa.GetName(), field)
	}
}

// vpaRecommendation returns the target, lowerBound and upperBound of the
// first container recommendation, and whether RecommendationProvided is True.
func vpaRecommendation(vpa *unstructured.Unstructured) (map[string]corev1.ResourceList, bool) {
	provided := false
	conds, _, _ := unstructured.NestedSlice(vpa.Object, "status", "conditions")
	for _, c := range conds {
		m, _ := c.(map[string]interface{})
		if m["type"] == "RecommendationProvided" && m["status"] == "True" {
			provided = true
		}
	}
	recs, _, _ := unstructured.NestedSlice(vpa.Object, "status", "recommendation", "containerRecommendations")
	if len(recs) == 0 {
		return nil, provided
	}
	rec, _ := recs[0].(map[string]interface{})
	out := map[string]corev1.ResourceList{}
	for _, field := range []string{"target", "lowerBound", "upperBound"} {
		raw, _, _ := unstructured.NestedStringMap(rec, field)
		rl := corev1.ResourceList{}
		for k, v := range raw {
			if q, err := resource.ParseQuantity(v); err == nil {
				rl[corev1.ResourceName(k)] = q
			}
		}
		out[field] = rl
	}
	return out, provided
}

func expectResources(g Gomega, got, want corev1.ResourceList, what string) {
	for _, r := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		q, ok := got[r]
		g.Expect(ok).To(BeTrue(), "%s: %s missing", what, r)
		w := want[r]
		g.Expect(q.Cmp(w)).To(BeZero(), "%s: %s is %s, want %s", what, r, q.String(), w.String())
	}
}

// waitForRecommendation waits until the recommender has published a
// recommendation whose target and bounds equal the pinned VPA bounds.
func (e *testEnv) waitForRecommendation(ctx context.Context, ns, name string, b vpaBounds) {
	GinkgoHelper()
	By(fmt.Sprintf("waiting for the recommender to publish a recommendation for VPA %s/%s", ns, name))
	Eventually(func(g Gomega) {
		vpa, err := e.getVPA(ctx, ns, name)
		g.Expect(err).NotTo(HaveOccurred(), "get VPA")
		rec, provided := vpaRecommendation(vpa)
		g.Expect(provided).To(BeTrue(), "RecommendationProvided condition is not True")
		g.Expect(rec).NotTo(BeNil(), "no container recommendation")
		for _, field := range []string{"target", "lowerBound", "upperBound"} {
			expectResources(g, rec[field], b.resources(), "recommendation "+field)
		}
	}, vpaTimeout, pollInterval).Should(Succeed())
}

// livePods returns the pods matching labels that are not being deleted.
func (e *testEnv) livePods(ctx context.Context, ns string, labels map[string]string) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := e.cl.List(ctx, list, client.InNamespace(ns), client.MatchingLabels(labels)); err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

func podUIDs(pods []corev1.Pod) sets.Set[types.UID] {
	s := sets.New[types.UID]()
	for _, p := range pods {
		s.Insert(p.UID)
	}
	return s
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podMutatedTo reports whether the admission controller set the pod's
// requests to want.
func podMutatedTo(p *corev1.Pod, want corev1.ResourceList) bool {
	if p.Annotations[vpaUpdatesAnnotation] == "" {
		return false
	}
	req := p.Spec.Containers[0].Resources.Requests
	for _, r := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		got, ok := req[r]
		w := want[r]
		if !ok || got.Cmp(w) != 0 {
			return false
		}
	}
	return true
}

func describePod(p *corev1.Pod) string {
	req := p.Spec.Containers[0].Resources.Requests
	return fmt.Sprintf("%s(cpu=%s memory=%s ready=%t %s=%q)", p.Name, req.Cpu(), req.Memory(),
		podReady(p), vpaUpdatesAnnotation, p.Annotations[vpaUpdatesAnnotation])
}

func (e *testEnv) createWorkload(ctx context.Context, ns, name string, replicas int32) *appsv1.Deployment {
	GinkgoHelper()
	dep := newWorkload(ns, name, replicas, e.workloadImage)
	Expect(e.cl.Create(ctx, dep)).To(Succeed(), "create workload %s/%s", ns, name)
	e.waitForRollout(ctx, ns, name)
	return dep
}

func (e *testEnv) createVPA(
	ctx context.Context, ns, name, target, mode string, b vpaBounds,
) *unstructured.Unstructured {
	GinkgoHelper()
	vpa := newVPA(ns, name, target, mode, b)
	Expect(e.cl.Create(ctx, vpa)).To(Succeed(), "create VPA %s/%s", ns, name)
	return vpa
}

// waitForUpdaterEviction waits until every pod in replacedUIDs has been
// replaced, the replacements carry the admission-controller mutation to
// want, all replicas are ready, and the updater recorded evictions in the
// namespace. Together this shows recommender -> updater -> admission working.
func (e *testEnv) waitForUpdaterEviction(
	ctx context.Context, dep *appsv1.Deployment, replacedUIDs sets.Set[types.UID], want corev1.ResourceList,
) {
	GinkgoHelper()
	By(fmt.Sprintf("waiting for the updater to evict and the admission controller to mutate all pods of %s/%s",
		dep.Namespace, dep.Name))
	Eventually(func(g Gomega) {
		pods, err := e.livePods(ctx, dep.Namespace, dep.Spec.Selector.MatchLabels)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pods).To(HaveLen(int(*dep.Spec.Replicas)), "live pods")
		for i := range pods {
			p := &pods[i]
			g.Expect(replacedUIDs.Has(p.UID)).To(BeFalse(), "pod %s has not been evicted yet", describePod(p))
			g.Expect(podMutatedTo(p, want)).To(BeTrue(), "pod %s was not mutated to the recommendation", describePod(p))
			g.Expect(podReady(p)).To(BeTrue(), "pod %s is not ready", describePod(p))
		}
		g.Expect(e.evictionEvents(ctx, dep.Namespace, replacedUIDs)).NotTo(BeZero(),
			"no %s events recorded for the replaced pods", evictedByVPAReason)
	}, vpaTimeout, pollInterval).Should(Succeed())
}

// evictionEvents counts EvictedByVPA events for the given pods.
func (e *testEnv) evictionEvents(ctx context.Context, ns string, uids sets.Set[types.UID]) (int, error) {
	list := &corev1.EventList{}
	if err := e.cl.List(ctx, list, client.InNamespace(ns)); err != nil {
		return 0, err
	}
	n := 0
	for _, ev := range list.Items {
		if ev.Reason == evictedByVPAReason && uids.Has(ev.InvolvedObject.UID) {
			n++
		}
	}
	return n, nil
}

// verifyAdmissionOnNewPod deletes one pod of the workload and waits until
// its replacement is created, ready, and has its CPU and memory requests set
// by the admission controller to exactly the values in the want parameter
// (the VPA bounds). Callers run
// this only after the admission controller has rolled out, so a replacement
// pod admitted without mutation fails the check immediately rather than
// being retried.
func (e *testEnv) verifyAdmissionOnNewPod(ctx context.Context, dep *appsv1.Deployment, want corev1.ResourceList) {
	GinkgoHelper()
	By(fmt.Sprintf("creating a new pod for %s/%s and checking the admission controller mutates it",
		dep.Namespace, dep.Name))
	pods, err := e.livePods(ctx, dep.Namespace, dep.Spec.Selector.MatchLabels)
	Expect(err).NotTo(HaveOccurred())
	Expect(pods).NotTo(BeEmpty(), "workload %s has no pods", dep.Name)
	original := podUIDs(pods)
	Expect(e.cl.Delete(ctx, &pods[0])).To(Succeed(), "delete pod %s", pods[0].Name)

	Eventually(func(g Gomega) {
		pods, err := e.livePods(ctx, dep.Namespace, dep.Spec.Selector.MatchLabels)
		g.Expect(err).NotTo(HaveOccurred())
		var fresh []*corev1.Pod
		for i := range pods {
			p := &pods[i]
			if original.Has(p.UID) {
				continue
			}
			if !podMutatedTo(p, want) {
				StopTrying(fmt.Sprintf("new pod was admitted without the expected mutation: %s", describePod(p))).Now()
			}
			fresh = append(fresh, p)
		}
		g.Expect(fresh).NotTo(BeEmpty(), "replacement pod not created yet")
		for _, p := range fresh {
			g.Expect(podReady(p)).To(BeTrue(), "new pod %s is not ready", describePod(p))
		}
		g.Expect(pods).To(HaveLen(int(*dep.Spec.Replicas)), "live pods")
	}, podTimeout, pollInterval).Should(Succeed())
}

// verifyRecreateFlow creates a workload, then a Recreate-mode VPA for it, and
// verifies recommendation, updater eviction and admission mutation.
func (e *testEnv) verifyRecreateFlow(
	ctx context.Context, ns, name string, b vpaBounds,
) (*appsv1.Deployment, types.UID) {
	GinkgoHelper()
	dep := e.createWorkload(ctx, ns, name, 2)
	pods, err := e.livePods(ctx, ns, dep.Spec.Selector.MatchLabels)
	Expect(err).NotTo(HaveOccurred())
	before := podUIDs(pods)
	for i := range pods {
		Expect(pods[i].Annotations).NotTo(HaveKey(vpaUpdatesAnnotation),
			"pod %s was mutated before any VPA existed", pods[i].Name)
	}

	vpa := e.createVPA(ctx, ns, name, name, "Recreate", b)
	e.waitForRecommendation(ctx, ns, name, b)
	e.waitForUpdaterEviction(ctx, dep, before, b.resources())
	return dep, vpa.GetUID()
}

// --- Identity / diagnostics ---------------------------------------------------

func (e *testEnv) phaseDir(phase string) string {
	dir := filepath.Join(e.artifactDir, phase)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		GinkgoWriter.Printf("cannot create artifact dir %s: %v\n", dir, err)
	}
	return dir
}

func writeFile(dir, name string, data []byte) {
	if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
		GinkgoWriter.Printf("cannot write %s: %v\n", name, err)
	}
}

func writeYAML(dir, name string, obj interface{}) {
	data, err := yaml.Marshal(obj)
	if err != nil {
		GinkgoWriter.Printf("cannot marshal %s: %v\n", name, err)
		return
	}
	writeFile(dir, name, data)
}

// recordIdentities writes the cluster payload, CSV and operator/operand
// images (including the resolved image IDs of running pods) for the phase.
func (e *testEnv) recordIdentities(ctx context.Context, phase string, csv *unstructured.Unstructured) {
	ids := map[string]interface{}{"phase": phase, "csv": csv.GetName()}
	if img, err := csvOperatorImage(csv); err == nil {
		ids["csvOperatorImage"] = img
	}
	cv := &configv1.ClusterVersion{}
	if err := e.cl.Get(ctx, types.NamespacedName{Name: "version"}, cv); err == nil {
		ids["clusterVersion"] = cv.Status.Desired.Version
		ids["clusterPayload"] = cv.Status.Desired.Image
	}
	images := map[string]interface{}{}
	for _, name := range []string{operatorDeployment, recommenderDeployment, updaterDeployment, admissionDeployment} {
		dep, err := e.getDeployment(ctx, e.operatorNamespace, name)
		if err != nil {
			continue
		}
		entry := map[string]interface{}{"image": dep.Spec.Template.Spec.Containers[0].Image}
		if pods, err := e.livePods(ctx, e.operatorNamespace, dep.Spec.Selector.MatchLabels); err == nil {
			var imageIDs []string
			for _, p := range pods {
				for _, cs := range p.Status.ContainerStatuses {
					imageIDs = append(imageIDs, p.Name+"/"+cs.Name+"="+cs.ImageID)
				}
			}
			entry["podImageIDs"] = imageIDs
		}
		if name == operatorDeployment {
			entry[operandImageEnv] = operandImage(dep)
		}
		images[name] = entry
	}
	ids["deployments"] = images
	writeYAML(e.phaseDir(phase), "identities.yaml", ids)
	data, _ := yaml.Marshal(ids)
	GinkgoWriter.Printf("%s identities:\n%s\n", phase, data)
}

// collectDiagnostics dumps OLM, operator, operand and test workload state
// into $ARTIFACT_DIR/<phase>/diagnostics. It never fails the test.
func (e *testEnv) collectDiagnostics(ctx context.Context, phase string, namespaces ...string) {
	dir := filepath.Join(e.phaseDir(phase), "diagnostics")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		GinkgoWriter.Printf("cannot create diagnostics dir: %v\n", err)
		return
	}
	GinkgoWriter.Printf("collecting diagnostics into %s\n", dir)

	for _, gvk := range olmListGVK {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := e.cl.List(ctx, list, client.InNamespace(e.operatorNamespace)); err == nil {
			writeYAML(dir, strings.ToLower(gvk.Kind)+".yaml", list)
		}
	}
	vpacs := &autoscalingv1.VerticalPodAutoscalerControllerList{}
	if err := e.cl.List(ctx, vpacs, client.InNamespace(e.operatorNamespace)); err == nil {
		writeYAML(dir, "verticalpodautoscalercontrollers.yaml", vpacs)
	}
	webhooks := &unstructured.UnstructuredList{}
	webhooks.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfigurationList",
	})
	if err := e.cl.List(ctx, webhooks); err == nil {
		var vpaHooks []unstructured.Unstructured
		for _, w := range webhooks.Items {
			if strings.Contains(w.GetName(), "vpa") {
				vpaHooks = append(vpaHooks, w)
			}
		}
		writeYAML(dir, "mutatingwebhookconfigurations.yaml", vpaHooks)
	}
	cv := &configv1.ClusterVersion{}
	if err := e.cl.Get(ctx, types.NamespacedName{Name: "version"}, cv); err == nil {
		writeYAML(dir, "clusterversion.yaml", cv)
	}

	for _, ns := range append([]string{e.operatorNamespace}, namespaces...) {
		if ns == "" {
			continue
		}
		prefix := ns + "-"
		deps := &appsv1.DeploymentList{}
		if err := e.cl.List(ctx, deps, client.InNamespace(ns)); err == nil {
			writeYAML(dir, prefix+"deployments.yaml", deps)
		}
		pods := &corev1.PodList{}
		if err := e.cl.List(ctx, pods, client.InNamespace(ns)); err == nil {
			writeYAML(dir, prefix+"pods.yaml", pods)
		}
		events := &corev1.EventList{}
		if err := e.cl.List(ctx, events, client.InNamespace(ns)); err == nil {
			writeYAML(dir, prefix+"events.yaml", events)
		}
		vpas := &unstructured.UnstructuredList{}
		vpas.SetGroupVersionKind(vpaGVK.GroupVersion().WithKind("VerticalPodAutoscalerList"))
		if err := e.cl.List(ctx, vpas, client.InNamespace(ns)); err == nil {
			writeYAML(dir, prefix+"verticalpodautoscalers.yaml", vpas)
		}
		if ns == e.operatorNamespace {
			e.collectLogs(ctx, dir, ns, pods.Items)
		}
	}
}

func (e *testEnv) collectLogs(ctx context.Context, dir, ns string, pods []corev1.Pod) {
	for _, p := range pods {
		for _, c := range p.Spec.Containers {
			opts := &corev1.PodLogOptions{Container: c.Name, TailLines: ptr.To[int64](5000)}
			data, err := e.kube.CoreV1().Pods(ns).GetLogs(p.Name, opts).DoRaw(ctx)
			if err != nil {
				data = []byte(fmt.Sprintf("failed to get logs: %v\n", err))
			}
			writeFile(dir, fmt.Sprintf("logs-%s-%s.log", p.Name, c.Name), data)
		}
	}
}

// deleteNamespace deletes ns and waits (bounded) for it to disappear. It
// returns an error instead of failing, so cleanup can run every step.
func (e *testEnv) deleteNamespace(ctx context.Context, ns string) error {
	if ns == "" {
		return nil
	}
	obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	if err := e.cl.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete namespace %s: %w", ns, err)
	}
	err := wait.PollUntilContextTimeout(ctx, pollInterval, rolloutTimeout, true, func(ctx context.Context) (bool, error) {
		return apierrors.IsNotFound(e.cl.Get(ctx, client.ObjectKeyFromObject(obj), obj)), nil
	})
	if err != nil {
		return fmt.Errorf("namespace %s was not deleted within %s: %w", ns, rolloutTimeout, err)
	}
	return nil
}

// restoreController puts spec back on the VerticalPodAutoscalerController,
// retrying (bounded) on conflicts and transient errors. It returns an error
// instead of failing, so cleanup can run every step.
func (e *testEnv) restoreController(
	ctx context.Context, spec *autoscalingv1.VerticalPodAutoscalerControllerSpec,
) error {
	var lastErr error
	err := wait.PollUntilContextTimeout(ctx, pollInterval, rolloutTimeout, true, func(ctx context.Context) (bool, error) {
		controller, err := e.getController(ctx)
		if err == nil {
			controller.Spec = *spec
			err = e.cl.Update(ctx, controller)
		}
		lastErr = err
		return err == nil, nil
	})
	if err != nil {
		return fmt.Errorf("restore VerticalPodAutoscalerController %q: %w (last error: %v)", controllerName, err, lastErr)
	}
	return nil
}

// imagesMatch reports whether two image references name the same image: the
// strings are identical, or both are pinned by digest (@sha256:...) and the
// digests are equal, even if the registry host or repository path differ. A
// tag reference never matches a digest reference, because the digest behind a
// tag cannot be known without asking the registry.
func imagesMatch(a, b string) bool {
	if a == b {
		return true
	}
	digest := func(ref string) string {
		if i := strings.LastIndex(ref, "@"); i >= 0 {
			return ref[i+1:]
		}
		return ""
	}
	return digest(a) != "" && digest(a) == digest(b)
}
