package upgrade

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Retained resources created before the upgrade and verified after it.
const (
	// recreateWorkload is managed by a Recreate-mode VPA: it exercises the
	// recommender, the updater and the admission controller.
	recreateWorkload = "recreate-workload"
	// initialWorkload is managed by an Initial-mode VPA: only the admission
	// controller acts on it, by setting requests on new pods.
	initialWorkload = "initial-workload"
	// newWorkload is created only after the upgrade.
	newWorkloadName = "new-workload"
)

var _ = Describe("VPA operator before the OLM upgrade", Ordered, Label(labelPre), func() {
	var (
		ctx context.Context
		st  *upgradeState
	)

	BeforeAll(func() {
		ctx = context.Background()
		st = &upgradeState{VPAs: map[string]types.UID{}, Workloads: map[string]types.UID{}}
	})

	AfterAll(func() {
		// Diagnostics only. The resources are intentionally retained for the
		// post-upgrade phase.
		env.collectDiagnostics(context.Background(), labelPre, env.retainedNamespace)
	})

	It("runs the previous operator version installed by OLM", func() {
		csv := env.waitForSucceededCSV(ctx, "", "")
		st.OldCSV = csv.GetName()

		operator := env.waitForRollout(ctx, env.operatorNamespace, operatorDeployment)
		st.OldOperatorImage = operator.Spec.Template.Spec.Containers[0].Image
		csvImage, err := csvOperatorImage(csv)
		Expect(err).NotTo(HaveOccurred())
		Expect(st.OldOperatorImage).To(Equal(csvImage), "operator deployment image differs from CSV %s", csv.GetName())

		env.recordIdentities(ctx, labelPre, csv)
	})

	It("starts without leftovers from an earlier run", func() {
		err := env.cl.Get(ctx, types.NamespacedName{Name: env.retainedNamespace}, &corev1.Namespace{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue(),
			"namespace %s already exists; delete it before running the pre-upgrade phase", env.retainedNamespace)
	})

	It("creates the retained namespace and state", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   env.retainedNamespace,
			Labels: map[string]string{testLabelKey: "retained"},
		}}
		Expect(env.cl.Create(ctx, ns)).To(Succeed(), "create namespace")
		st.NamespaceUID = ns.UID
		env.saveState(ctx, st)
	})

	It("applies the controller configuration and rolls it out to the operands", func() {
		Eventually(func() error {
			_, err := env.getController(ctx)
			return err
		}, rolloutTimeout, pollInterval).Should(Succeed(),
			"the operator did not create the %q VerticalPodAutoscalerController", controllerName)

		controller, err := env.getController(ctx)
		Expect(err).NotTo(HaveOccurred())
		// Edge case: if an earlier pre phase failed after patching the
		// controller and was re-run (after deleting the retained namespace),
		// the controller still holds the test values. Saving those as the
		// original would make cleanup "restore" the test values and leave
		// them on the cluster, so refuse to continue instead.
		Expect(InterceptGomegaFailure(func() { expectControllerSpec(Default, controller.Spec) })).To(HaveOccurred(),
			"VerticalPodAutoscalerController %q already holds the test values from an earlier run; "+
				"restore its original spec before re-running the pre-upgrade phase", controllerName)
		st.OriginalControllerSpec = controller.Spec.DeepCopy()
		st.ControllerUID = controller.UID
		env.saveState(ctx, st)

		patch := client.MergeFrom(controller.DeepCopy())
		controller.Spec.SafetyMarginFraction = upgradeControllerSpec.SafetyMarginFraction
		controller.Spec.PodMinCPUMillicores = upgradeControllerSpec.PodMinCPUMillicores
		controller.Spec.PodMinMemoryMb = upgradeControllerSpec.PodMinMemoryMb
		controller.Spec.MinReplicas = upgradeControllerSpec.MinReplicas
		controller.Spec.RecommendationOnly = upgradeControllerSpec.RecommendationOnly
		Expect(env.cl.Patch(ctx, controller, patch)).To(Succeed(), "patch VerticalPodAutoscalerController")

		env.waitForOperands(ctx, &upgradeControllerSpec)
	})

	It("recommends, evicts and mutates pods for a Recreate-mode VPA", func() {
		dep, vpaUID := env.verifyRecreateFlow(ctx, env.retainedNamespace, recreateWorkload, preUpgradeBounds)
		st.Workloads[recreateWorkload] = dep.UID
		st.VPAs[recreateWorkload] = vpaUID
		env.saveState(ctx, st)
	})

	It("mutates new pods for an Initial-mode VPA", func() {
		vpa := env.createVPA(ctx, env.retainedNamespace, initialWorkload, initialWorkload, "Initial", preUpgradeBounds)
		dep := env.createWorkload(ctx, env.retainedNamespace, initialWorkload, 1)
		st.Workloads[initialWorkload] = dep.UID
		st.VPAs[initialWorkload] = vpa.GetUID()
		env.saveState(ctx, st)

		env.waitForRecommendation(ctx, env.retainedNamespace, initialWorkload, preUpgradeBounds)
		env.verifyAdmissionOnNewPod(ctx, dep, preUpgradeBounds.resources())
	})

	It("records that the pre-upgrade phase completed", func() {
		st.Completed = true
		env.saveState(ctx, st)
	})
})

var _ = Describe("VPA operator after the OLM upgrade", Ordered, Label(labelPost), func() {
	var (
		ctx    context.Context
		st     *upgradeState
		newNS  string
		retain = map[string]*appsv1.Deployment{}
	)

	BeforeAll(func() {
		ctx = context.Background()
		st = env.loadState(ctx)
		Expect(st.Completed).To(BeTrue(), "the pre-upgrade phase did not complete; re-run it before the upgrade")
	})

	AfterAll(func() {
		ctx := context.Background()
		env.collectDiagnostics(ctx, labelPost, env.retainedNamespace, newNS)
		if env.skipCleanup {
			GinkgoWriter.Println("VPA_UPGRADE_SKIP_CLEANUP=true, leaving test resources in place")
			return
		}
		// Run every cleanup step even if an earlier one fails, so a stuck
		// namespace cannot leave a modified controller configuration behind
		// for the next CI step. Fail at the end if any step failed.
		var errs []error
		note := func(err error) {
			if err != nil {
				GinkgoWriter.Println("cleanup:", err)
				errs = append(errs, err)
			}
		}
		// Restore the controller configuration first: it is quick and is what
		// the broader e2e suites that run after this phase depend on. Then
		// wait until the operands run with the restored args.
		if st != nil && st.OriginalControllerSpec != nil {
			if err := env.restoreController(ctx, st.OriginalControllerSpec); err != nil {
				note(err)
			} else {
				note(InterceptGomegaFailure(func() { env.waitForOperands(ctx, st.OriginalControllerSpec) }))
			}
		}
		note(env.deleteNamespace(ctx, newNS))
		note(env.deleteNamespace(ctx, env.retainedNamespace))
		Expect(errors.Join(errs...)).NotTo(HaveOccurred(), "post-upgrade cleanup")
	})

	It("completed the OLM upgrade to the target CSV", func() {
		csv := env.waitForSucceededCSV(ctx, env.targetCSV, st.OldCSV)
		By("waiting for OLM to remove the previous CSV " + st.OldCSV)
		Eventually(func(g Gomega) {
			csvs, err := env.listCSVs(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			for i := range csvs {
				g.Expect(csvs[i].GetName()).NotTo(Equal(st.OldCSV), "previous CSV still present (phase %s)",
					csvPhase(&csvs[i]))
			}
		}, rolloutTimeout, pollInterval).Should(Succeed())
		env.recordIdentities(ctx, labelPost, csv)
	})

	It("runs the intended operator image", func() {
		csv := env.waitForSucceededCSV(ctx, env.targetCSV, st.OldCSV)
		csvImage, err := csvOperatorImage(csv)
		Expect(err).NotTo(HaveOccurred())

		operator := env.waitForRollout(ctx, env.operatorNamespace, operatorDeployment)
		image := operator.Spec.Template.Spec.Containers[0].Image
		Expect(image).To(Equal(csvImage), "operator deployment image differs from CSV %s", csv.GetName())
		Expect(image).NotTo(Equal(st.OldOperatorImage), "operator image did not change during the upgrade")
		if env.expectedOperatorImage != "" {
			Expect(imagesMatch(image, env.expectedOperatorImage)).To(BeTrue(),
				"operator runs %s, expected %s", image, env.expectedOperatorImage)
		}
	})

	It("completed the operand rollouts with the expected image and configuration", func() {
		env.waitForOperands(ctx, &upgradeControllerSpec)
	})

	It("retained the controller configuration", func() {
		controller, err := env.getController(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(controller.UID).To(Equal(st.ControllerUID),
			"VerticalPodAutoscalerController %q was recreated during the upgrade", controllerName)
		expectControllerSpec(Default, controller.Spec)
	})

	It("retained the namespace, VPAs and workloads", func() {
		ns := &corev1.Namespace{}
		Expect(env.cl.Get(ctx, types.NamespacedName{Name: env.retainedNamespace}, ns)).To(Succeed())
		Expect(ns.UID).To(Equal(st.NamespaceUID), "namespace was recreated")

		modes := map[string]string{recreateWorkload: "Recreate", initialWorkload: "Initial"}
		for name, mode := range modes {
			vpa, err := env.getVPA(ctx, env.retainedNamespace, name)
			Expect(err).NotTo(HaveOccurred(), "get retained VPA %s", name)
			Expect(vpa.GetUID()).To(Equal(st.VPAs[name]), "VPA %s was recreated", name)
			expectVPASpec(Default, vpa, name, mode, preUpgradeBounds)

			dep, err := env.getDeployment(ctx, env.retainedNamespace, name)
			Expect(err).NotTo(HaveOccurred(), "get retained workload %s", name)
			Expect(dep.UID).To(Equal(st.Workloads[name]), "workload %s was recreated", name)
			retain[name] = dep
		}
	})

	// The recommendations stored on the retained VPAs were written by the old
	// recommender, so reading them proves nothing. Each retained VPA gets new
	// bounds, and the checks wait for recommendations that only the upgraded
	// recommender can have written.

	It("recommends new values for the retained Recreate-mode VPA, then evicts and mutates its pods", func() {
		dep := retain[recreateWorkload]
		pods, err := env.livePods(ctx, dep.Namespace, dep.Spec.Selector.MatchLabels)
		Expect(err).NotTo(HaveOccurred())
		before := podUIDs(pods)

		env.setVPABounds(ctx, dep.Namespace, recreateWorkload, postUpgradeBounds)
		env.waitForRecommendation(ctx, dep.Namespace, recreateWorkload, postUpgradeBounds)
		env.waitForUpdaterEviction(ctx, dep, before, postUpgradeBounds.resources())
	})

	It("recommends new values for the retained Initial-mode VPA", func() {
		env.setVPABounds(ctx, env.retainedNamespace, initialWorkload, postUpgradeBounds)
		env.waitForRecommendation(ctx, env.retainedNamespace, initialWorkload, postUpgradeBounds)
	})

	It("mutates new pods of the retained workloads", func() {
		for _, name := range []string{recreateWorkload, initialWorkload} {
			env.verifyAdmissionOnNewPod(ctx, retain[name], postUpgradeBounds.resources())
		}
	})

	It("recommends, evicts and mutates pods for a VPA created after the upgrade", func() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			GenerateName: "vpa-upgrade-new-",
			Labels:       map[string]string{testLabelKey: "new"},
		}}
		Expect(env.cl.Create(ctx, ns)).To(Succeed(), "create namespace")
		newNS = ns.Name
		env.verifyRecreateFlow(ctx, newNS, newWorkloadName, postUpgradeBounds)
	})
})
