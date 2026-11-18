// Package upgrade contains the functional OLM upgrade checks for the VPA
// operator. The suite is split into two phases that run as separate processes
// (and separate CI steps) around an OLM-managed operator upgrade:
//
//   - upgrade-pre:  run against the previous operator version. Creates and
//     verifies controller configuration, VPAs and workloads, and records their
//     identities. Nothing is cleaned up.
//   - upgrade-post: run against the upgraded operator. Verifies the upgrade
//     itself, then verifies that the retained resources survived the upgrade
//     and still behave correctly, and that new workloads work too.
//
// Exactly one phase must be selected with --ginkgo.label-filter. Use the
// e2e-upgrade-pre / e2e-upgrade-post Makefile targets rather than running the
// package directly.
package upgrade

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	labelPre  = "upgrade-pre"
	labelPost = "upgrade-post"
)

func TestUpgrade(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "VPA Operator OLM Upgrade Suite")
}

var _ = BeforeSuite(func() {
	// The two phases share state through the cluster and must never run in
	// the same process: running post right after pre would not exercise an
	// upgrade, and running both would delete the retained resources.
	filter := GinkgoLabelFilter()
	Expect(filter).To(BeElementOf(labelPre, labelPost),
		"select exactly one phase with --ginkgo.label-filter=%s or --ginkgo.label-filter=%s "+
			"(use make e2e-upgrade-pre / make e2e-upgrade-post)", labelPre, labelPost)

	var err error
	env, err = newTestEnv()
	Expect(err).NotTo(HaveOccurred(), "create test environment")
})
