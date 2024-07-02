/*
Copyright 2022 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/tonglil/buflogr"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"

	"github.com/konflux-ci/integration-service/api/v1beta2"
	"github.com/konflux-ci/integration-service/loader"
	"github.com/konflux-ci/integration-service/tekton"
	toolkit "github.com/konflux-ci/operator-toolkit/loader"
	releasev1alpha1 "github.com/konflux-ci/release-service/api/v1alpha1"
	releasemetadata "github.com/konflux-ci/release-service/metadata"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/konflux-ci/integration-service/gitops"
	"github.com/konflux-ci/integration-service/helpers"
	intgteststat "github.com/konflux-ci/integration-service/pkg/integrationteststatus"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Snapshot Adapter", Ordered, func() {
	var (
		adapter *Adapter
		logger  helpers.IntegrationLogger

		testReleasePlan                           *releasev1alpha1.ReleasePlan
		hasApp                                    *applicationapiv1alpha1.Application
		hasComp                                   *applicationapiv1alpha1.Component
		hasCompMissingImageDigest                 *applicationapiv1alpha1.Component
		hasSnapshot                               *applicationapiv1alpha1.Snapshot
		hasSnapshotPR                             *applicationapiv1alpha1.Snapshot
		hasOverRideSnapshot                       *applicationapiv1alpha1.Snapshot
		hasInvalidSnapshot                        *applicationapiv1alpha1.Snapshot
		integrationTestScenario                   *v1beta2.IntegrationTestScenario
		integrationTestScenarioForInvalidSnapshot *v1beta2.IntegrationTestScenario
		env                                       *applicationapiv1alpha1.Environment
	)
	const (
		SampleRepoLink  = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		sample_image    = "quay.io/redhat-appstudio/sample-image"
		sample_revision = "random-value"
		sampleDigest    = "sha256:841328df1b9f8c4087adbdcfec6cc99ac8308805dea83f6d415d6fb8d40227c1"
		customLabel     = "custom.appstudio.openshift.io/custom-label"
	)

	BeforeAll(func() {
		hasApp = &applicationapiv1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "application-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ApplicationSpec{
				DisplayName: "application-sample",
				Description: "This is an example application",
			},
		}
		Expect(k8sClient.Create(ctx, hasApp)).Should(Succeed())

		integrationTestScenario = &v1beta2.IntegrationTestScenario{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-pass",
				Namespace: "default",

				Labels: map[string]string{
					"test.appstudio.openshift.io/optional": "false",
				},

				Annotations: map[string]string{
					"test.appstudio.openshift.io/kind": "kind",
				},
			},
			Spec: v1beta2.IntegrationTestScenarioSpec{
				Application: "application-sample",
				ResolverRef: v1beta2.ResolverRef{
					Resolver: "git",
					Params: []v1beta2.ResolverParameter{
						{
							Name:  "url",
							Value: "https://github.com/redhat-appstudio/integration-examples.git",
						},
						{
							Name:  "revision",
							Value: "main",
						},
						{
							Name:  "pathInRepo",
							Value: "pipelineruns/integration_pipelinerun_pass.yaml",
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, integrationTestScenario)).Should(Succeed())
		helpers.SetScenarioIntegrationStatusAsValid(integrationTestScenario, "valid")

		testReleasePlan = &releasev1alpha1.ReleasePlan{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-releaseplan-",
				Namespace:    "default",
				Labels: map[string]string{
					releasemetadata.AutoReleaseLabel: "true",
				},
			},
			Spec: releasev1alpha1.ReleasePlanSpec{
				Application: hasApp.Name,
				Target:      "default",
			},
		}
		Expect(k8sClient.Create(ctx, testReleasePlan)).Should(Succeed())

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample",
				Application:    "application-sample",
				ContainerImage: "",
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL: SampleRepoLink,
						},
					},
				},
			},
			Status: applicationapiv1alpha1.ComponentStatus{
				LastBuiltCommit: "",
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		hasCompMissingImageDigest = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample-missing-image",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName: "component-sample-missing-image",
				Application:   "application-sample",
			},
		}
		Expect(k8sClient.Create(ctx, hasCompMissingImageDigest)).Should(Succeed())
	})

	BeforeEach(func() {
		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:              "component",
					gitops.SnapshotComponentLabel:         "component-sample",
					"build.appstudio.redhat.com/pipeline": "enterprise-contract",
					gitops.PipelineAsCodeEventTypeLabel:   "push",
					customLabel:                           "custom-label",
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation:   "123",
					"build.appstudio.redhat.com/commit_sha":         "6c65b2fcaea3e1a0a92476c8b5dc89e92a85f025",
					"appstudio.redhat.com/updateComponentOnSuccess": "false",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           "component-sample",
						ContainerImage: sample_image,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
								GitSource: &applicationapiv1alpha1.GitSource{
									Revision: sample_revision,
								},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

		hasSnapshotPR = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshotpr-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:            "component",
					gitops.SnapshotComponentLabel:       "component-sample",
					gitops.PipelineAsCodeEventTypeLabel: "pull_request",
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation: "123",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           "component-sample",
						ContainerImage: sample_image,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshotPR)).Should(Succeed())

		hasOverRideSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshotpr-sample-override",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel: gitops.SnapshotOverrideType,
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           hasComp.Name,
						ContainerImage: sample_image + "@" + sampleDigest,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
								GitSource: &applicationapiv1alpha1.GitSource{
									URL:      SampleRepoLink,
									Revision: sample_revision,
								},
							},
						},
					},
					{
						Name: "nonexisting-component",
					},
					{
						Name:           hasCompMissingImageDigest.Name,
						ContainerImage: sample_image,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
								GitSource: &applicationapiv1alpha1.GitSource{
									URL:      SampleRepoLink,
									Revision: sample_revision,
								},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasOverRideSnapshot)).Should(Succeed())

		Eventually(func() error {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      hasSnapshot.Name,
				Namespace: "default",
			}, hasSnapshot)
			return err
		}, time.Second*10).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		err := k8sClient.Delete(ctx, hasSnapshotPR)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasOverRideSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasCompMissingImageDigest)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, integrationTestScenario)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, testReleasePlan)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	When("adapter is created for Snapshot hasSnapshot", func() {
		var buf bytes.Buffer

		It("can create a new Adapter instance", func() {
			Expect(reflect.TypeOf(NewAdapter(ctx, hasSnapshot, hasApp, logger, loader.NewMockLoader(), k8sClient))).To(Equal(reflect.TypeOf(&Adapter{})))
		})

		It("ensures the integrationTestPipelines are created", func() {
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.SnapshotComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.AllIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenario},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenario},
				},
			})

			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())

			requiredIntegrationTestScenarios, err := adapter.loader.GetRequiredIntegrationTestScenariosForApplication(adapter.context, k8sClient, hasApp)
			Expect(err).ToNot(HaveOccurred())
			Expect(requiredIntegrationTestScenarios).NotTo(BeNil())
			expectedLogEntry := "Creating new pipelinerun for integrationTestscenario integrationTestScenario.Name example-pass"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Snapshot integration status marked as In Progress. Snapshot starts being tested by the integrationPipelineRun"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

			// Snapshot must have InProgress tests
			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).ToNot(HaveOccurred())
			detail, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
			Expect(ok).To(BeTrue())
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusInProgress))

			integrationPipelineRuns := []tektonv1.PipelineRun{}
			Eventually(func() error {
				integrationPipelineRuns, err = getAllIntegrationPipelineRunsForSnapshot(adapter.context, hasSnapshot)
				if err != nil {
					return err
				}

				if expected, got := 1, len(integrationPipelineRuns); expected != got {
					return fmt.Errorf("found %d PipelineRuns, expected: %d", got, expected)
				}
				return nil
			}, time.Second*10).Should(BeNil())

			Expect(integrationPipelineRuns).To(HaveLen(1))
			Expect(k8sClient.Delete(adapter.context, &integrationPipelineRuns[0])).Should(Succeed())

			// It will not re-trigger integration pipelineRuns
			result, err = adapter.EnsureIntegrationPipelineRunsExist()
			expectedLogEntry = "Found existing integrationPipelineRun"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})

		It("ensures global Component Image will not be updated in the PR context", func() {
			adapter.snapshot = hasSnapshotPR

			Eventually(func() bool {
				result, err := adapter.EnsureGlobalCandidateImageUpdated()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			Expect(hasComp.Spec.ContainerImage).To(Equal(""))
			Expect(hasComp.Status.LastBuiltCommit).To(Equal(""))
		})

		It("ensures global Component Image updated when AppStudio Tests failed", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter.logger = log
			err := gitops.MarkSnapshotAsFailed(ctx, k8sClient, hasSnapshot, "test failed")
			Expect(err).To(Succeed())
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeFalse())
			adapter.snapshot = hasSnapshot
			result, err := adapter.EnsureGlobalCandidateImageUpdated()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())

			expectedLogEntry := "Updated .Spec.ContainerImage of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Updated .Status.LastBuiltCommit of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("ensures global Component Image updated when AppStudio Tests succeeded", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter.logger = log
			err := gitops.MarkSnapshotAsPassed(ctx, k8sClient, hasSnapshot, "test passed")
			Expect(err).To(Succeed())
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeTrue())
			adapter.snapshot = hasSnapshot

			result, err := adapter.EnsureGlobalCandidateImageUpdated()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())

			expectedLogEntry := "Updated .Spec.ContainerImage of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Updated .Status.LastBuiltCommit of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      hasSnapshot.Name,
					Namespace: "default",
				}, hasSnapshot)
				return err == nil && gitops.IsSnapshotMarkedAsAddedToGlobalCandidateList(hasSnapshot)
			}, time.Second*10).Should(BeTrue())

			// Check if the adapter function detects that it already promoted the snapshot component
			result, err = adapter.EnsureGlobalCandidateImageUpdated()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())

			expectedLogEntry = "The Snapshot's component was previously added to the global candidate list, skipping adding it."
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("ensures global Component Image updated when AppStudio Tests succeeded for override snapshot", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter.logger = log
			adapter.snapshot = hasOverRideSnapshot

			// don't update Global Candidate List for a snapshot if it is neither component snapshot nor override snapshot
			hasOverRideSnapshot.Labels[gitops.SnapshotTypeLabel] = ""
			result, err := adapter.EnsureGlobalCandidateImageUpdated()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeFalse())
			expectedLogEntry := "The Snapshot was neither created for a single component push event nor override type, not updating the global candidate list."
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

			// update Glocal Candidate List for the component in a override snapshot
			hasOverRideSnapshot.Labels[gitops.SnapshotTypeLabel] = gitops.SnapshotOverrideType
			adapter.snapshot = hasOverRideSnapshot
			result, err = adapter.EnsureGlobalCandidateImageUpdated()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())
			expectedLogEntry = "Updated .Spec.ContainerImage of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Updated .Status.LastBuiltCommit of Global Candidate for the Component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			// don't update Global Candidate List for the component included in a override snapshot but doesn't existw
			expectedLogEntry = "Failed to get component from applicaion, won't update global candidate list for this component"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "containerImage cannot be updated to component Global Candidate List due to invalid digest in containerImage"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("ensures Release created successfully", func() {
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			err := gitops.MarkSnapshotIntegrationStatusAsFinished(ctx, k8sClient, hasSnapshot, "Snapshot integration status condition is finished since all testing pipelines completed")
			Expect(err).ToNot(HaveOccurred())
			err = gitops.MarkSnapshotAsPassed(ctx, k8sClient, hasSnapshot, "test passed")
			Expect(err).To(Succeed())
			Expect(gitops.HaveAppStudioTestsFinished(hasSnapshot)).To(BeTrue())
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeTrue())
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)

			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.AutoReleasePlansContextKey,
					Resource:   []releasev1alpha1.ReleasePlan{*testReleasePlan},
				},
				{
					ContextKey: loader.ReleaseContextKey,
					Resource:   &releasev1alpha1.Release{},
				},
			})

			Eventually(func() bool {
				result, err := adapter.EnsureAllReleasesExist()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      hasSnapshot.Name,
					Namespace: "default",
				}, hasSnapshot)
				return err == nil && gitops.IsSnapshotMarkedAsAutoReleased(hasSnapshot)
			}, time.Second*10).Should(BeTrue())

			// Check if the adapter function detects that it already released the snapshot
			result, err := adapter.EnsureAllReleasesExist()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(result.CancelRequest).To(BeFalse())

			expectedLogEntry := "The Snapshot was previously auto-released, skipping auto-release."
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("ensures Snapshot labels/annotations prefixed with 'appstudio.openshift.io' are propagated to the release", func() {
			releasePlans := []releasev1alpha1.ReleasePlan{*testReleasePlan}
			releaseList := &releasev1alpha1.ReleaseList{}
			err := adapter.createMissingReleasesForReleasePlans(hasApp, &releasePlans, hasSnapshot)

			Expect(err).To(BeNil())

			opts := []client.ListOption{
				client.InNamespace(hasApp.Namespace),
				client.MatchingLabels{
					"appstudio.openshift.io/component": hasComp.Name,
					//"appstudio.openshift.io/application": hasApp.Name,
				},
			}

			Eventually(func() error {
				if err := k8sClient.List(adapter.context, releaseList, opts...); err != nil {
					return err
				}
				if len(releaseList.Items) > 0 {
					Expect(releaseList.Items[0].ObjectMeta.Labels["appstudio.openshift.io/component"]).To(Equal(hasComp.Name))
				}
				return nil
			}, time.Second*10).Should(BeNil())
		})

		It("no action when EnsureAllReleasesExist function runs when AppStudio Tests failed and the snapshot is invalid", func() {
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			// Set the snapshot up for failure by setting its status as failed and invalid
			// as well as marking it as PaC pull request event type
			err := gitops.MarkSnapshotAsFailed(ctx, k8sClient, hasSnapshot, "test failed")
			Expect(err).ShouldNot(HaveOccurred())
			gitops.SetSnapshotIntegrationStatusAsInvalid(hasSnapshot, "snapshot invalid")
			hasSnapshot.Labels[gitops.PipelineAsCodeEventTypeLabel] = gitops.PipelineAsCodePullRequestType
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeFalse())
			Expect(gitops.IsSnapshotValid(hasSnapshot)).To(BeFalse())

			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			Eventually(func() bool {
				result, err := adapter.EnsureAllReleasesExist()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			expectedLogEntry := "The Snapshot won't be released"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "the Snapshot hasn't passed all required integration tests"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "the Snapshot is invalid"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "the Snapshot was created for a PaC pull request event"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("ensures build, PaC, test, and custom labels/annotations are propagated from snapshot to Integration test PLR", func() {
			pipelineRun, err := adapter.createIntegrationPipelineRun(hasApp, integrationTestScenario, hasSnapshot)
			Expect(err).To(BeNil())
			Expect(pipelineRun).ToNot(BeNil())

			// build annotations and labels prefixed with `build.appstudio` are copied
			annotation, found := pipelineRun.GetAnnotations()["build.appstudio.redhat.com/commit_sha"]
			Expect(found).To(BeTrue())
			Expect(annotation).To(Equal("6c65b2fcaea3e1a0a92476c8b5dc89e92a85f025"))

			label, found := pipelineRun.GetLabels()["build.appstudio.redhat.com/pipeline"]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal("enterprise-contract"))

			// Pac labels prefixed with 'pac.test.appstudio.openshift.io' are copied
			_, found = hasSnapshot.GetLabels()[gitops.PipelineAsCodeEventTypeLabel]
			Expect(found).To(BeTrue())
			label, found = pipelineRun.GetLabels()[gitops.PipelineAsCodeEventTypeLabel]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal(hasSnapshot.GetLabels()[gitops.PipelineAsCodeEventTypeLabel]))

			// test labels prefixed with 'test.appstudio.openshift.io' are copied
			_, found = hasSnapshot.GetLabels()[gitops.SnapshotTypeLabel]
			Expect(found).To(BeTrue())
			label, found = pipelineRun.GetLabels()[gitops.SnapshotTypeLabel]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal(hasSnapshot.GetLabels()[gitops.SnapshotTypeLabel]))

			// custom labels prefixed with 'custom.appstudio.openshift.io' are copied
			_, found = hasSnapshot.GetLabels()[customLabel]
			Expect(found).To(BeTrue())
			label, found = pipelineRun.GetLabels()[customLabel]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal(hasSnapshot.GetLabels()[customLabel]))

		})

		It("ensures other labels/annotations are NOT propagated from snapshot to Integration test PLR", func() {
			pipelineRun, err := adapter.createIntegrationPipelineRun(hasApp, integrationTestScenario, hasSnapshot)
			Expect(err).To(BeNil())
			Expect(pipelineRun).ToNot(BeNil())

			// build annotations non-prefixed with 'build.appstudio' are not copied
			_, found := hasSnapshot.GetAnnotations()["appstudio.redhat.com/updateComponentOnSuccess"]
			Expect(found).To(BeTrue())
			_, found = pipelineRun.GetAnnotations()["appstudio.redhat.com/updateComponentOnSuccess"]
			Expect(found).To(BeFalse())
		})

		When("pull request updates repo with integration test", func() {

			const (
				sourceRepoUrl = "https://test-repo.example.com" // is without .git suffix
				sourceRepoRef = "db2c043b72b3f8d292ee0e38768d0a94859a308b"
				targetRepoUrl = "https://github.com/redhat-appstudio/integration-examples" // is without .git suffix
			)

			BeforeEach(func() {
				hasSnapshotPR.Annotations[gitops.SnapshotGitSourceRepoURLAnnotation] = sourceRepoUrl
				hasSnapshotPR.Annotations[gitops.PipelineAsCodeSHAAnnotation] = sourceRepoRef
				hasSnapshotPR.Annotations[gitops.PipelineAsCodeRepoURLAnnotation] = targetRepoUrl
			})

			It("pullrequest repo reference and URL should be used", func() {
				pipelineRun, err := adapter.createIntegrationPipelineRun(hasApp, integrationTestScenario, hasSnapshotPR)
				Expect(err).ToNot(HaveOccurred())
				Expect(pipelineRun).ToNot(BeNil())

				foundUrl := false
				foundRevision := false

				for _, param := range pipelineRun.Spec.PipelineRef.Params {
					if param.Name == tekton.TektonResolverGitParamURL {
						foundUrl = true
						Expect(param.Value.StringVal).To(Equal(sourceRepoUrl + ".git")) // must have .git suffix
					}
					if param.Name == tekton.TektonResolverGitParamRevision {
						foundRevision = true
						Expect(param.Value.StringVal).To(Equal(sourceRepoRef))
					}
				}
				Expect(foundUrl).To(BeTrue())
				Expect(foundRevision).To(BeTrue())
			})
		})

		It("Ensure error is logged when experiencing error when fetching ITS for application", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.AllIntegrationTestScenariosContextKey,
					Err:        fmt.Errorf("not found"),
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Err:        fmt.Errorf("not found"),
				},
			})
			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(buf.String()).Should(ContainSubstring("Failed to get Integration test scenarios for the following application"))
			Expect(buf.String()).Should(ContainSubstring("Failed to get all required IntegrationTestScenarios"))
			Expect(result.CancelRequest).To(BeTrue())
			Expect(result.RequeueRequest).To(BeFalse())
			Expect(err).To(BeNil())
		})

		It("Mark snapshot as pass when required ITS is not found", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   nil,
				},
			})
			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(buf.String()).Should(ContainSubstring("Snapshot marked as successful. No required IntegrationTestScenarios found, skipped testing"))
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeFalse())
			Expect(err).To(BeNil())
		})

		It("Skip integration test for passed Snapshot", func() {
			err := gitops.MarkSnapshotAsPassed(ctx, k8sClient, hasSnapshot, "test pass")
			Expect(err).To(Succeed())
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeTrue())
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
			})
			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(buf.String()).Should(ContainSubstring("The Snapshot has finished testing."))
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeFalse())
			Expect(err == nil).To(BeTrue())
		})
	})

	When("Snapshot is Invalid by way of oversized name", func() {

		BeforeEach(func() {
			hasInvalidSnapshot = &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "this-name-is-too-long-it-has-64-characters-and-we-allow-max-63ch",
					Namespace: "default",
					Labels: map[string]string{
						gitops.SnapshotTypeLabel:              "component",
						gitops.SnapshotComponentLabel:         "component-sample",
						"build.appstudio.redhat.com/pipeline": "enterprise-contract",
						gitops.PipelineAsCodeEventTypeLabel:   "push",
					},
					Annotations: map[string]string{
						gitops.PipelineAsCodeInstallationIDAnnotation:   "123",
						"build.appstudio.redhat.com/commit_sha":         "6c65b2fcaea3e1a0a92476c8b5dc89e92a85f025",
						"appstudio.redhat.com/updateComponentOnSuccess": "false",
					},
				},
				Spec: applicationapiv1alpha1.SnapshotSpec{
					Application: hasApp.Name,
					Components: []applicationapiv1alpha1.SnapshotComponent{
						{
							Name:           "component-sample",
							ContainerImage: sample_image,
							Source: applicationapiv1alpha1.ComponentSource{
								ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
									GitSource: &applicationapiv1alpha1.GitSource{
										Revision: sample_revision,
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, hasInvalidSnapshot)).Should(Succeed())

			integrationTestScenarioForInvalidSnapshot = &v1beta2.IntegrationTestScenario{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invald-snapshot-its",
					Namespace: "default",

					Labels: map[string]string{
						"test.appstudio.openshift.io/optional": "false",
					},
				},
				Spec: v1beta2.IntegrationTestScenarioSpec{
					Application: "application-sample",
					ResolverRef: v1beta2.ResolverRef{
						Resolver: "git",
						Params: []v1beta2.ResolverParameter{
							{
								Name:  "url",
								Value: "https://github.com/redhat-appstudio/integration-examples.git",
							},
							{
								Name:  "revision",
								Value: "main",
							},
							{
								Name:  "pathInRepo",
								Value: "pipelineruns/integration_pipelinerun_pass.yaml",
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, integrationTestScenarioForInvalidSnapshot)).Should(Succeed())
			helpers.SetScenarioIntegrationStatusAsValid(integrationTestScenarioForInvalidSnapshot, "valid")

			Eventually(func() error {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Name:      hasInvalidSnapshot.Name,
					Namespace: "default",
				}, hasInvalidSnapshot)
				return err
			}, time.Second*10).ShouldNot(HaveOccurred())
		})

		AfterEach(func() {
			err := k8sClient.Delete(ctx, hasInvalidSnapshot)
			Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
			err = k8sClient.Delete(ctx, integrationTestScenarioForInvalidSnapshot)
			Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		})

		It("will stop reconciliation", func() {
			var buf bytes.Buffer

			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasInvalidSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)

			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasInvalidSnapshot,
				},
				{
					ContextKey: loader.SnapshotComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.AllIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenarioForInvalidSnapshot},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenarioForInvalidSnapshot},
				},
			})

			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(result.CancelRequest).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())

			expectedLogEntry := "Failed to create pipelineRun for snapshot and scenario"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("Write status to snapshot annotation when meeting invalid resource error", func() {
			var buf bytes.Buffer

			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)

			helpers.SetScenarioIntegrationStatusAsInvalid(integrationTestScenarioForInvalidSnapshot, "invalid")
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.ComponentContextKey,
					Resource:   hasComp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.EnvironmentContextKey,
					Resource:   env,
				},
				{
					ContextKey: loader.SnapshotComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.AllIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenarioForInvalidSnapshot},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenarioForInvalidSnapshot},
				},
			})
			result, err := adapter.EnsureIntegrationPipelineRunsExist()
			Expect(result.CancelRequest).To(BeFalse())
			Expect(err).NotTo(HaveOccurred())

			expectedLogEntry := "IntegrationTestScenario is invalid, will not create pipelineRun for it"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(Succeed())
			detail, ok := statuses.GetScenarioStatus(integrationTestScenarioForInvalidSnapshot.Name)
			Expect(ok).To(BeTrue())
			Expect(detail.Status).Should(Equal(intgteststat.IntegrationTestStatusTestInvalid))
		})
	})

	When("When EnsureAllReleasesExist experiences error", func() {
		var buf bytes.Buffer
		log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

		BeforeAll(func() {
			err := gitops.MarkSnapshotAsPassed(ctx, k8sClient, hasSnapshot, "test passed")
			Expect(err).To(Succeed())
			Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeTrue())
		})

		It("Cancel request when GetAutoReleasePlansForApplication returns an error", func() {
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			// Mock the context with error for AutoReleasePlansContextKey
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.AutoReleasePlansContextKey,
					Err:        fmt.Errorf("Failed to get all ReleasePlans"),
				},
			})

			result, err := adapter.EnsureAllReleasesExist()
			Expect(result.CancelRequest).To(BeFalse())
			Expect(result.RequeueRequest).To(BeTrue())
			Expect(err).To(HaveOccurred())
			Expect(buf.String()).Should(ContainSubstring("Snapshot integration status marked as Invalid. Failed to get all ReleasePlans"))
		})

		It("Returns RequeueWithError if the snapshot is less than three hours old", func() {
			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			testErr := fmt.Errorf("something went wrong with the release")

			result, err := adapter.RequeueIfYoungerThanThreshold(testErr)
			Expect(err).To(HaveOccurred())
			Expect(err).To(Equal(testErr))
			Expect(result).NotTo(BeNil())
			Expect(result.RequeueRequest).To(BeTrue())
		})

		It("Returns ContinueProcessing if the snapshot is greater than or equal to three hours old", func() {
			// Set snapshot creation time to 3 hours ago
			// time.Sub takes a time.Time and returns a time.Duration.  Time.Add takes a time.Duration
			// and returns a time.Time.  Why?  Who knows.  We want the latter, so we add -3 hours here
			hasSnapshot.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * SnapshotRetryTimeout))

			adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
			testErr := fmt.Errorf("something went wrong with the release")

			result, err := adapter.RequeueIfYoungerThanThreshold(testErr)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.RequeueRequest).To(BeFalse())
		})
	})

	Describe("EnsureRerunPipelineRunsExist", func() {

		When("manual re-run of scenario using static env is trigerred", func() {
			BeforeEach(func() {
				var (
					buf bytes.Buffer
				)

				// add rerun label
				// we cannot update it into k8s DB via patch, it would trigger reconciliation in background
				// and test wouldn't test anything
				hasSnapshot.Labels[gitops.SnapshotIntegrationTestRun] = integrationTestScenario.Name

				log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
				adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.ApplicationContextKey,
						Resource:   hasApp,
					},
					{
						ContextKey: loader.ComponentContextKey,
						Resource:   hasComp,
					},
					{
						ContextKey: loader.SnapshotContextKey,
						Resource:   hasSnapshot,
					},
					{
						ContextKey: loader.EnvironmentContextKey,
						Resource:   env,
					},
					{
						ContextKey: loader.SnapshotComponentsContextKey,
						Resource:   []applicationapiv1alpha1.Component{*hasComp},
					},
					{
						ContextKey: loader.GetScenarioContextKey,
						Resource:   integrationTestScenario,
					},
				})
			})

			It("creates integration test in static environemnt", func() {
				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
				Expect(err).To(Succeed())
				_, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
				Expect(ok).To(BeFalse()) // no scenario test yet

				result, err := adapter.EnsureRerunPipelineRunsExist()
				Expect(err).To(Succeed())
				Expect(result.CancelRequest).To(BeFalse())

				statuses, err = gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
				Expect(err).To(Succeed())
				detail, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
				Expect(ok).To(BeTrue()) // test restarted has a status now
				Expect(detail).ToNot(BeNil())
				Expect(detail.TestPipelineRunName).ToNot(BeEmpty()) // must set PLR name to prevent creation of duplicated PLR

				m := MatchKeys(IgnoreExtras, Keys{
					gitops.SnapshotIntegrationTestRun: Equal(integrationTestScenario.Name),
				})
				Expect(hasSnapshot.GetLabels()).ShouldNot(m, "shouln't have re-run label after re-running scenario")

			})
		})

		When("test for scenario is alreday in-progress", func() {

			const (
				fakePLRName string = "pipelinerun-test"
				fakeDetails string = "Lorem ipsum sit dolor mit amet"
			)
			var (
				buf bytes.Buffer
			)

			BeforeEach(func() {
				// mock that test for scenario is already in progress by setting it in annotation
				statuses, err := intgteststat.NewSnapshotIntegrationTestStatuses("")
				Expect(err).To(Succeed())
				statuses.UpdateTestStatusIfChanged(integrationTestScenario.Name, intgteststat.IntegrationTestStatusInProgress, fakeDetails)
				Expect(statuses.UpdateTestPipelineRunName(integrationTestScenario.Name, fakePLRName)).To(Succeed())
				Expect(gitops.WriteIntegrationTestStatusesIntoSnapshot(ctx, hasSnapshot, statuses, k8sClient)).Should(Succeed())

				// add rerun label
				// we cannot update it into k8s DB via patch, it would trigger reconciliation in background
				// and test wouldn't test anything
				hasSnapshot.Labels[gitops.SnapshotIntegrationTestRun] = integrationTestScenario.Name

				log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
				adapter = NewAdapter(ctx, hasSnapshot, hasApp, log, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.ApplicationContextKey,
						Resource:   hasApp,
					},
					{
						ContextKey: loader.ComponentContextKey,
						Resource:   hasComp,
					},
					{
						ContextKey: loader.SnapshotContextKey,
						Resource:   hasSnapshot,
					},
					{
						ContextKey: loader.EnvironmentContextKey,
						Resource:   env,
					},
					{
						ContextKey: loader.SnapshotComponentsContextKey,
						Resource:   []applicationapiv1alpha1.Component{*hasComp},
					},
					{
						ContextKey: loader.GetScenarioContextKey,
						Resource:   integrationTestScenario,
					},
				})
			})

			It("doesn't create new test", func() {
				result, err := adapter.EnsureRerunPipelineRunsExist()
				Expect(err).To(Succeed())
				Expect(result.CancelRequest).To(BeFalse())

				// make sure that test details hasn't changed
				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
				Expect(err).To(Succeed())
				detail, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
				Expect(ok).To(BeTrue())
				Expect(detail.Status).Should(Equal(intgteststat.IntegrationTestStatusInProgress))
				Expect(detail.Details).Should(Equal(fakeDetails))
				Expect(detail.TestPipelineRunName).Should(Equal(fakePLRName))

				m := MatchKeys(IgnoreExtras, Keys{
					gitops.SnapshotIntegrationTestRun: Equal(integrationTestScenario.Name),
				})
				Expect(hasSnapshot.GetLabels()).ShouldNot(m, "shouln't have re-run label after re-running scenario")

			})
		})
	})

})

func getAllIntegrationPipelineRunsForSnapshot(ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) ([]tektonv1.PipelineRun, error) {
	integrationPipelineRuns := &tektonv1.PipelineRunList{}
	opts := []client.ListOption{
		client.InNamespace(snapshot.Namespace),
		client.MatchingLabels{
			"pipelines.appstudio.openshift.io/type": "test",
			"appstudio.openshift.io/snapshot":       snapshot.Name,
		},
	}
	err := k8sClient.List(ctx, integrationPipelineRuns, opts...)

	return integrationPipelineRuns.Items, err
}
