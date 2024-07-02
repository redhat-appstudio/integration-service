/*
Copyright 2023.

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

package statusreport

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/konflux-ci/integration-service/api/v1beta2"
	"github.com/tonglil/buflogr"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/api/meta"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/konflux-ci/integration-service/loader"
	intgteststat "github.com/konflux-ci/integration-service/pkg/integrationteststatus"
	"github.com/konflux-ci/integration-service/status"
	toolkit "github.com/konflux-ci/operator-toolkit/loader"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/konflux-ci/integration-service/gitops"
	"github.com/konflux-ci/integration-service/helpers"
	"k8s.io/apimachinery/pkg/api/errors"
)

var _ = Describe("Snapshot Adapter", Ordered, func() {
	var (
		adapter *Adapter
		logger  helpers.IntegrationLogger
		buf     bytes.Buffer

		hasComp                 *applicationapiv1alpha1.Component
		hasComp2                *applicationapiv1alpha1.Component
		hasApp                  *applicationapiv1alpha1.Application
		hasSnapshot             *applicationapiv1alpha1.Snapshot
		hasPRSnapshot           *applicationapiv1alpha1.Snapshot
		integrationTestScenario *v1beta2.IntegrationTestScenario
	)
	const (
		SampleRepoLink = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		SampleImage    = "quay.io/redhat-appstudio/sample-image@sha256:841328df1b9f8c4087adbdcfec6cc99ac8308805dea83f6d415d6fb8d40227c1"
		SampleCommit   = "a2ba645d50e471d5f084b"
		SampleRevision = "random-value"
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

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample",
				Application:    "application-sample",
				ContainerImage: SampleImage,
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL:      SampleRepoLink,
							Revision: SampleCommit,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		hasComp2 = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "another-component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "another-component-sample",
				Application:    "application-sample",
				ContainerImage: SampleImage,
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL:      SampleRepoLink,
							Revision: "lastbuiltcommit",
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp2)).Should(Succeed())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp2)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	BeforeEach(func() {
		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:            "component",
					gitops.SnapshotComponentLabel:       hasComp.Name,
					gitops.PipelineAsCodeEventTypeLabel: gitops.PipelineAsCodePushType,
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation: "123",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           hasComp.Name,
						ContainerImage: SampleImage,
					},
				},
			},
		}

		hasPRSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-PR-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:                         "component",
					gitops.SnapshotComponentLabel:                    "component-sample",
					"build.appstudio.redhat.com/pipeline":            "enterprise-contract",
					gitops.PipelineAsCodeEventTypeLabel:              "pull_request",
					"pac.test.appstudio.openshift.io/url-org":        "testorg",
					"pac.test.appstudio.openshift.io/url-repository": "testrepo",
					"pac.test.appstudio.openshift.io/sha":            "testsha",
					gitops.PipelineAsCodeGitProviderLabel:            gitops.PipelineAsCodeGitHubProviderType,
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation:   "123",
					"build.appstudio.redhat.com/commit_sha":         "6c65b2fcaea3e1a0a92476c8b5dc89e92a85f025",
					"appstudio.redhat.com/updateComponentOnSuccess": "false",
					gitops.SnapshotTestsStatusAnnotation:            "[{\"scenario\":\"scenario-1\",\"status\":\"EnvironmentProvisionError\",\"startTime\":\"2023-07-26T16:57:49+02:00\",\"completionTime\":\"2023-07-26T17:57:49+02:00\",\"lastUpdateTime\":\"2023-08-26T17:57:49+02:00\",\"details\":\"Failed to find deploymentTargetClass with right provisioner for copy of existingEnvironment\"}]",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           hasComp.Name,
						ContainerImage: SampleImage,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
								GitSource: &applicationapiv1alpha1.GitSource{
									Revision: SampleRevision,
								},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

		integrationTestScenario = &v1beta2.IntegrationTestScenario{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-pass",
				Namespace: "default",

				Labels: map[string]string{
					"test.appstudio.openshift.io/optional": "false",
				},
			},
			Spec: v1beta2.IntegrationTestScenarioSpec{
				Application: hasApp.Name,
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
	})

	AfterEach(func() {
		err := k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasPRSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, integrationTestScenario)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	When("adapter is created", func() {
		It("can create a new Adapter instance", func() {
			Expect(reflect.TypeOf(NewAdapter(ctx, hasSnapshot, hasApp, logger, loader.NewMockLoader(), k8sClient))).To(Equal(reflect.TypeOf(&Adapter{})))
		})

		It("ensures the statusReport is called", func() {

			ctrl := gomock.NewController(GinkgoT())
			mockReporter := status.NewMockReporterInterface(ctrl)
			mockStatus := status.NewMockStatusInterface(ctrl)

			mockReporter.EXPECT().GetReporterName().Return("mocked_reporter")

			mockStatus.EXPECT().GetReporter(gomock.Any()).Return(mockReporter)
			// ReportSnapshotStatus must be called once
			mockStatus.EXPECT().ReportSnapshotStatus(gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

			mockScenarios := []v1beta2.IntegrationTestScenario{}
			adapter = NewAdapter(ctx, hasPRSnapshot, hasApp, logger, loader.NewMockLoader(), k8sClient)
			adapter.status = mockStatus
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.ApplicationContextKey,
					Resource:   hasApp,
				},
				{
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasPRSnapshot,
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   mockScenarios,
				},
			})
			result, err := adapter.EnsureSnapshotTestStatusReportedToGitProvider()
			fmt.Fprintf(GinkgoWriter, "-------err: %v\n", err)
			fmt.Fprintf(GinkgoWriter, "-------result: %v\n", result)
			Expect(!result.CancelRequest && err == nil).To(BeTrue())
		})
	})

	When("New Adapter is created for a push-type Snapshot that passed all tests", func() {
		BeforeEach(func() {
			buf = bytes.Buffer{}
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			statuses.UpdateTestStatusIfChanged(integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestPassed, "testDetails")
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(ctx, hasSnapshot, statuses, k8sClient)
			Expect(err).To(BeNil())

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
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenario},
				},
				{
					ContextKey: loader.AllSnapshotsContextKey,
					Resource:   []applicationapiv1alpha1.Snapshot{},
				},
			})
		})

		It("ensures Snapshot passed all tests", func() {
			result, err := adapter.EnsureSnapshotFinishedAllTests()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionTrue(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionTrue(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).To(BeTrue())

			expectedLogEntry := "Snapshot integration status condition is finished since all testing pipelines completed"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Snapshot integration status condition marked as passed, all of 1 required Integration PipelineRuns succeeded"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})

		It("testing function findUntriggeredIntegrationTestFromStatus ", func() {

			integrationTestScenarioTest := &v1beta2.IntegrationTestScenario{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-pass-test",
					Namespace: "default",

					Labels: map[string]string{
						"test.appstudio.openshift.io/optional": "false",
					},
				},
				Spec: v1beta2.IntegrationTestScenarioSpec{
					Application: hasApp.Name,
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

			// Check when all integrationTestScenarion exist in testStatuses of the snapshot
			testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			integrationTestScenarios, err := adapter.loader.GetRequiredIntegrationTestScenariosForApplication(adapter.context, adapter.client, adapter.application)
			Expect(err).To(BeNil())
			result := adapter.findUntriggeredIntegrationTestFromStatus(integrationTestScenarios, testStatuses)
			Expect(result).To(BeEmpty())

			// Check when we have one integrationTestScenario not exist in testStatuses of the snapshot
			*integrationTestScenarios = append(*integrationTestScenarios, *integrationTestScenarioTest)

			result = adapter.findUntriggeredIntegrationTestFromStatus(integrationTestScenarios, testStatuses)
			Expect(result).To(BeEquivalentTo("example-pass-test"))

		})

	})

	When("New Adapter is created for a push-type Snapshot that failed one of the tests", func() {
		BeforeEach(func() {
			buf = bytes.Buffer{}
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			statuses.UpdateTestStatusIfChanged(integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestFail, "Failed test")
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(ctx, hasSnapshot, statuses, k8sClient)
			Expect(err).To(BeNil())

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
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenario},
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
			})
			Expect(reflect.TypeOf(adapter)).To(Equal(reflect.TypeOf(&Adapter{})))
		})

		It("ensures Snapshot failed", func() {
			result, err := adapter.EnsureSnapshotFinishedAllTests()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionFalse(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionTrue(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).To(BeTrue())

			expectedLogEntry := "Snapshot integration status condition is finished since all testing pipelines completed"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Snapshot integration status condition marked as failed, some tests within Integration PipelineRuns failed"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})
	})

	When("New Adapter is created for a push-type Snapshot that has no tests", func() {
		BeforeEach(func() {
			buf = bytes.Buffer{}
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).ToNot(HaveOccurred())
			statuses.UpdateTestStatusIfChanged(integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestFail, "Failed test")
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(ctx, hasSnapshot, statuses, k8sClient)
			Expect(err).ToNot(HaveOccurred())

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
					Resource:   []v1beta2.IntegrationTestScenario{},
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
			})
			Expect(reflect.TypeOf(adapter)).To(Equal(reflect.TypeOf(&Adapter{})))
		})

		It("ensures tests report as skipped and overall result is success", func() {
			result, err := adapter.EnsureSnapshotFinishedAllTests()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionTrue(hasSnapshot.Status.Conditions, gitops.AppStudioTestSucceededCondition)).To(BeTrue())

			Expect(meta.FindStatusCondition(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).ToNot(BeNil())
			Expect(meta.IsStatusConditionTrue(hasSnapshot.Status.Conditions, gitops.AppStudioIntegrationStatusCondition)).To(BeTrue())

			expectedLogEntry := "Snapshot integration status condition is finished since there are no required testing pipelines defined for its application"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Snapshot integration status condition marked as passed, all of 0 required Integration PipelineRuns succeeded"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})
	})

	When("New Adapter is created for a push-type Snapshot that passed all tests, but has out-of date components", func() {
		BeforeEach(func() {
			buf = bytes.Buffer{}
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			statuses.UpdateTestStatusIfChanged(integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestPassed, "testDetails")
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(ctx, hasSnapshot, statuses, k8sClient)
			Expect(err).To(BeNil())

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
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp, *hasComp2},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta2.IntegrationTestScenario{*integrationTestScenario},
				},
				{
					ContextKey: loader.AllSnapshotsContextKey,
					Resource:   []applicationapiv1alpha1.Snapshot{},
				},
			})
		})

	})
})
