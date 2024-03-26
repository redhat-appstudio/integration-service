/*
Copyright 2023 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions andF
limitations under the License.
*/

package gitops_test

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"

	"github.com/konflux-ci/operator-toolkit/metadata"
	"github.com/redhat-appstudio/integration-service/gitops"
)

var _ = Describe("Snapshot integration test statuses", func() {

	Context("SnapshotTestsStatus", func() {
		const (
			testScenarioName = "test-scenario"
			testDetails      = "test-details"
			namespace        = "default"
			applicationName  = "application-sample"
			componentName    = "component-sample"
			snapshotName     = "snapshot-sample"
			pipelineRunName  = "pipeline-run-abcdf"
		)
		var (
			sits     *intgteststat.SnapshotIntegrationTestStatuses
			snapshot *applicationapiv1alpha1.Snapshot
		)

		BeforeEach(func() {
			var err error
			sits, err = intgteststat.NewSnapshotIntegrationTestStatuses("")
			Expect(err).To(BeNil())

			snapshot = &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      snapshotName,
					Namespace: namespace,
					Labels: map[string]string{
						gitops.SnapshotTypeLabel:               gitops.SnapshotComponentType,
						gitops.SnapshotComponentLabel:          componentName,
						gitops.BuildPipelineRunFinishTimeLabel: "1675992257",
					},
				},
				Spec: applicationapiv1alpha1.SnapshotSpec{
					Application: applicationName,
					Components: []applicationapiv1alpha1.SnapshotComponent{
						{
							Name:           componentName,
							ContainerImage: "quay.io/redhat-appstudio/sample-image:latest",
						},
					},
				},
			}
		})

		It("Creates empty statuses when a snaphost doesn't have test status annotation", func() {
			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
			Expect(err).To(BeNil())
			Expect(statuses.GetStatuses()).To(BeEmpty())
		})

		When("Snapshot contains empty test status annotation", func() {

			BeforeEach(func() {
				err := metadata.SetAnnotation(snapshot, gitops.SnapshotTestsStatusAnnotation, "[]")
				Expect(err).To(BeNil())
			})

			It("Returns empty test statuses", func() {
				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
				Expect(err).To(BeNil())
				Expect(statuses.GetStatuses()).To(BeEmpty())
			})
		})

		When("Snapshot contains valid test status annotation", func() {
			BeforeEach(func() {
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
				testAnnotation, err := json.Marshal(sits)
				Expect(err).To(BeNil())
				err = metadata.SetAnnotation(snapshot, gitops.SnapshotTestsStatusAnnotation, string(testAnnotation))
				Expect(err).To(BeNil())

			})

			It("Returns expected test statuses", func() {
				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
				Expect(err).To(BeNil())
				Expect(statuses.GetStatuses()).To(HaveLen(1))

				statusDetail := statuses.GetStatuses()[0]
				Expect(statusDetail.Status).To(Equal(intgteststat.IntegrationTestStatusInProgress))
				Expect(statusDetail.ScenarioName).To(Equal(testScenarioName))
				Expect(statusDetail.Details).To(Equal(testDetails))
			})

		})

		When("Snapshot contains invalid test status annotation", func() {
			BeforeEach(func() {
				err := metadata.SetAnnotation(
					snapshot, gitops.SnapshotTestsStatusAnnotation, "[{\"invalid\":\"data\"}]")
				Expect(err).To(BeNil())
			})

			It("Returns error", func() {
				_, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
				Expect(err).NotTo(BeNil())
			})
		})

		When("Snapshot contains invalid JSON test status annotation", func() {
			BeforeEach(func() {
				err := metadata.SetAnnotation(snapshot, gitops.SnapshotTestsStatusAnnotation, "{}")
				Expect(err).To(BeNil())
			})

			It("Returns error", func() {
				_, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
				Expect(err).NotTo(BeNil())
			})
		})

		Context("Writes data into snapshot", func() {

			// Make sure that snapshot is written to k8s for following tests
			BeforeEach(func() {
				Expect(k8sClient.Create(ctx, snapshot)).Should(Succeed())

				Eventually(func() error {
					err := k8sClient.Get(ctx, types.NamespacedName{
						Name:      snapshot.Name,
						Namespace: namespace,
					}, snapshot)
					return err
				}, time.Second*10).ShouldNot(HaveOccurred())
			})

			AfterEach(func() {
				err := k8sClient.Delete(ctx, snapshot)
				Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
			})

			It("Test results are written into snapshot", func() {
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)

				err := gitops.WriteIntegrationTestStatusesIntoSnapshot(snapshot, sits, k8sClient, ctx)
				Expect(err).To(BeNil())
				Expect(sits.IsDirty()).To(BeFalse())

				// fetch updated snapshot
				Eventually(func() error {
					if err := k8sClient.Get(ctx, types.NamespacedName{
						Name:      snapshot.Name,
						Namespace: namespace,
					}, snapshot); err != nil {
						return err
					}
					// race condition, sometimes it fetched old object
					annotations := snapshot.GetAnnotations()
					if _, ok := annotations[gitops.SnapshotTestsStatusAnnotation]; ok != true {
						return fmt.Errorf("Snapshot doesn't contain the expected annotation")
					}
					return nil
				}, time.Second*10).ShouldNot(HaveOccurred())

				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
				Expect(err).To(BeNil())
				Expect(statuses.GetStatuses()).To(HaveLen(1))
			})
		})

	})

})
