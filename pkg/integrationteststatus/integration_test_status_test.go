/*
Copyright 2023 Red Hat Inc.

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

package integrationteststatus_test

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
)

var _ = Describe("integrationteststatus library unittests", func() {

	Describe("IntegrationTestStatus type tests", func() {
		DescribeTable("Status to string and vice versa",
			func(st intgteststat.IntegrationTestStatus, expectedStr string) {
				strRepr := st.String()
				Expect(strRepr).To(Equal(expectedStr))
				controlStatus, err := intgteststat.IntegrationTestStatusString(strRepr)
				Expect(err).To(BeNil())
				Expect(controlStatus).To(Equal(st))
			},
			Entry("When status is Pending", intgteststat.IntegrationTestStatusPending, "Pending"),
			Entry("When status is InProgress", intgteststat.IntegrationTestStatusInProgress, "InProgress"),
			Entry("When status is EnvironmentProvisionError", intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, "EnvironmentProvisionError"),
			Entry("When status is DeploymentError", intgteststat.IntegrationTestStatusDeploymentError_Deprecated, "DeploymentError"),
			Entry("When status is TestFail", intgteststat.IntegrationTestStatusTestFail, "TestFail"),
			Entry("When status is TestPass", intgteststat.IntegrationTestStatusTestPassed, "TestPassed"),
			Entry("When status is Deleted", intgteststat.IntegrationTestStatusDeleted, "Deleted"),
			Entry("When status is Invalid", intgteststat.IntegrationTestStatusTestInvalid, "TestInvalid"),
		)

		DescribeTable("Status to JSON and vice versa",
			func(st intgteststat.IntegrationTestStatus, expectedStr string) {
				jsonRepr, err := st.MarshalJSON()
				Expect(err).To(BeNil())
				Expect(jsonRepr).To(Equal([]byte("\"" + expectedStr + "\"")))

				var controlStatus intgteststat.IntegrationTestStatus
				err = controlStatus.UnmarshalJSON(jsonRepr)
				Expect(err).To(BeNil())
				Expect(controlStatus).To(Equal(st))
			},
			Entry("When status is Pending", intgteststat.IntegrationTestStatusPending, "Pending"),
			Entry("When status is InProgress", intgteststat.IntegrationTestStatusInProgress, "InProgress"),
			Entry("When status is EnvironmentProvisionError", intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, "EnvironmentProvisionError"),
			Entry("When status is DeploymentError", intgteststat.IntegrationTestStatusDeploymentError_Deprecated, "DeploymentError"),
			Entry("When status is TestFail", intgteststat.IntegrationTestStatusTestFail, "TestFail"),
			Entry("When status is TestPass", intgteststat.IntegrationTestStatusTestPassed, "TestPassed"),
			Entry("When status is Deleted", intgteststat.IntegrationTestStatusDeleted, "Deleted"),
			Entry("When status is Invalid", intgteststat.IntegrationTestStatusTestInvalid, "TestInvalid"),
		)

		DescribeTable("Check IsFinal logic",
			func(st intgteststat.IntegrationTestStatus, isFinal bool) {
				result := st.IsFinal()
				Expect(result).To(Equal(isFinal))
			},
			Entry("When status is Deleted", intgteststat.IntegrationTestStatusDeleted, true),
			Entry("When status is DeploymentError", intgteststat.IntegrationTestStatusDeploymentError_Deprecated, true),
			Entry("When status is EnvironmentProvisionError", intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, true),
			Entry("When status is TestFail", intgteststat.IntegrationTestStatusTestFail, true),
			Entry("When status is TestPass", intgteststat.IntegrationTestStatusTestPassed, true),
			Entry("When status is Invalid", intgteststat.IntegrationTestStatusTestInvalid, true),
			Entry("When status is Other", intgteststat.IntegrationTestStatusPending, false),
		)

		It("Invalid status to type fails with error", func() {
			_, err := intgteststat.IntegrationTestStatusString("Unknown")
			Expect(err).NotTo(BeNil())
		})

		It("Invalid JSON status to type fails with error", func() {
			const unknownJson = "\"Unknown\""
			var controlStatus intgteststat.IntegrationTestStatus
			err := controlStatus.UnmarshalJSON([]byte(unknownJson))
			Expect(err).NotTo(BeNil())
		})
	})

	Describe("TestStatusDetail type tests", func() {
		var (
			statusDetailPending intgteststat.IntegrationTestStatusDetail
		)

		BeforeEach(func() {
			statusDetailPending = intgteststat.IntegrationTestStatusDetail{Status: intgteststat.IntegrationTestStatusPending}
		})

		Describe("JSON operations", func() {
			It("Struct can be transformed to JSON", func() {
				jsonData, err := json.Marshal(statusDetailPending)
				Expect(err).To(BeNil())
				Expect(jsonData).Should(ContainSubstring("Pending"))
			})

			It("From JSON back to struct", func() {
				jsonData, err := json.Marshal(statusDetailPending)
				Expect(err).To(BeNil())
				var statusDetailFromJSON intgteststat.IntegrationTestStatusDetail
				err = json.Unmarshal(jsonData, &statusDetailFromJSON)
				Expect(err).To(BeNil())
				Expect(statusDetailFromJSON).Should(Equal(statusDetailPending))
			})
		})
	})

	var _ = Describe("SnapshotTestsStatus type tests", func() {

		const (
			testScenarioName = "test-scenario"
			testDetails      = "test-details"
			pipelineRunName  = "pipeline-run-abcdf"
		)
		var (
			sits *intgteststat.SnapshotIntegrationTestStatuses
		)

		BeforeEach(func() {
			var err error
			sits, err = intgteststat.NewSnapshotIntegrationTestStatuses("")
			Expect(err).To(BeNil())
		})

		It("Can add new scenario test status", func() {
			Expect(sits.GetStatuses()).To(BeEmpty())
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, testDetails)
			Expect(sits.GetStatuses()).To(HaveLen(1))

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(detail.ScenarioName).To(Equal(testScenarioName))
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusPending))
		})

		DescribeTable("Test expected additions of startTime",
			func(st intgteststat.IntegrationTestStatus, shouldAdd bool) {
				sits.UpdateTestStatusIfChanged(testScenarioName, st, testDetails)
				detail, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())
				if shouldAdd {
					Expect(detail.StartTime).NotTo(BeNil())
				} else {
					Expect(detail.StartTime).To(BeNil())
				}
			},
			Entry("When status is Pending", intgteststat.IntegrationTestStatusPending, false),
			Entry("When status is InProgress", intgteststat.IntegrationTestStatusInProgress, true),
			Entry("When status is EnvironmentProvisionError", intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, false),
			Entry("When status is DeploymentError", intgteststat.IntegrationTestStatusDeploymentError_Deprecated, false),
			Entry("When status is TestFail", intgteststat.IntegrationTestStatusTestFail, false),
			Entry("When status is TestPass", intgteststat.IntegrationTestStatusTestPassed, false),
			Entry("When status is Deleted", intgteststat.IntegrationTestStatusDeleted, false),
			Entry("When status is Invalid", intgteststat.IntegrationTestStatusTestInvalid, false),
		)

		DescribeTable("Test expected additions of completionTime",
			func(st intgteststat.IntegrationTestStatus, shouldAdd bool) {
				sits.UpdateTestStatusIfChanged(testScenarioName, st, testDetails)
				detail, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())
				if shouldAdd {
					Expect(detail.CompletionTime).NotTo(BeNil())
				} else {
					Expect(detail.CompletionTime).To(BeNil())
				}
			},
			Entry("When status is Pending", intgteststat.IntegrationTestStatusPending, false),
			Entry("When status is InProgress", intgteststat.IntegrationTestStatusInProgress, false),
			Entry("When status is EnvironmentProvisionError", intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, true),
			Entry("When status is DeploymentError", intgteststat.IntegrationTestStatusDeploymentError_Deprecated, true),
			Entry("When status is TestFail", intgteststat.IntegrationTestStatusTestFail, true),
			Entry("When status is TestPass", intgteststat.IntegrationTestStatusTestPassed, true),
			Entry("When status is Deleted", intgteststat.IntegrationTestStatusDeleted, true),
			Entry("When status is Invalid", intgteststat.IntegrationTestStatusTestInvalid, true),
		)

		It("Change back to InProgress updates timestamps accordingly", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusTestPassed, testDetails)
			originalDetail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(originalDetail.CompletionTime).ToNot(BeNil())
			originalStartTime := originalDetail.StartTime // copy time, it's all in pointers

			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
			newDetail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(originalDetail.CompletionTime).To(BeNil())
			Expect(newDetail.StartTime).NotTo(Equal(originalStartTime))
		})

		It("not changing status keeps starting time the same", func() {
			newDetails := "something important"
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
			originalDetail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			originalStartTime := originalDetail.StartTime // copy time, it's all in pointers
			originalLastUpdateTime := originalDetail.LastUpdateTime

			time.Sleep(time.Duration(100)) // wait 100ns to avoid race condition when test is too quick and timestamp is the same
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, newDetails)
			newDetail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(newDetail.StartTime).To(Equal(originalStartTime))
			// but details and lastUpdateTimestamp must changed
			Expect(newDetail.Details).To(Equal(newDetails))
			Expect(newDetail.LastUpdateTime).NotTo(Equal(originalLastUpdateTime))
		})

		It("can update details with pipeline run name", func() {
			// test detail must exist first
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
			sits.ResetDirty()

			err := sits.UpdateTestPipelineRunName(testScenarioName, pipelineRunName)
			Expect(err).To(BeNil())

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(detail.TestPipelineRunName).To(Equal(pipelineRunName))
			Expect(sits.IsDirty()).To(BeTrue())

			// other data hasn't been removed
			Expect(detail.ScenarioName).To(Equal(testScenarioName))
			Expect(detail.Details).To(Equal(testDetails))
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusInProgress))
		})

		It("doesn't update the same pipeline run name twice", func() {
			// test detail must exist first
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
			sits.ResetDirty()

			err := sits.UpdateTestPipelineRunName(testScenarioName, pipelineRunName)
			Expect(err).To(BeNil())
			Expect(sits.IsDirty()).To(BeTrue())
			sits.ResetDirty()

			err = sits.UpdateTestPipelineRunName(testScenarioName, pipelineRunName)
			Expect(err).To(BeNil())

			Expect(sits.IsDirty()).To(BeFalse())
		})

		It("fails to update details with pipeline run name when testScenario doesn't exist", func() {
			err := sits.UpdateTestPipelineRunName(testScenarioName, pipelineRunName)
			Expect(err).NotTo(BeNil())
		})

		It("Can export valid JSON without start and completion time (Pending)", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, testDetails)
			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())

			expectedFormatStr := `[
					{
						"scenario": "%s",
						"status": "Pending",
						"lastUpdateTime": "%s",
						"details": "%s"
					}
				]`
			marshaledTime, err := detail.LastUpdateTime.MarshalText()
			Expect(err).To(BeNil())
			expectedStr := fmt.Sprintf(expectedFormatStr, testScenarioName, marshaledTime, testDetails)
			expected := []byte(expectedStr)

			Expect(json.Marshal(sits)).To(MatchJSON(expected))
		})

		It("Can export valid JSON with start time (InProgress)", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())

			expectedFormatStr := `[
					{
						"scenario": "%s",
						"status": "InProgress",
						"lastUpdateTime": "%s",
						"details": "%s",
						"startTime": "%s"
					}
				]`
			marshaledTime, err := detail.LastUpdateTime.MarshalText()
			Expect(err).To(BeNil())
			marshaledStartTime, err := detail.StartTime.MarshalText()
			Expect(err).To(BeNil())
			expectedStr := fmt.Sprintf(expectedFormatStr, testScenarioName, marshaledTime, testDetails, marshaledStartTime)
			expected := []byte(expectedStr)

			Expect(json.Marshal(sits)).To(MatchJSON(expected))
		})

		It("Can export valid JSON with completion time (TestFailed)", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusTestFail, testDetails)

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())

			expectedFormatStr := `[
					{
						"scenario": "%s",
						"status": "TestFail",
						"lastUpdateTime": "%s",
						"details": "%s",
						"completionTime": "%s"
					}
				]`
			marshaledTime, err := detail.LastUpdateTime.MarshalText()
			Expect(err).To(BeNil())
			marshaledCompletionTime, err := detail.CompletionTime.MarshalText()
			Expect(err).To(BeNil())
			expectedStr := fmt.Sprintf(expectedFormatStr, testScenarioName, marshaledTime, testDetails, marshaledCompletionTime)
			expected := []byte(expectedStr)

			Expect(json.Marshal(sits)).To(MatchJSON(expected))
		})

		It("Can export valid JSON with startTime and completion time", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, "yolo")
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusTestFail, testDetails)

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())

			expectedFormatStr := `[
					{
						"scenario": "%s",
						"status": "TestFail",
						"lastUpdateTime": "%s",
						"details": "%s",
						"startTime": "%s",
						"completionTime": "%s"
					}
				]`
			marshaledTime, err := detail.LastUpdateTime.MarshalText()
			Expect(err).To(BeNil())
			marshaledStartTime, err := detail.StartTime.MarshalText()
			Expect(err).To(BeNil())
			marshaledCompletionTime, err := detail.CompletionTime.MarshalText()
			Expect(err).To(BeNil())
			expectedStr := fmt.Sprintf(expectedFormatStr, testScenarioName, marshaledTime, testDetails, marshaledStartTime, marshaledCompletionTime)
			expected := []byte(expectedStr)

			Expect(json.Marshal(sits)).To(MatchJSON(expected))
		})

		It("Can export valid JSON with TestPipelineRunName", func() {
			sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, testDetails)
			err := sits.UpdateTestPipelineRunName(testScenarioName, pipelineRunName)
			Expect(err).To(BeNil())

			detail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())

			expectedFormatStr := `[
					{
						"scenario": "%s",
						"status": "Pending",
						"lastUpdateTime": "%s",
						"details": "%s",
						"testPipelineRunName": "%s"
					}
				]`
			marshaledTime, err := detail.LastUpdateTime.MarshalText()
			Expect(err).To(BeNil())
			expectedStr := fmt.Sprintf(expectedFormatStr, testScenarioName, marshaledTime, testDetails, pipelineRunName)
			expected := []byte(expectedStr)

			Expect(json.Marshal(sits)).To(MatchJSON(expected))
		})

		When("Contains updates to status", func() {

			BeforeEach(func() {
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, testDetails)
			})

			It("Status is marked as dirty", func() {
				Expect(sits.IsDirty()).To(BeTrue())
			})

			It("Status can be reset as non-dirty", func() {
				sits.ResetDirty()
				Expect(sits.IsDirty()).To(BeFalse())
			})

			It("Adding the same update keeps status non-dirty", func() {
				sits.ResetDirty()
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, testDetails)
				Expect(sits.IsDirty()).To(BeFalse())
			})

			It("Updating status of scenario is reflected", func() {
				oldSt, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())

				oldTimestamp := oldSt.LastUpdateTime

				sits.ResetDirty()
				// needs different status
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
				Expect(sits.IsDirty()).To(BeTrue())

				newSt, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())
				Expect(newSt.Status).To(Equal(intgteststat.IntegrationTestStatusInProgress))
				// timestamp must be updated too
				Expect(newSt.LastUpdateTime).NotTo(Equal(oldTimestamp))

				// no changes to number of records
				Expect(sits.GetStatuses()).To(HaveLen(1))
			})

			It("Updating details of scenario is reflected", func() {
				newDetails := "_Testing details_"
				oldSt, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())

				oldTimestamp := oldSt.LastUpdateTime

				sits.ResetDirty()
				// needs the same status but different details
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusPending, newDetails)
				Expect(sits.IsDirty()).To(BeTrue())

				newSt, ok := sits.GetScenarioStatus(testScenarioName)
				Expect(ok).To(BeTrue())
				Expect(newSt.Details).To(Equal(newDetails))
				// timestamp must be updated too
				Expect(newSt.LastUpdateTime).NotTo(Equal(oldTimestamp))

				// no changes to number of records
				Expect(sits.GetStatuses()).To(HaveLen(1))
			})

			It("Scenario can be deleted", func() {
				sits.ResetDirty()
				sits.DeleteStatus(testScenarioName)
				Expect(sits.GetStatuses()).To(BeEmpty())
				Expect(sits.IsDirty()).To(BeTrue())
			})

			It("Initialization with empty scenario list will remove data", func() {
				sits.ResetDirty()
				sits.InitStatuses(&[]string{})
				Expect(sits.GetStatuses()).To(BeEmpty())
				Expect(sits.IsDirty()).To(BeTrue())
			})

		})

		It("Initialization with new test scenario creates pending status", func() {
			sits.ResetDirty()
			sits.InitStatuses(&[]string{testScenarioName})

			Expect(sits.IsDirty()).To(BeTrue())
			Expect(sits.GetStatuses()).To(HaveLen(1))

			statusDetail, ok := sits.GetScenarioStatus(testScenarioName)
			Expect(ok).To(BeTrue())
			Expect(statusDetail.ScenarioName).To(Equal(testScenarioName))
			Expect(statusDetail.Status).To(Equal(intgteststat.IntegrationTestStatusPending))
		})

		When("JSON contains empty test status annotation", func() {
			It("Returns empty test statuses", func() {
				statuses, err := intgteststat.NewSnapshotIntegrationTestStatuses("[]")
				Expect(err).To(BeNil())
				Expect(statuses.GetStatuses()).To(BeEmpty())
			})
		})

		When("JSON contains valid test status annotation", func() {
			var jsondata string

			BeforeEach(func() {
				sits.UpdateTestStatusIfChanged(testScenarioName, intgteststat.IntegrationTestStatusInProgress, testDetails)
				testAnnotation, err := json.Marshal(sits)
				Expect(err).To(BeNil())
				jsondata = string(testAnnotation)

			})

			It("Returns expected test statuses", func() {
				statuses, err := intgteststat.NewSnapshotIntegrationTestStatuses(jsondata)
				Expect(err).To(BeNil())
				Expect(statuses.GetStatuses()).To(HaveLen(1))

				statusDetail := statuses.GetStatuses()[0]
				Expect(statusDetail.Status).To(Equal(intgteststat.IntegrationTestStatusInProgress))
				Expect(statusDetail.ScenarioName).To(Equal(testScenarioName))
				Expect(statusDetail.Details).To(Equal(testDetails))
			})

		})

		When("JSON contains invalid attributes", func() {
			It("Returns error", func() {
				_, err := intgteststat.NewSnapshotIntegrationTestStatuses("[{\"invalid\":\"data\"}]")
				Expect(err).NotTo(BeNil())
			})
		})

		When("JSON contains invalid format", func() {

			It("Returns error", func() {
				_, err := intgteststat.NewSnapshotIntegrationTestStatuses("{}")
				Expect(err).NotTo(BeNil())
			})
		})

	})

})
