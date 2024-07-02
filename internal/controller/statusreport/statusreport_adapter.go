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
	"context"
	"fmt"
	"time"

	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/konflux-ci/integration-service/api/v1beta2"
	"github.com/konflux-ci/integration-service/gitops"
	"github.com/konflux-ci/integration-service/helpers"
	"github.com/konflux-ci/integration-service/loader"
	intgteststat "github.com/konflux-ci/integration-service/pkg/integrationteststatus"
	"github.com/konflux-ci/integration-service/status"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

const SnapshotRetryTimeout = time.Duration(3 * time.Hour)

// Adapter holds the objects needed to reconcile a snapshot's test status report.
type Adapter struct {
	snapshot    *applicationapiv1alpha1.Snapshot
	application *applicationapiv1alpha1.Application
	logger      helpers.IntegrationLogger
	loader      loader.ObjectLoader
	client      client.Client
	context     context.Context
	status      status.StatusInterface
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(context context.Context, snapshot *applicationapiv1alpha1.Snapshot, application *applicationapiv1alpha1.Application,
	logger helpers.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
) *Adapter {
	return &Adapter{
		snapshot:    snapshot,
		application: application,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
		status:      status.NewStatus(logger.Logger, client),
	}
}

// EnsureSnapshotTestStatusReportedToGitProvider will ensure that integration test status is reported to the git provider
// which (indirectly) triggered its execution.
func (a *Adapter) EnsureSnapshotTestStatusReportedToGitProvider() (controller.OperationResult, error) {
	if gitops.IsSnapshotCreatedByPACPushEvent(a.snapshot) {
		return controller.ContinueProcessing()
	}

	reporter := a.status.GetReporter(a.snapshot)
	if reporter == nil {
		a.logger.Info("No suitable reporter found, skipping report")
		return controller.ContinueProcessing()
	}
	a.logger.Info(fmt.Sprintf("Detected reporter: %s", reporter.GetReporterName()))

	err := a.status.ReportSnapshotStatus(a.context, reporter, a.snapshot)
	if err != nil {
		a.logger.Error(err, "failed to report test status to git provider for snapshot",
			"snapshot.Namespace", a.snapshot.Namespace, "snapshot.Name", a.snapshot.Name)
		if helpers.IsObjectYoungerThanThreshold(a.snapshot, SnapshotRetryTimeout) {
			return controller.RequeueWithError(err)
		}
	}
	testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
	if err != nil {
		return controller.RequeueWithError(err)
	}
	for _, testDetails := range testStatuses.GetStatuses() {
		if testDetails.Status.IsFinal() && testDetails.TestPipelineRunName != "" {
			pipelineRunName := testDetails.TestPipelineRunName
			pipelineRun := &tektonv1.PipelineRun{}
			err := a.client.Get(a.context, types.NamespacedName{
				Namespace: a.snapshot.Namespace,
				Name:      pipelineRunName,
			}, pipelineRun)

			// if the PLR doesn't exist on cluster we continue the loop
			if err != nil {
				if !errors.IsNotFound(err) {
					return controller.RequeueWithError(err)
				}
				continue
			}

			err = helpers.RemoveFinalizerFromPipelineRun(a.context, a.client, a.logger, pipelineRun, helpers.IntegrationPipelineRunFinalizer)
			if err != nil {
				return controller.RequeueWithError(err)
			}
		}
	}
	return controller.ContinueProcessing()
}

// EnsureSnapshotFinishedAllTests is an operation that will ensure that a pipeline Snapshot
// to the PipelineRun being processed finished and passed all tests for all defined required IntegrationTestScenarios.
func (a *Adapter) EnsureSnapshotFinishedAllTests() (controller.OperationResult, error) {
	// Get all required integrationTestScenarios for the Application and then use the Snapshot status annotation
	// to check if all Integration tests were finished for that Snapshot
	integrationTestScenarios, err := a.loader.GetRequiredIntegrationTestScenariosForApplication(a.context, a.client, a.application)
	if err != nil {
		return controller.RequeueWithError(err)
	}
	a.logger.Info(fmt.Sprintf("Found %d required integration test scenarios", len(*integrationTestScenarios)))

	testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
	if err != nil {
		return controller.RequeueWithError(err)
	}

	allIntegrationTestsFinished, allIntegrationTestsPassed := a.determineIfAllRequiredIntegrationTestsFinishedAndPassed(integrationTestScenarios, testStatuses)
	if err != nil {
		a.logger.Error(err, "Failed to determine outcomes for Integration Tests",
			"snapshot.Name", a.snapshot.Name)
		return controller.RequeueWithError(err)
	}

	// Skip doing anything if not all Integration tests were finished for all integrationTestScenarios
	if !allIntegrationTestsFinished {
		a.logger.Info("Not all required Integration PipelineRuns finished",
			"snapshot.Name", a.snapshot.Name)

		// If for the snapshot there is an IntegrationTestScenario that is not triggered, it will add run labebl to snapshot
		integrationTestScenarioNotTriggered := a.findUntriggeredIntegrationTestFromStatus(integrationTestScenarios, testStatuses)
		if integrationTestScenarioNotTriggered != "" {
			a.logger.Info("Detected an integrationTestScenario was not triggered, applying snapshot reconcilation",
				"integrationTestScenario.Name", integrationTestScenarioNotTriggered)
			if err = gitops.AddIntegrationTestRerunLabel(a.context, a.client, a.snapshot, integrationTestScenarioNotTriggered); err != nil {
				return controller.RequeueWithError(err)
			}

		}

		return controller.ContinueProcessing()
	}

	finishedStatusMessage := "Snapshot integration status condition is finished since all testing pipelines completed"
	if len(*integrationTestScenarios) == 0 {
		finishedStatusMessage = "Snapshot integration status condition is finished since there are no required testing pipelines defined for its application"
	}

	if !gitops.IsSnapshotIntegrationStatusMarkedAsFinished(a.snapshot) {
		err = gitops.MarkSnapshotIntegrationStatusAsFinished(a.context, a.client, a.snapshot, finishedStatusMessage)
		if err != nil {
			a.logger.Error(err, "Failed to Update Snapshot AppStudioIntegrationStatus status")
			return controller.RequeueWithError(err)
		}
		a.logger.LogAuditEvent(finishedStatusMessage, a.snapshot, helpers.LogActionUpdate)
	}

	// If all Integration Pipeline runs passed, mark the snapshot as succeeded, otherwise mark it as failed
	// This updates the Snapshot resource on the cluster
	if allIntegrationTestsPassed {
		if !gitops.IsSnapshotMarkedAsPassed(a.snapshot) {
			err = gitops.MarkSnapshotAsPassed(a.context, a.client, a.snapshot, "All Integration Pipeline tests passed")
			if err != nil {
				a.logger.Error(err, "Failed to Update Snapshot AppStudioTestSucceeded status")
				return controller.RequeueWithError(err)
			}
			a.logger.LogAuditEvent(fmt.Sprintf("Snapshot integration status condition marked as passed, all of %d required Integration PipelineRuns succeeded", len(*integrationTestScenarios)),
				a.snapshot, helpers.LogActionUpdate)
		}
	} else {
		if !gitops.IsSnapshotMarkedAsFailed(a.snapshot) {
			err = gitops.MarkSnapshotAsFailed(a.context, a.client, a.snapshot, "Some Integration pipeline tests failed")
			if err != nil {
				a.logger.Error(err, "Failed to Update Snapshot AppStudioTestSucceeded status")
				return controller.RequeueWithError(err)
			}
			a.logger.LogAuditEvent("Snapshot integration status condition marked as failed, some tests within Integration PipelineRuns failed",
				a.snapshot, helpers.LogActionUpdate)
		}
	}

	return controller.ContinueProcessing()
}

// determineIfAllRequiredIntegrationTestsFinishedAndPassed checks if all Integration tests finished and passed for the given
// list of integrationTestScenarios.
func (a *Adapter) determineIfAllRequiredIntegrationTestsFinishedAndPassed(integrationTestScenarios *[]v1beta2.IntegrationTestScenario, testStatuses *intgteststat.SnapshotIntegrationTestStatuses) (bool, bool) {
	allIntegrationTestsFinished, allIntegrationTestsPassed := true, true
	integrationTestsFinished := 0
	integrationTestsPassed := 0

	for _, integrationTestScenario := range *integrationTestScenarios {
		integrationTestScenario := integrationTestScenario // G601
		testDetails, ok := testStatuses.GetScenarioStatus(integrationTestScenario.Name)
		if !ok || !testDetails.Status.IsFinal() {
			allIntegrationTestsFinished = false
		} else {
			integrationTestsFinished++
		}
		if ok && testDetails.Status != intgteststat.IntegrationTestStatusTestPassed {
			allIntegrationTestsPassed = false
		} else {
			integrationTestsPassed++
		}

	}
	a.logger.Info(fmt.Sprintf("%[1]d out of %[3]d required integration tests finished, %[2]d out of %[3]d required integration tests passed", integrationTestsFinished, integrationTestsPassed, len(*integrationTestScenarios)))
	return allIntegrationTestsFinished, allIntegrationTestsPassed
}

// findUntriggeredIntegrationTestFromStatus returns name of integrationTestScenario that is not triggered yet.
func (a *Adapter) findUntriggeredIntegrationTestFromStatus(integrationTestScenarios *[]v1beta2.IntegrationTestScenario, testStatuses *intgteststat.SnapshotIntegrationTestStatuses) string {
	for _, integrationTestScenario := range *integrationTestScenarios {
		integrationTestScenario := integrationTestScenario // G601
		_, ok := testStatuses.GetScenarioStatus(integrationTestScenario.Name)
		if !ok {
			return integrationTestScenario.Name
		}

	}
	return ""
}
