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

package integrationpipeline

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
	"github.com/redhat-appstudio/integration-service/tekton"

	"github.com/konflux-ci/operator-toolkit/controller"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Adapter holds the objects needed to reconcile an integration PipelineRun.
type Adapter struct {
	pipelineRun *tektonv1.PipelineRun
	application *applicationapiv1alpha1.Application
	snapshot    *applicationapiv1alpha1.Snapshot
	loader      loader.ObjectLoader
	logger      h.IntegrationLogger
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(pipelineRun *tektonv1.PipelineRun, application *applicationapiv1alpha1.Application, snapshot *applicationapiv1alpha1.Snapshot, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		pipelineRun: pipelineRun,
		application: application,
		snapshot:    snapshot,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

// EnsureStatusReportedInSnapshot will ensure that status of the integration test pipelines is reported to snapshot
// to be consumed by user
func (a *Adapter) EnsureStatusReportedInSnapshot() (controller.OperationResult, error) {
	var pipelinerunStatus intgteststat.IntegrationTestStatus
	var detail string
	var err error

	// pipelines run in parallel and have great potential to cause conflict on update
	// thus `RetryOnConflict` is easy solution here, given the snapshot must be loaded specifically here
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {

		a.snapshot, err = a.loader.GetSnapshotFromPipelineRun(a.client, a.context, a.pipelineRun)
		if err != nil {
			return err
		}

		statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
		if err != nil {
			return err
		}

		pipelinerunStatus, detail, err = a.GetIntegrationPipelineRunStatus(a.client, a.context, a.pipelineRun)
		if err != nil {
			return err
		}
		statuses.UpdateTestStatusIfChanged(a.pipelineRun.Labels[tekton.ScenarioNameLabel], pipelinerunStatus, detail)
		if err = statuses.UpdateTestPipelineRunName(a.pipelineRun.Labels[tekton.ScenarioNameLabel], a.pipelineRun.Name); err != nil {
			return err
		}

		// don't return wrapped err for retries
		err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, statuses, a.client, a.context)
		return err
	})
	if err != nil {
		a.logger.Error(err, "Failed to update pipeline status in snapshot")
		return controller.RequeueWithError(fmt.Errorf("failed to update test status in snapshot: %w", err))
	}

	// Remove the finalizer from Integration PLRs only if they are related to Snapshots created by Push event
	// If they are related, then the statusreport controller removes the finalizers from these PLRs
	if gitops.IsSnapshotCreatedByPACPushEvent(a.snapshot) && (h.HasPipelineRunFinished(a.pipelineRun) || pipelinerunStatus == intgteststat.IntegrationTestStatusDeleted) {
		err = h.RemoveFinalizerFromPipelineRun(a.client, a.logger, a.context, a.pipelineRun, h.IntegrationPipelineRunFinalizer)
		if err != nil {
			return controller.RequeueWithError(fmt.Errorf("failed to remove the finalizer: %w", err))
		}
	}

	return controller.ContinueProcessing()
}

// EnsureEphemeralEnvironmentsCleanedUp will ensure that ephemeral environment(s) associated with the
// integration PipelineRun are cleaned up.
func (a *Adapter) EnsureEphemeralEnvironmentsCleanedUp() (controller.OperationResult, error) {
	if !h.HasPipelineRunFinished(a.pipelineRun) {
		return controller.ContinueProcessing()
	}

	testEnvironment, err := a.loader.GetEnvironmentFromIntegrationPipelineRun(a.client, a.context, a.pipelineRun)
	if err != nil && !errors.IsNotFound(err) {
		a.logger.Error(err, "Failed to find the environment for the pipelineRun")
		return controller.RequeueWithError(err)
	}
	if testEnvironment == nil {
		a.logger.Info("The pipelineRun does not have any test Environments associated with it, skipping cleanup.")
		return controller.ContinueProcessing()
	}

	isEphemeral := h.IsEnvironmentEphemeral(testEnvironment)

	if isEphemeral {
		dtc, err := a.loader.GetDeploymentTargetClaimForEnvironment(a.client, a.context, testEnvironment)
		if err != nil || dtc == nil {
			a.logger.Error(err, "Failed to find deploymentTargetClaim defined in environment", "environment.Name", testEnvironment.Name)
			return controller.RequeueWithError(err)
		}

		binding, err := a.loader.FindExistingSnapshotEnvironmentBinding(a.client, a.context, a.application, testEnvironment)
		if err != nil || binding == nil {
			a.logger.Error(err, "Failed to find snapshotEnvironmentBinding associated with environment", "environment.Name", testEnvironment.Name)
			return controller.RequeueWithError(err)
		}

		err = h.CleanUpEphemeralEnvironments(a.client, &a.logger, a.context, testEnvironment, dtc)
		if err != nil {
			a.logger.Error(err, "Failed to delete the Ephemeral Environment")
			return controller.RequeueWithError(err)
		}
	}

	return controller.ContinueProcessing()
}

// GetIntegrationPipelineRunStatus checks the Tekton results for a given PipelineRun and returns status of test.
func (a *Adapter) GetIntegrationPipelineRunStatus(adapterClient client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (intgteststat.IntegrationTestStatus, string, error) {
	// Check if the pipelineRun finished from the condition of status
	if !h.HasPipelineRunFinished(pipelineRun) {
		// Mark the pipelineRun's status as "Deleted" if its not finished yet and is marked for deletion (with a non-nil deletionTimestamp)
		if pipelineRun.GetDeletionTimestamp() != nil {
			return intgteststat.IntegrationTestStatusDeleted, fmt.Sprintf("Integration test which is running as pipeline run '%s', has been deleted", pipelineRun.Name), nil
		} else {
			return intgteststat.IntegrationTestStatusInProgress, fmt.Sprintf("Integration test is running as pipeline run '%s'", pipelineRun.Name), nil
		}
	}

	taskRuns, err := a.loader.GetAllTaskRunsWithMatchingPipelineRunLabel(adapterClient, ctx, pipelineRun)
	if err != nil {
		return intgteststat.IntegrationTestStatusTestInvalid, fmt.Sprintf("Unable to get all the TaskRun(s) related to the pipelineRun '%s'", pipelineRun.Name), err
	}

	taskRunsInClusterCount := len(*taskRuns)
	taskRunsInChildRefCount := len(pipelineRun.Status.ChildReferences)

	if taskRunsInClusterCount != taskRunsInChildRefCount {
		return intgteststat.IntegrationTestStatusTestInvalid, fmt.Sprintf("Failed to determine status of pipelinerun '%s'"+
			", due to mismatch in TaskRuns present in cluster (%v) and those referenced within childReferences (%v)",
			pipelineRun.Name, taskRunsInClusterCount, taskRunsInChildRefCount), nil
	}

	outcome, err := h.GetIntegrationPipelineRunOutcome(adapterClient, ctx, pipelineRun)
	if err != nil {
		return intgteststat.IntegrationTestStatusTestFail, "", fmt.Errorf("failed to evaluate integration test results: %w", err)
	}

	if !outcome.HasPipelineRunPassedTesting() {
		if !outcome.HasPipelineRunValidTestOutputs() {
			return intgteststat.IntegrationTestStatusTestFail, strings.Join(outcome.GetValidationErrorsList(), "; "), nil
		}
		return intgteststat.IntegrationTestStatusTestFail, "Integration test failed", nil
	}

	return intgteststat.IntegrationTestStatusTestPassed, "Integration test passed", nil
}
