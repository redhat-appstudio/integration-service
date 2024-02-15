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
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	clienterrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/metrics"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
	"github.com/redhat-appstudio/integration-service/release"
	"github.com/redhat-appstudio/integration-service/tekton"

	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/operator-toolkit/controller"
	"github.com/redhat-appstudio/operator-toolkit/metadata"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const RetryReleaseTimeout = time.Duration(3 * time.Hour)

// configuration options for scenario
type ScenarioOptions struct {
	IsReRun bool
}

// Adapter holds the objects needed to reconcile a Release.
type Adapter struct {
	snapshot    *applicationapiv1alpha1.Snapshot
	application *applicationapiv1alpha1.Application
	component   *applicationapiv1alpha1.Component
	logger      h.IntegrationLogger
	loader      loader.ObjectLoader
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(snapshot *applicationapiv1alpha1.Snapshot, application *applicationapiv1alpha1.Application, component *applicationapiv1alpha1.Component, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		snapshot:    snapshot,
		application: application,
		component:   component,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

func scenariosNamesToList(integrationTestScenarios *[]v1beta1.IntegrationTestScenario) *[]string {
	// transform list of structs into list of strings
	result := make([]string, 0, len(*integrationTestScenarios))
	for _, v := range *integrationTestScenarios {
		result = append(result, v.Name)
	}
	return &result
}

// EnsureRerunPipelineRunsExist is responsible for recreating integration test pipelines triggered by users
func (a *Adapter) EnsureRerunPipelineRunsExist() (controller.OperationResult, error) {

	scenarioName, ok := gitops.GetIntegrationTestRunLabelValue(a.snapshot)
	if !ok {
		// no test rerun triggered
		return controller.ContinueProcessing()
	}
	integrationTestScenario, err := a.loader.GetScenario(a.client, a.context, scenarioName, a.application.Namespace)

	if err != nil {
		if clienterrors.IsNotFound(err) {
			a.logger.Error(err, "scenario for integration test re-run not found", "scenario", scenarioName)
			// scenario doesn't exist just remove label and continue
			if err = gitops.RemoveIntegrationTestRerunLabel(a.client, a.context, a.snapshot); err != nil {
				return controller.RequeueWithError(err)
			}
			return controller.ContinueProcessing()
		}
		return controller.RequeueWithError(fmt.Errorf("failed to fetch requested scenario %s: %w", scenarioName, err))
	}

	a.logger.Info("Re-running integration test for scenario", "scenario", scenarioName)

	testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
	if err != nil {
		return controller.RequeueWithError(err)
	}

	integrationTestScenarioStatus, ok := testStatuses.GetScenarioStatus(integrationTestScenario.Name)
	if ok && (integrationTestScenarioStatus.Status == intgteststat.IntegrationTestStatusInProgress ||
		integrationTestScenarioStatus.Status == intgteststat.IntegrationTestStatusPending) {
		a.logger.Info(fmt.Sprintf("Found existing test in %s status, skipping re-run", integrationTestScenarioStatus.Status),
			"integrationTestScenario.Name", integrationTestScenario.Name)
		if err = gitops.RemoveIntegrationTestRerunLabel(a.client, a.context, a.snapshot); err != nil {
			return controller.RequeueWithError(err)
		}
		return controller.ContinueProcessing()
	}
	testStatuses.ResetStatus(scenarioName)

	pipelineRun, err := a.createIntegrationPipelineRun(a.application, integrationTestScenario, a.snapshot)
	if err != nil {
		return a.HandlePipelineCreationError(err, integrationTestScenario, testStatuses)
	}
	testStatuses.UpdateTestStatusIfChanged(
		integrationTestScenario.Name, intgteststat.IntegrationTestStatusInProgress,
		fmt.Sprintf("IntegrationTestScenario pipeline '%s' has been created", pipelineRun.Name))
	if err = testStatuses.UpdateTestPipelineRunName(integrationTestScenario.Name, pipelineRun.Name); err != nil {
		// it doesn't make sense to restart reconciliation here, it will be eventually updated by integrationpipeline adapter
		a.logger.Error(err, "Failed to update pipelinerun name in test status")
	}

	if err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context); err != nil {
		return controller.RequeueWithError(err)
	}

	if err = gitops.ResetSnapshotStatusConditions(a.client, a.context, a.snapshot, "Integration test is being rerun for snapshot"); err != nil {
		a.logger.Error(err, "Failed to reset snapshot status conditions")
		return controller.RequeueWithError(err)
	}

	if err = gitops.RemoveIntegrationTestRerunLabel(a.client, a.context, a.snapshot); err != nil {
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// EnsureIntegrationPipelineRunsExist is an operation that will ensure that all Integration pipeline runs
// associated with the Snapshot and the Application's IntegrationTestScenarios exist.
func (a *Adapter) EnsureIntegrationPipelineRunsExist() (controller.OperationResult, error) {
	if gitops.HaveAppStudioTestsFinished(a.snapshot) {
		a.logger.Info("The Snapshot has finished testing.")
		return controller.ContinueProcessing()
	}

	integrationTestScenarios, err := a.loader.GetAllIntegrationTestScenariosForApplication(a.client, a.context, a.application)
	if err != nil {
		a.logger.Error(err, "Failed to get Integration test scenarios for the following application",
			"Application.Namespace", a.application.Namespace)
	}

	if integrationTestScenarios != nil {
		a.logger.Info(
			fmt.Sprintf("Found %d IntegrationTestScenarios for application", len(*integrationTestScenarios)),
			"Application.Name", a.application.Name,
			"IntegrationTestScenarios", len(*integrationTestScenarios))

		// Initialize status of all integration tests
		// This is starting place where integration tests are being initialized
		testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
		if err != nil {
			return controller.RequeueWithError(err)
		}
		testStatuses.InitStatuses(scenariosNamesToList(integrationTestScenarios))
		err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context)
		if err != nil {
			return controller.RequeueWithError(err)
		}

		defer func() {
			// Try to update status of test if something failed, because test loop can stop prematurely on error,
			// we should record current status.
			// This is only best effort update
			//
			// When update of statuses worked fine at the end of function, this is just a no-op
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context)
			if err != nil {
				a.logger.Error(err, "Defer: Updating statuses of tests in snapshot failed")
			}
		}()

		for _, integrationTestScenario := range *integrationTestScenarios {
			integrationTestScenario := integrationTestScenario //G601
			// Check if an existing integration pipelineRun is registered in the Snapshot's status
			// We rely on this because the actual pipelineRun CR may have been pruned by this point
			integrationTestScenarioStatus, ok := testStatuses.GetScenarioStatus(integrationTestScenario.Name)
			if ok && integrationTestScenarioStatus.TestPipelineRunName != "" {
				a.logger.Info("Found existing integrationPipelineRun",
					"integrationTestScenario.Name", integrationTestScenario.Name,
					"pipelineRun.Name", integrationTestScenarioStatus.TestPipelineRunName)
			} else {
				pipelineRun, err := a.createIntegrationPipelineRun(a.application, &integrationTestScenario, a.snapshot)
				if err != nil {
					return a.HandlePipelineCreationError(err, &integrationTestScenario, testStatuses)
				}
				gitops.PrepareToRegisterIntegrationPipelineRunStarted(a.snapshot) // don't count re-runs
				testStatuses.UpdateTestStatusIfChanged(
					integrationTestScenario.Name, intgteststat.IntegrationTestStatusInProgress,
					fmt.Sprintf("IntegrationTestScenario pipeline '%s' has been created", pipelineRun.Name))
				if err = testStatuses.UpdateTestPipelineRunName(integrationTestScenario.Name, pipelineRun.Name); err != nil {
					// it doesn't make sense to restart reconciliation here, it will be eventually updated by integrationpipeline adapter
					a.logger.Error(err, "Failed to update pipelinerun name in test status")
				}
			}
		}

		err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context)
		if err != nil {
			return controller.RequeueWithError(err)
		}
	}

	requiredIntegrationTestScenarios, err := a.loader.GetRequiredIntegrationTestScenariosForApplication(a.client, a.context, a.application)
	if err != nil {
		a.logger.Error(err, "Failed to get all required IntegrationTestScenarios")
		patch := client.MergeFrom(a.snapshot.DeepCopy())
		gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to get all required IntegrationTestScenarios: "+err.Error())
		a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to get all required IntegrationTestScenarios",
			a.snapshot, h.LogActionUpdate)
		return controller.RequeueOnErrorOrStop(a.client.Status().Patch(a.context, a.snapshot, patch))
	}
	if len(*requiredIntegrationTestScenarios) == 0 && !gitops.IsSnapshotMarkedAsPassed(a.snapshot) {
		err := gitops.MarkSnapshotAsPassed(a.client, a.context, a.snapshot, "No required IntegrationTestScenarios found, skipped testing")
		if err != nil {
			a.logger.Error(err, "Failed to update Snapshot status")
			return controller.RequeueWithError(err)
		}
		a.logger.LogAuditEvent("Snapshot marked as successful. No required IntegrationTestScenarios found, skipped testing",
			a.snapshot, h.LogActionUpdate,
			"snapshot.Status", a.snapshot.Status)
	}

	return controller.ContinueProcessing()
}

// EnsureGlobalCandidateImageUpdated is an operation that ensure the ContainerImage in the Global Candidate List
// being updated when the Snapshot passed all the integration tests
func (a *Adapter) EnsureGlobalCandidateImageUpdated() (controller.OperationResult, error) {
	if a.component == nil || gitops.IsSnapshotCreatedByPACPullRequestEvent(a.snapshot) {
		a.logger.Info("The Snapshot wasn't created for a single component push event, not updating the global candidate list.")
		return controller.ContinueProcessing()
	}
	if !gitops.HaveAppStudioTestsSucceeded(a.snapshot) {
		a.logger.Info("The Snapshot hasn't yet passed all required integration tests, not updating the global candidate list.")
		return controller.ContinueProcessing()
	}
	if gitops.IsSnapshotMarkedAsAddedToGlobalCandidateList(a.snapshot) {
		a.logger.Info("The Snapshot's component was previously added to the global candidate list, skipping adding it.")
		return controller.ContinueProcessing()
	}

	for _, component := range a.snapshot.Spec.Components {
		if component.Name == a.component.Name {
			patch := client.MergeFrom(a.component.DeepCopy())
			a.component.Spec.ContainerImage = component.ContainerImage
			err := a.client.Patch(a.context, a.component, patch)
			if err != nil {
				a.logger.Error(err, "Failed to update .Spec.ContainerImage of Global Candidate for the Component",
					"component.Name", a.component.Name)
				return controller.RequeueWithError(err)
			}
			a.logger.LogAuditEvent("Updated .Spec.ContainerImage of Global Candidate for the Component",
				a.component, h.LogActionUpdate,
				"containerImage", component.ContainerImage)
			if reflect.ValueOf(component.Source).IsValid() && component.Source.GitSource != nil && component.Source.GitSource.Revision != "" {
				patch := client.MergeFrom(a.component.DeepCopy())
				a.component.Status.LastBuiltCommit = component.Source.GitSource.Revision
				err = a.client.Status().Patch(a.context, a.component, patch)
				if err != nil {
					a.logger.Error(err, "Failed to update .Status.LastBuiltCommit of Global Candidate for the Component",
						"component.Name", a.component.Name)
					return controller.RequeueWithError(err)
				}
				a.logger.LogAuditEvent("Updated .Status.LastBuiltCommit of Global Candidate for the Component",
					a.component, h.LogActionUpdate,
					"lastBuildCommit", a.component.Status.LastBuiltCommit)
			}
			break
		}
	}

	// Mark the Snapshot as already added to global candidate list to prevent it from getting added again when the Snapshot
	// gets reconciled at a later time
	err := gitops.MarkSnapshotAsAddedToGlobalCandidateList(a.client, a.context, a.snapshot, "The Snapshot's component was added to the global candidate list")
	if err != nil {
		a.logger.Error(err, "Failed to update the Snapshot's status to AddedToGlobalCandidateList")
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// EnsureAllReleasesExist is an operation that will ensure that all pipeline Releases associated
// to the Snapshot and the Application's ReleasePlans exist.
// Otherwise, it will create new Releases for each ReleasePlan.
func (a *Adapter) EnsureAllReleasesExist() (controller.OperationResult, error) {
	canSnapshotBePromoted, reasons := gitops.CanSnapshotBePromoted(a.snapshot)
	if !canSnapshotBePromoted {
		a.logger.Info("The Snapshot won't be released.",
			"reasons", strings.Join(reasons, ","))
		return controller.ContinueProcessing()
	}
	if gitops.IsSnapshotMarkedAsAutoReleased(a.snapshot) {
		a.logger.Info("The Snapshot was previously auto-released, skipping auto-release.")
		return controller.ContinueProcessing()
	}

	releasePlans, err := a.loader.GetAutoReleasePlansForApplication(a.client, a.context, a.application)
	if err != nil {
		a.logger.Error(err, "Failed to get all ReleasePlans")
		patch := client.MergeFrom(a.snapshot.DeepCopy())
		gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to get all ReleasePlans: "+err.Error())
		a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to get all ReleasePlans",
			a.snapshot, h.LogActionUpdate)
		er := a.client.Status().Patch(a.context, a.snapshot, patch)
		if er != nil {
			a.logger.Error(er, "Failed to mark snapshot integration status as invalid",
				"snapshot.Name", a.snapshot.Name)
			return a.RequeueIfYoungerThanThreshold(errors.Join(err, er))
		}
		return a.RequeueIfYoungerThanThreshold(err)
	}

	err = a.createMissingReleasesForReleasePlans(a.application, releasePlans, a.snapshot)
	if err != nil {
		a.logger.Error(err, "Failed to create new Releases")
		patch := client.MergeFrom(a.snapshot.DeepCopy())
		gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to create new Releases: "+err.Error())
		a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to create new Releases",
			a.snapshot, h.LogActionUpdate)
		er := a.client.Status().Patch(a.context, a.snapshot, patch)
		if er != nil {
			a.logger.Error(er, "Failed to mark snapshot integration status as invalid",
				"snapshot.Name", a.snapshot.Name)
			return a.RequeueIfYoungerThanThreshold(errors.Join(err, er))
		}
		return a.RequeueIfYoungerThanThreshold(err)
	}

	// Mark the Snapshot as already auto-released to prevent re-releasing the Snapshot when it gets reconciled
	// at a later time, especially if new ReleasePlans are introduced or existing ones are renamed
	err = gitops.MarkSnapshotAsAutoReleased(a.client, a.context, a.snapshot, "The Snapshot was auto-released")
	if err != nil {
		a.logger.Error(err, "Failed to update the Snapshot's status to auto-released")
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// EnsureSnapshotEnvironmentBindingExist is an operation that will ensure that all
// SnapshotEnvironmentBindings for non-ephemeral root environments point to the newly constructed snapshot.
// If the bindings don't already exist, it will create new ones for each of the environments.
func (a *Adapter) EnsureSnapshotEnvironmentBindingExist() (controller.OperationResult, error) {
	canSnapshotBePromoted, reasons := gitops.CanSnapshotBePromoted(a.snapshot)
	if !canSnapshotBePromoted {
		a.logger.Info("The Snapshot won't be deployed.",
			"reasons", strings.Join(reasons, ","))
		return controller.ContinueProcessing()
	}
	if gitops.IsSnapshotMarkedAsDeployedToRootEnvironments(a.snapshot) {
		a.logger.Info("The Snapshot was previously deployed to all root environments, skipping deployment.")
		return controller.ContinueProcessing()
	}

	availableEnvironments, err := a.findAvailableEnvironments()
	if err != nil {
		return controller.RequeueWithError(err)
	}

	for _, availableEnvironment := range *availableEnvironments {
		availableEnvironment := availableEnvironment // G601
		snapshotEnvironmentBinding, err := a.loader.FindExistingSnapshotEnvironmentBinding(a.client, a.context, a.application, &availableEnvironment)
		if err != nil {
			return controller.RequeueWithError(err)
		}
		if snapshotEnvironmentBinding != nil {
			err = a.updateExistingSnapshotEnvironmentBindingWithSnapshot(snapshotEnvironmentBinding, a.snapshot)
			if err != nil {
				a.logger.Error(err, "Failed to update SnapshotEnvironmentBinding",
					"snapshotEnvironmentBinding.Environment", snapshotEnvironmentBinding.Spec.Environment,
					"snapshotEnvironmentBinding.Snapshot", snapshotEnvironmentBinding.Spec.Snapshot)
				patch := client.MergeFrom(a.snapshot.DeepCopy())
				gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to update SnapshotEnvironmentBinding: "+err.Error())
				a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to update SnapshotEnvironmentBinding",
					a.snapshot, h.LogActionUpdate)
				return controller.RequeueOnErrorOrStop(a.client.Status().Patch(a.context, a.snapshot, patch))
			}
			a.logger.LogAuditEvent("Existing SnapshotEnvironmentBinding updated with Snapshot",
				snapshotEnvironmentBinding, h.LogActionUpdate,
				"snapshotEnvironmentBinding.Environment", snapshotEnvironmentBinding.Spec.Environment,
				"snapshotEnvironmentBinding.Snapshot", snapshotEnvironmentBinding.Spec.Snapshot)

		} else {
			snapshotEnvironmentBinding, err = a.createSnapshotEnvironmentBindingForSnapshot(a.application, &availableEnvironment, a.snapshot, map[string]string{})
			if err != nil {
				a.logger.Error(err, "Failed to create SnapshotEnvironmentBinding for snapshot",
					"environment", availableEnvironment.Name,
					"snapshot", a.snapshot.Name)
				patch := client.MergeFrom(a.snapshot.DeepCopy())
				gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to create SnapshotEnvironmentBinding: "+err.Error())
				a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to create SnapshotEnvironmentBinding",
					a.snapshot, h.LogActionUpdate)
				return controller.RequeueOnErrorOrStop(a.client.Status().Patch(a.context, a.snapshot, patch))
			}
			a.logger.LogAuditEvent("SnapshotEnvironmentBinding created for Snapshot",
				snapshotEnvironmentBinding, h.LogActionAdd,
				"snapshotEnvironmentBinding.Application", snapshotEnvironmentBinding.Spec.Application,
				"snapshotEnvironmentBinding.Environment", snapshotEnvironmentBinding.Spec.Environment,
				"snapshotEnvironmentBinding.Snapshot", snapshotEnvironmentBinding.Spec.Snapshot)

		}
	}

	// Mark the Snapshot as already deployed to root environments to prevent re-deploying the Snapshot when it gets
	// reconciled at a later time
	err = gitops.MarkSnapshotAsDeployedToRootEnvironments(a.client, a.context, a.snapshot, "The Snapshot was deployed to all available root environments at the time of promotion")
	if err != nil {
		a.logger.Error(err, "Failed to update the Snapshot's status to DeployedToRootEnvironments")
		return controller.RequeueWithError(err)
	}
	return controller.ContinueProcessing()
}

// createMissingReleasesForReleasePlans checks if there's existing Releases for a given list of ReleasePlans and creates
// new ones if they are missing. In case the Releases can't be created, an error will be returned.
func (a *Adapter) createMissingReleasesForReleasePlans(application *applicationapiv1alpha1.Application, releasePlans *[]releasev1alpha1.ReleasePlan, snapshot *applicationapiv1alpha1.Snapshot) error {
	releases, err := a.loader.GetReleasesWithSnapshot(a.client, a.context, a.snapshot)
	if err != nil {
		return err
	}

	firstRelease := true

	for _, releasePlan := range *releasePlans {
		releasePlan := releasePlan // G601
		existingRelease := release.FindMatchingReleaseWithReleasePlan(releases, releasePlan)
		if existingRelease != nil {
			a.logger.Info("Found existing Release",
				"snapshot.Name", snapshot.Name,
				"releasePlan.Name", releasePlan.Name,
				"release.Name", existingRelease.Name)
		} else {
			newRelease := release.NewReleaseForReleasePlan(&releasePlan, snapshot)
			// Propagate PAC annotations/labels from Snapshot to Release
			_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &newRelease.ObjectMeta, gitops.PipelinesAsCodePrefix)
			_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &newRelease.ObjectMeta, gitops.PipelinesAsCodePrefix)

			// Propagate annotations/labels prefixed with 'appstudio.openshift.io' from Snapshot to Release
			_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &newRelease.ObjectMeta, gitops.AppstudioLabelPrefix)
			_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &newRelease.ObjectMeta, gitops.AppstudioLabelPrefix)

			err := ctrl.SetControllerReference(application, newRelease, a.client.Scheme())
			if err != nil {
				return err
			}
			err = a.client.Create(a.context, newRelease)
			if err != nil {
				return err
			}
			a.logger.LogAuditEvent("Created a new Release", newRelease, h.LogActionAdd,
				"releasePlan.Name", releasePlan.Name)

			patch := client.MergeFrom(newRelease.DeepCopy())
			newRelease.SetAutomated()
			err = a.client.Status().Patch(a.context, newRelease, patch)
			if err != nil {
				return err
			}
			a.logger.Info("Marked Release status automated", "release.Name", newRelease.Name)
		}
		// Register the first release time for metrics calculation
		if firstRelease {
			startTime, ok := gitops.GetAppStudioTestsFinishedTime(a.snapshot)
			if ok {
				metrics.RegisterReleaseLatency(startTime)
				firstRelease = false
			}
		}
	}
	return nil
}

// findAvailableEnvironments gets all environments that don't have a ParentEnvironment and are not tagged as ephemeral.
func (a *Adapter) findAvailableEnvironments() (*[]applicationapiv1alpha1.Environment, error) {
	allEnvironments, err := a.loader.GetAllEnvironments(a.client, a.context, a.application)
	if err != nil {
		return nil, err
	}
	availableEnvironments := []applicationapiv1alpha1.Environment{}
	for _, environment := range *allEnvironments {
		if environment.Spec.ParentEnvironment == "" {
			isEphemeral := false
			for _, tag := range environment.Spec.Tags {
				if tag == "ephemeral" {
					isEphemeral = true
					break
				}
			}
			if !isEphemeral {
				availableEnvironments = append(availableEnvironments, environment)
			}
		}
	}
	return &availableEnvironments, nil
}

// createIntegrationPipelineRun creates and returns a new integration PipelineRun. The Pipeline information and the parameters to it
// will be extracted from the given integrationScenario. The integration's Snapshot will also be passed to the integration PipelineRun.
func (a *Adapter) createIntegrationPipelineRun(application *applicationapiv1alpha1.Application, integrationTestScenario *v1beta1.IntegrationTestScenario, snapshot *applicationapiv1alpha1.Snapshot) (*tektonv1.PipelineRun, error) {
	a.logger.Info("Creating new pipelinerun for integrationTestscenario",
		"integrationTestScenario.Name", integrationTestScenario.Name)

	pipelineRun := tekton.NewIntegrationPipelineRun(snapshot.Name, application.Namespace, *integrationTestScenario).
		WithSnapshot(snapshot).
		WithIntegrationLabels(integrationTestScenario).
		WithIntegrationAnnotations(integrationTestScenario).
		WithApplicationAndComponent(a.application, a.component).
		WithExtraParams(integrationTestScenario.Spec.Params).
		WithFinalizer(h.IntegrationPipelineRunFinalizer).
		AsPipelineRun()
	// copy PipelineRun PAC annotations/labels from snapshot to integration test PipelineRuns
	_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.PipelinesAsCodePrefix)
	_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.PipelinesAsCodePrefix)

	// Copy build labels and annotations prefixed with build.appstudio from snapshot to integration test PipelineRuns
	_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix)
	_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix)

	err := ctrl.SetControllerReference(snapshot, pipelineRun, a.client.Scheme())
	if err != nil {
		return nil, fmt.Errorf("failed to set snapshot %s as ControllerReference of pipelineRun: %w", snapshot.Name, err)
	}
	err = a.client.Create(a.context, pipelineRun)
	if err != nil {
		return nil, fmt.Errorf("failed to call client.Create to create pipelineRun for snapshot %s: %w", snapshot.Name, err)
	}

	go metrics.RegisterNewIntegrationPipelineRun()

	a.logger.LogAuditEvent("IntegrationTestscenario pipeline has been created", pipelineRun, h.LogActionAdd,
		"integrationTestScenario.Name", integrationTestScenario.Name)
	if gitops.IsSnapshotNotStarted(a.snapshot) {
		err := gitops.MarkSnapshotIntegrationStatusAsInProgress(a.client, a.context, a.snapshot, "Snapshot starts being tested by the integrationPipelineRun")
		if err != nil {
			a.logger.Error(err, "Failed to update integration status condition to in progress for snapshot")
		} else {
			a.logger.LogAuditEvent("Snapshot integration status marked as In Progress. Snapshot starts being tested by the integrationPipelineRun",
				a.snapshot, h.LogActionUpdate)
		}
	}
	return pipelineRun, nil
}

// createSnapshotEnvironmentBindingForSnapshot creates and returns a new snapshotEnvironmentBinding
// for the given application, environment, and snapshot.
// If it's not possible to create it and set the application as the owner, an error will be returned
func (a *Adapter) createSnapshotEnvironmentBindingForSnapshot(application *applicationapiv1alpha1.Application,
	environment *applicationapiv1alpha1.Environment, snapshot *applicationapiv1alpha1.Snapshot,
	additionalLabels map[string]string) (*applicationapiv1alpha1.SnapshotEnvironmentBinding, error) {
	bindingName := application.Name + "-" + environment.Name + "-" + "binding"

	snapshotEnvironmentBinding := gitops.NewSnapshotEnvironmentBinding(
		bindingName, application.Namespace, application.Name,
		environment.Name,
		snapshot)

	err := ctrl.SetControllerReference(snapshot, snapshotEnvironmentBinding, a.client.Scheme())
	if err != nil {
		return nil, err
	}

	_ = metadata.AddLabels(&snapshotEnvironmentBinding.ObjectMeta, additionalLabels)

	err = a.client.Create(a.context, snapshotEnvironmentBinding)
	if err != nil {
		return nil, err
	}

	return snapshotEnvironmentBinding, nil
}

// updateExistingSnapshotEnvironmentBindingWithSnapshot updates and returns snapshotEnvironmentBinding
// with the given snapshot. If it's not possible to patch, an error will be returned.
func (a *Adapter) updateExistingSnapshotEnvironmentBindingWithSnapshot(snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding,
	snapshot *applicationapiv1alpha1.Snapshot) error {

	patch := client.MergeFrom(snapshotEnvironmentBinding.DeepCopy())

	snapshotEnvironmentBinding.Spec.Snapshot = snapshot.Name
	snapshotComponents := gitops.NewBindingComponents(snapshot)
	snapshotEnvironmentBinding.Spec.Components = *snapshotComponents

	err := a.client.Patch(a.context, snapshotEnvironmentBinding, patch)
	if err != nil {
		return fmt.Errorf("failed to patch snapshotEnvironmentBinding: %w", err)
	}

	return nil
}

// RequeueIfYoungerThanThreshold checks if the adapter' snapshot is younger than the threshold defined
// in the function.  If it is, the function returns an operation result instructing the reconciler
// to requeue the object and the error message passed to the function.  If not, the function returns
// an operation result instructing the reconciler NOT to requeue the object.
func (a *Adapter) RequeueIfYoungerThanThreshold(retErr error) (controller.OperationResult, error) {
	snapshotCreationTime := a.snapshot.GetCreationTimestamp().Time
	durationSinceSnapshotCreation := time.Since(snapshotCreationTime)

	if durationSinceSnapshotCreation < RetryReleaseTimeout {
		return controller.RequeueWithError(retErr)
	}
	return controller.ContinueProcessing()
}

func (a *Adapter) HandlePipelineCreationError(err error, integrationTestScenario *v1beta1.IntegrationTestScenario, testStatuses *intgteststat.SnapshotIntegrationTestStatuses) (controller.OperationResult, error) {
	if clienterrors.IsInvalid(err) {
		a.logger.Error(err, "pipelineRun failed during creation due to invalid resource")
		testStatuses.UpdateTestStatusIfChanged(
			integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestInvalid,
			fmt.Sprintf("Creation of pipelineRun failed during creation due to invalid resource: %s.", err))
		itsErr := gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context)
		if itsErr != nil {
			a.logger.Error(err, "Failed to write Test Status into Snapshot")
			return controller.RequeueWithError(err)
		}
		return controller.StopProcessing()
	}
	a.logger.Error(err, "Failed to create pipelineRun for snapshot and scenario")
	return controller.RequeueWithError(err)
}
