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
	"k8s.io/client-go/util/retry"
	"reflect"
	"strings"
	"time"

	clienterrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/konflux-ci/integration-service/api/v1beta2"
	"github.com/konflux-ci/integration-service/gitops"
	h "github.com/konflux-ci/integration-service/helpers"
	intgteststat "github.com/konflux-ci/integration-service/pkg/integrationteststatus"
	"github.com/konflux-ci/integration-service/pkg/metrics"
	"github.com/konflux-ci/integration-service/release"
	"github.com/konflux-ci/integration-service/tekton"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"

	"github.com/konflux-ci/integration-service/loader"
	"github.com/konflux-ci/operator-toolkit/controller"
	"github.com/konflux-ci/operator-toolkit/metadata"
	releasev1alpha1 "github.com/konflux-ci/release-service/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const SnapshotRetryTimeout = time.Duration(3 * time.Hour)

// configuration options for scenario
type ScenarioOptions struct {
	IsReRun bool
}

// Adapter holds the objects needed to reconcile a Release.
type Adapter struct {
	snapshot    *applicationapiv1alpha1.Snapshot
	application *applicationapiv1alpha1.Application
	logger      h.IntegrationLogger
	loader      loader.ObjectLoader
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(context context.Context, snapshot *applicationapiv1alpha1.Snapshot, application *applicationapiv1alpha1.Application, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
) *Adapter {
	return &Adapter{
		snapshot:    snapshot,
		application: application,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

func scenariosNamesToList(integrationTestScenarios *[]v1beta2.IntegrationTestScenario) *[]string {
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
	integrationTestScenario, err := a.loader.GetScenario(a.context, a.client, scenarioName, a.application.Namespace)

	if err != nil {
		if clienterrors.IsNotFound(err) {
			a.logger.Error(err, "scenario for integration test re-run not found", "scenario", scenarioName)
			// scenario doesn't exist just remove label and continue
			if err = gitops.RemoveIntegrationTestRerunLabel(a.context, a.client, a.snapshot); err != nil {
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
		if err = gitops.RemoveIntegrationTestRerunLabel(a.context, a.client, a.snapshot); err != nil {
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

	if err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.context, a.snapshot, testStatuses, a.client); err != nil {
		return controller.RequeueWithError(err)
	}

	if err = gitops.ResetSnapshotStatusConditions(a.context, a.client, a.snapshot, "Integration test is being rerun for snapshot"); err != nil {
		a.logger.Error(err, "Failed to reset snapshot status conditions")
		return controller.RequeueWithError(err)
	}

	if err = gitops.RemoveIntegrationTestRerunLabel(a.context, a.client, a.snapshot); err != nil {
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

	integrationTestScenarios, err := a.loader.GetAllIntegrationTestScenariosForApplication(a.context, a.client, a.application)
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
		err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.context, a.snapshot, testStatuses, a.client)
		if err != nil {
			return controller.RequeueWithError(err)
		}
		defer func() {
			// Try to update status of test if something failed, because test loop can stop prematurely on error,
			// we should record current status.
			// This is only best effort update
			//
			// When update of statuses worked fine at the end of function, this is just a no-op
			err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.context, a.snapshot, testStatuses, a.client)
			if err != nil {
				a.logger.Error(err, "Defer: Updating statuses of tests in snapshot failed")
			}
		}()

		var errsForPLRCreation error
		for _, integrationTestScenario := range *integrationTestScenarios {
			integrationTestScenario := integrationTestScenario //G601
			if !h.IsScenarioValid(&integrationTestScenario) {
				a.logger.Info("IntegrationTestScenario is invalid, will not create pipelineRun for it",
					"integrationTestScenario.Name", integrationTestScenario.Name)
				scenarioStatusCondition := meta.FindStatusCondition(integrationTestScenario.Status.Conditions, h.IntegrationTestScenarioValid)
				testStatuses.UpdateTestStatusIfChanged(
					integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestInvalid,
					fmt.Sprintf("IntegrationTestScenario '%s' is invalid: %s", integrationTestScenario.Name, scenarioStatusCondition.Message))
				continue
			}
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
					a.logger.Error(err, "Failed to create pipelineRun for snapshot and scenario",
						"integrationScenario.Name", integrationTestScenario.Name)

					testStatuses.UpdateTestStatusIfChanged(
						integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestInvalid,
						fmt.Sprintf("Creation of pipelineRun failed during creation due to: %s.", err))

					if !clienterrors.IsInvalid(err) {
						errsForPLRCreation = errors.Join(errsForPLRCreation, err)
					}
					continue
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

		err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.context, a.snapshot, testStatuses, a.client)
		if err != nil {
			a.logger.Error(err, "Failed to update test status in snapshot annotation")
			errsForPLRCreation = errors.Join(errsForPLRCreation, err)
		}

		if errsForPLRCreation != nil {
			return controller.RequeueWithError(errsForPLRCreation)
		}
	}

	requiredIntegrationTestScenarios, err := a.loader.GetRequiredIntegrationTestScenariosForApplication(a.context, a.client, a.application)
	if err != nil {
		a.logger.Error(err, "Failed to get all required IntegrationTestScenarios")
		patch := client.MergeFrom(a.snapshot.DeepCopy())
		gitops.SetSnapshotIntegrationStatusAsError(a.snapshot, "Failed to get all required IntegrationTestScenarios: "+err.Error())
		a.logger.LogAuditEvent("Snapshot integration status marked as Invalid. Failed to get all required IntegrationTestScenarios",
			a.snapshot, h.LogActionUpdate)
		return controller.RequeueOnErrorOrStop(a.client.Status().Patch(a.context, a.snapshot, patch))
	}
	if len(*requiredIntegrationTestScenarios) == 0 && !gitops.IsSnapshotMarkedAsPassed(a.snapshot) {
		err := gitops.MarkSnapshotAsPassed(a.context, a.client, a.snapshot, "No required IntegrationTestScenarios found, skipped testing")
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
// being updated when the Snapshot is created
func (a *Adapter) EnsureGlobalCandidateImageUpdated() (controller.OperationResult, error) {
	if !gitops.IsComponentSnapshotCreatedByPACPushEvent(a.snapshot) && !gitops.IsOverrideSnapshot(a.snapshot) {
		a.logger.Info("The Snapshot was neither created for a single component push event nor override type, not updating the global candidate list.")
		return controller.ContinueProcessing()
	}

	if gitops.IsSnapshotMarkedAsAddedToGlobalCandidateList(a.snapshot) {
		a.logger.Info("The Snapshot's component was previously added to the global candidate list, skipping adding it.")
		return controller.ContinueProcessing()
	}

	var componentToUpdate *applicationapiv1alpha1.Component
	var err error

	// get component from component snapshot
	if gitops.IsComponentSnapshot(a.snapshot) {
		err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
			componentToUpdate, err = a.loader.GetComponentFromSnapshot(a.context, a.client, a.snapshot)
			return err
		})
		if err != nil {
			_, loaderError := h.HandleLoaderError(a.logger, err, fmt.Sprintf("Component or '%s' label", tekton.ComponentNameLabel), "Snapshot")
			if loaderError != nil {
				return controller.RequeueWithError(loaderError)
			}
			return controller.ContinueProcessing()
		}

		// look for the expected snapshotComponnet and update
		for _, snapshotComponent := range a.snapshot.Spec.Components {
			snapshotComponent := snapshotComponent //G601
			if snapshotComponent.Name == componentToUpdate.Name {
				//update .Spec.ContainerImage for the component included in component snapshot
				err = a.updateComponentContainerImage(a.context, a.client, componentToUpdate, &snapshotComponent)
				if err != nil {
					return controller.RequeueWithError(err)
				}

				// update .Status.LastBuiltCommit for the component included in component snapshot
				err = a.updateComponentSource(a.context, a.client, componentToUpdate, &snapshotComponent)
				if err != nil {
					return controller.RequeueWithError(err)
				}
				// Updated the component for component snapshot, break
				break
			} else {
				// expected component not found, continue
				continue
			}
		}
	} else if gitops.IsOverrideSnapshot(a.snapshot) {
		// update Spec.ContainerImage for each component in override snapshot
		for _, snapshotComponent := range a.snapshot.Spec.Components {
			snapshotComponent := snapshotComponent //G601
			// get component for each snapshotComponent in override snapshot
			componentToUpdate, err = a.loader.GetComponent(a.context, a.client, snapshotComponent.Name, a.snapshot.Namespace)
			if err != nil {
				a.logger.Error(err, "Failed to get component from applicaion, won't update global candidate list for this component", "application.Name", a.application.Name, "component.Name", snapshotComponent.Name)
				_, loaderError := h.HandleLoaderError(a.logger, err, snapshotComponent.Name, a.application.Name)
				if loaderError != nil {
					return controller.RequeueWithError(loaderError)
				}
				continue
			}

			// if the containerImage in snapshotComponent doesn't have a valid digest, the containerImage
			// will not be added to component Global Candidate List
			err = gitops.ValidateImageDigest(snapshotComponent.ContainerImage)
			if err != nil {
				a.logger.Error(err, "containerImage cannot be updated to component Global Candidate List due to invalid digest in containerImage", "component.Name", snapshotComponent.Name, "snapshotComponent.ContainerImage", snapshotComponent.ContainerImage)
				continue
			}

			//update component.Spec.ContainerImage for each snapshotComponent in override snapshot
			err = a.updateComponentContainerImage(a.context, a.client, componentToUpdate, &snapshotComponent)
			if err != nil {
				return controller.RequeueWithError(err)
			}
			// update .Status.LastBuiltCommit for each snapshotComponent in override snapshot
			err = a.updateComponentSource(a.context, a.client, componentToUpdate, &snapshotComponent)
			if err != nil {
				return controller.RequeueWithError(err)
			}
		}
	}

	// Mark the Snapshot as already added to global candidate list to prevent it from getting added again when the Snapshot
	// gets reconciled at a later time
	err = gitops.MarkSnapshotAsAddedToGlobalCandidateList(a.context, a.client, a.snapshot, "The Snapshot's component(s) was/were added to the global candidate list")
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

	releasePlans, err := a.loader.GetAutoReleasePlansForApplication(a.context, a.client, a.application)
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
	err = gitops.MarkSnapshotAsAutoReleased(a.context, a.client, a.snapshot, "The Snapshot was auto-released")
	if err != nil {
		a.logger.Error(err, "Failed to update the Snapshot's status to auto-released")
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// createMissingReleasesForReleasePlans checks if there's existing Releases for a given list of ReleasePlans and creates
// new ones if they are missing. In case the Releases can't be created, an error will be returned.
func (a *Adapter) createMissingReleasesForReleasePlans(application *applicationapiv1alpha1.Application, releasePlans *[]releasev1alpha1.ReleasePlan, snapshot *applicationapiv1alpha1.Snapshot) error {
	releases, err := a.loader.GetReleasesWithSnapshot(a.context, a.client, a.snapshot)
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

func resolverParamsToMap(params []v1beta2.ResolverParameter) map[string]string {
	result := make(map[string]string)
	for _, param := range params {
		result[param.Name] = param.Value
	}
	return result
}

// urlToGitUrl appends `.git` to the URL if it doesn't already have it
func urlToGitUrl(url string) string {
	if strings.HasSuffix(url, ".git") {
		return url
	}
	return url + ".git"
}

// shouldUpdateIntegrationTestGitResolver checks if the integration test resolver should be updated based on the source repo
func shouldUpdateIntegrationTestGitResolver(integrationTestScenario *v1beta2.IntegrationTestScenario, snapshot *applicationapiv1alpha1.Snapshot) bool {

	// only "pull-requests" are applicable
	if gitops.IsSnapshotCreatedByPACPushEvent(snapshot) {
		return false
	}

	testResolverRef := integrationTestScenario.Spec.ResolverRef

	// only tekton git resolver is applicable
	if testResolverRef.Resolver != tekton.TektonResolverGit {
		return false
	}

	annotations := snapshot.GetAnnotations()
	params := resolverParamsToMap(testResolverRef.Params)

	if urlVal, ok := params[tekton.TektonResolverGitParamURL]; ok {
		// urlVal may or may not have git suffix specified :')
		return urlToGitUrl(urlVal) == urlToGitUrl(annotations[gitops.PipelineAsCodeRepoURLAnnotation])
	}

	// undefined state, no idea what was configured in resolver, don't touch it
	return false
}

func getGitResolverUpdateMap(snapshot *applicationapiv1alpha1.Snapshot) map[string]string {
	annotations := snapshot.GetAnnotations()
	return map[string]string{
		tekton.TektonResolverGitParamURL:      urlToGitUrl(annotations[gitops.SnapshotGitSourceRepoURLAnnotation]), // should have .git in url for consistency and compatibility
		tekton.TektonResolverGitParamRevision: annotations[gitops.PipelineAsCodeSHAAnnotation],
	}
}

// createIntegrationPipelineRun creates and returns a new integration PipelineRun. The Pipeline information and the parameters to it
// will be extracted from the given integrationScenario. The integration's Snapshot will also be passed to the integration PipelineRun.
func (a *Adapter) createIntegrationPipelineRun(application *applicationapiv1alpha1.Application, integrationTestScenario *v1beta2.IntegrationTestScenario, snapshot *applicationapiv1alpha1.Snapshot) (*tektonv1.PipelineRun, error) {
	a.logger.Info("Creating new pipelinerun for integrationTestscenario",
		"integrationTestScenario.Name", integrationTestScenario.Name)

	pipelineRunBuilder := tekton.NewIntegrationPipelineRun(snapshot.Name, application.Namespace, *integrationTestScenario).
		WithSnapshot(snapshot).
		WithIntegrationLabels(integrationTestScenario).
		WithIntegrationAnnotations(integrationTestScenario).
		WithApplication(a.application).
		WithExtraParams(integrationTestScenario.Spec.Params).
		WithFinalizer(h.IntegrationPipelineRunFinalizer).
		WithDefaultIntegrationTimeouts(a.logger.Logger)

	if shouldUpdateIntegrationTestGitResolver(integrationTestScenario, snapshot) {
		pipelineRunBuilder.WithUpdatedTestsGitResolver(getGitResolverUpdateMap(snapshot))
	}

	pipelineRun := pipelineRunBuilder.AsPipelineRun()

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
		err := gitops.MarkSnapshotIntegrationStatusAsInProgress(a.context, a.client, a.snapshot, "Snapshot starts being tested by the integrationPipelineRun")
		if err != nil {
			a.logger.Error(err, "Failed to update integration status condition to in progress for snapshot")
		} else {
			a.logger.LogAuditEvent("Snapshot integration status marked as In Progress. Snapshot starts being tested by the integrationPipelineRun",
				a.snapshot, h.LogActionUpdate)
		}
	}
	return pipelineRun, nil
}

// RequeueIfYoungerThanThreshold checks if the adapter' snapshot is younger than the threshold defined
// in the function.  If it is, the function returns an operation result instructing the reconciler
// to requeue the object and the error message passed to the function.  If not, the function returns
// an operation result instructing the reconciler NOT to requeue the object.
func (a *Adapter) RequeueIfYoungerThanThreshold(retErr error) (controller.OperationResult, error) {
	if h.IsObjectYoungerThanThreshold(a.snapshot, SnapshotRetryTimeout) {
		return controller.RequeueWithError(retErr)
	}
	return controller.ContinueProcessing()
}

func (a *Adapter) HandlePipelineCreationError(err error, integrationTestScenario *v1beta2.IntegrationTestScenario, testStatuses *intgteststat.SnapshotIntegrationTestStatuses) (controller.OperationResult, error) {
	a.logger.Error(err, "Failed to create pipelineRun for snapshot and scenario",
		"integrationScenario.Name", integrationTestScenario.Name)
	testStatuses.UpdateTestStatusIfChanged(
		integrationTestScenario.Name, intgteststat.IntegrationTestStatusTestInvalid,
		fmt.Sprintf("Creation of pipelineRun failed during creation due to: %s.", err))
	itsErr := gitops.WriteIntegrationTestStatusesIntoSnapshot(a.context, a.snapshot, testStatuses, a.client)
	if itsErr != nil {
		a.logger.Error(err, "Failed to write Test Status into Snapshot")
		return controller.RequeueWithError(itsErr)
	}

	if strings.Contains(err.Error(), "admission webhook") && strings.Contains(err.Error(), "denied the request") {
		//Stop processing in case the error runs in admission webhook validation error:
		//failed to call client.Create to create pipelineRun for snapshot
		//<snapshot-name>: admission webhook \"validation.webhook.pipeline.tekton.dev\" denied the request: validation failed: <reason>
		return controller.StopProcessing()
	}
	if clienterrors.IsInvalid(err) {
		return controller.StopProcessing()
	}
	return controller.RequeueWithError(err)
}

func (a *Adapter) updateComponentContainerImage(ctx context.Context, c client.Client, component *applicationapiv1alpha1.Component, snapshotComponent *applicationapiv1alpha1.SnapshotComponent) error {
	patch := client.MergeFrom(component.DeepCopy())
	component.Spec.ContainerImage = snapshotComponent.ContainerImage
	err := a.client.Patch(a.context, component, patch)
	if err != nil {
		a.logger.Error(err, "Failed to update .Spec.ContainerImage of Global Candidate for the Component",
			"component.Name", component.Name)
		return err
	}
	a.logger.LogAuditEvent("Updated .Spec.ContainerImage of Global Candidate for the Component",
		component, h.LogActionUpdate,
		"containerImage", snapshotComponent.ContainerImage)
	return nil
}

func (a *Adapter) updateComponentSource(ctx context.Context, c client.Client, component *applicationapiv1alpha1.Component, snapshotComponent *applicationapiv1alpha1.SnapshotComponent) error {
	if reflect.ValueOf(snapshotComponent.Source).IsValid() && snapshotComponent.Source.GitSource != nil && snapshotComponent.Source.GitSource.Revision != "" {
		patch := client.MergeFrom(component.DeepCopy())
		component.Status.LastBuiltCommit = snapshotComponent.Source.GitSource.Revision
		err := a.client.Status().Patch(a.context, component, patch)
		if err != nil {
			a.logger.Error(err, "Failed to update .Status.LastBuiltCommit of Global Candidate for the Component",
				"component.Name", component.Name)
			return err
		}
		a.logger.LogAuditEvent("Updated .Status.LastBuiltCommit of Global Candidate for the Component",
			component, h.LogActionUpdate,
			"lastBuildCommit", component.Status.LastBuiltCommit)
	}
	return nil
}
