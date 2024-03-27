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

package binding

import (
	"context"
	"fmt"
	"time"

	"github.com/redhat-appstudio/operator-toolkit/controller"
	"github.com/redhat-appstudio/operator-toolkit/metadata"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/metrics"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"

	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/integration-service/tekton"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const SnapshotEnvironmentBindingErrorTimeoutSeconds float64 = 300

// Adapter holds the objects needed to reconcile a SnapshotEnvironmentBinding.
type Adapter struct {
	snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding
	snapshot                   *applicationapiv1alpha1.Snapshot
	application                *applicationapiv1alpha1.Application
	component                  *applicationapiv1alpha1.Component
	environment                *applicationapiv1alpha1.Environment
	integrationTestScenario    *v1beta1.IntegrationTestScenario
	logger                     h.IntegrationLogger
	client                     client.Client
	context                    context.Context
	loader                     loader.ObjectLoader
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding, snapshot *applicationapiv1alpha1.Snapshot, environment *applicationapiv1alpha1.Environment, application *applicationapiv1alpha1.Application, component *applicationapiv1alpha1.Component, integrationTestScenario *v1beta1.IntegrationTestScenario, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		snapshotEnvironmentBinding: snapshotEnvironmentBinding,
		snapshot:                   snapshot,
		environment:                environment,
		application:                application,
		component:                  component,
		integrationTestScenario:    integrationTestScenario,
		logger:                     logger,
		loader:                     loader,
		client:                     client,
		context:                    context,
	}
}

// EnsureIntegrationTestPipelineForScenarioExists is an operation that will ensure that the Integration test pipeline
// associated with the Snapshot and the SnapshotEnvironmentBinding's IntegrationTestScenarios exist.
func (a *Adapter) EnsureIntegrationTestPipelineForScenarioExists() (controller.OperationResult, error) {
	if gitops.HaveBindingsFailed(a.snapshotEnvironmentBinding) {
		// don't log here it floods logs
		return controller.ContinueProcessing()
	}

	if !gitops.IsBindingDeployed(a.snapshotEnvironmentBinding) {
		a.logger.Info("The SnapshotEnvironmentBinding hasn't yet deployed to the ephemeral environment.", "snapshotEnvironmentBinding.Name", a.snapshotEnvironmentBinding.Name)
		return controller.ContinueProcessing()
	}
	a.logger.Info("The SnapshotEnvironmentBinding's deployment succeeded", "snapshotEnvironmentBinding.Name", a.snapshotEnvironmentBinding.Name)

	if a.integrationTestScenario != nil {
		// Check if an existing integration pipelineRun is registered in the Snapshot's status
		// We rely on this because the actual pipelineRun CR may have been pruned by this point
		testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
		if err != nil {
			a.logger.Error(err, "Failed to extract test statuses from the Snapshot's status annotation")
			return controller.RequeueWithError(err)
		}
		integrationTestScenarioStatus, ok := testStatuses.GetScenarioStatus(a.integrationTestScenario.Name)
		if ok && integrationTestScenarioStatus.TestPipelineRunName != "" {
			a.logger.Info("Found existing integrationPipelineRun",
				"integrationTestScenario.Name", a.integrationTestScenario.Name,
				"pipelineRun.Name", integrationTestScenarioStatus.TestPipelineRunName)
		} else {
			a.logger.Info("Creating new pipelinerun for integrationTestscenario",
				"integrationTestScenario.Name", a.integrationTestScenario.Name,
				"app name", a.application.Name,
				"namespace", a.application.Namespace)
			pipelineRun, err := a.createIntegrationPipelineRunWithEnvironment(a.application, a.integrationTestScenario, a.snapshot, a.environment)
			if err != nil {
				a.logger.Error(err, "Failed to create pipelineRun for snapshot, environment and scenario")
				return controller.RequeueWithError(err)
			}

			a.logger.LogAuditEvent("PipelineRun for snapshot created", pipelineRun, h.LogActionAdd,
				"snapshot.Name", a.snapshot.Name)
		}
	}
	return controller.ContinueProcessing()
}

// EnsureEphemeralEnvironmentsCleanedUp will ensure that ephemeral environment(s) associated with the
// SnapshotEnvironmentBinding are cleaned up.
func (a *Adapter) EnsureEphemeralEnvironmentsCleanedUp() (controller.OperationResult, error) {
	if !gitops.HaveBindingsFailed(a.snapshotEnvironmentBinding) {
		return controller.ContinueProcessing()
	}

	// The default value for ErrorOccured is 'True'.  We know that the state
	// of the condition is still 'True' due to the earlier call to
	// HaveBindingsFailed().  If this condition is still true after a
	// reasonable time then we assume that the SEB is stuck in an unrecoverable
	// state and clean it up.  Otherwise we requeue and wait until the timeout
	// has passed
	var lastTransitionTime time.Time
	bindingStatus := meta.FindStatusCondition(a.snapshotEnvironmentBinding.Status.BindingConditions, gitops.BindingErrorOccurredStatusConditionType)
	if bindingStatus != nil {
		lastTransitionTime = bindingStatus.LastTransitionTime.Time
	}
	sinceLastTransition := time.Since(lastTransitionTime).Seconds()
	if sinceLastTransition < SnapshotEnvironmentBindingErrorTimeoutSeconds {
		// don't log here, it floods logs
		return controller.RequeueAfter(time.Duration(SnapshotEnvironmentBindingErrorTimeoutSeconds*float64(time.Second)), nil)
	}

	reasonMsg := "Unknown reason"

	if conditionStatus := gitops.GetBindingConditionStatus(a.snapshotEnvironmentBinding); conditionStatus != nil {
		reasonMsg = fmt.Sprintf("%s (%s)", conditionStatus.Reason, conditionStatus.Message)
	}
	// we do not want to scare users prematurely, report error only after SEB timeout for trying to deploy
	a.logger.Info(
		fmt.Sprintf("SEB has been in the error state for more than the threshold time of %f seconds", SnapshotEnvironmentBindingErrorTimeoutSeconds),
		"reason", reasonMsg,
	)

	err := a.writeTestStatusIntoSnapshot(intgteststat.IntegrationTestStatusDeploymentError_Deprecated,
		fmt.Sprintf("The SnapshotEnvironmentBinding has failed to deploy on ephemeral environment: %s", reasonMsg))
	if err != nil {
		return controller.RequeueWithError(fmt.Errorf("failed to update snapshot test status: %w", err))
	}

	// mark snapshot as failed
	snapshotErrorMessage := "Encountered issue deploying snapshot on ephemeral environments: " +
		meta.FindStatusCondition(a.snapshotEnvironmentBinding.Status.BindingConditions, "ErrorOccurred").Message
	a.logger.Info("The SnapshotEnvironmentBinding encountered an issue deploying snapshot on ephemeral environments",
		"snapshotEnvironmentBinding.Name", a.snapshotEnvironmentBinding.Name,
		"message", snapshotErrorMessage)

	if !gitops.IsSnapshotMarkedAsFailed(a.snapshot) {
		err = gitops.MarkSnapshotAsFailed(a.client, a.context, a.snapshot, snapshotErrorMessage)
		if err != nil {
			a.logger.Error(err, "Failed to Update Snapshot status")
			return controller.RequeueWithError(err)
		}
	}

	deploymentTargetClaim, err := a.loader.GetDeploymentTargetClaimForEnvironment(a.client, a.context, a.environment)
	if err != nil {
		a.logger.Error(err, "failed to find deploymentTargetClaim for environment",
			"environment", a.environment.Name)
		return controller.RequeueWithError(err)
	}

	err = h.CleanUpEphemeralEnvironments(a.client, &a.logger, a.context, a.environment, deploymentTargetClaim)
	if err != nil {
		a.logger.Error(err, "Failed to delete the Ephemeral Environment")
		return controller.RequeueWithError(err)
	}
	return controller.ContinueProcessing()

}

// createIntegrationPipelineRunWithEnvironment creates new integration PipelineRun. The Pipeline information and the parameters to it
// will be extracted from the given integrationScenario. The integration's Snapshot will also be passed to the integration PipelineRun.
// If the creation of the PipelineRun is unsuccessful, an error will be returned.
func (a *Adapter) createIntegrationPipelineRunWithEnvironment(application *applicationapiv1alpha1.Application, integrationTestScenario *v1beta1.IntegrationTestScenario, snapshot *applicationapiv1alpha1.Snapshot, environment *applicationapiv1alpha1.Environment) (*tektonv1.PipelineRun, error) {
	deploymentTarget, err := a.getDeploymentTargetForEnvironment(environment)
	if err != nil || deploymentTarget == nil {
		return nil, err
	}

	pipelineRun := tekton.NewIntegrationPipelineRun(snapshot.Name, application.Namespace, *integrationTestScenario).
		WithSnapshot(snapshot).
		WithIntegrationLabels(integrationTestScenario).
		WithIntegrationAnnotations(integrationTestScenario).
		WithApplicationAndComponent(a.application, a.component).
		WithExtraParams(integrationTestScenario.Spec.Params).
		WithEnvironmentAndDeploymentTarget(deploymentTarget, environment.Name).
		WithFinalizer(h.IntegrationPipelineRunFinalizer).
		AsPipelineRun()

	// copy PipelineRun PAC annotations/labels from snapshot to integration test PipelineRuns
	_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.PipelinesAsCodePrefix)
	_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.PipelinesAsCodePrefix)

	// Copy build labels and annotations prefixed with build.appstudio from snapshot to integration test PipelineRuns
	_ = metadata.CopyLabelsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix)
	_ = metadata.CopyAnnotationsByPrefix(&snapshot.ObjectMeta, &pipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix)

	err = ctrl.SetControllerReference(snapshot, pipelineRun, a.client.Scheme())
	if err != nil {
		return nil, err
	}
	err = a.client.Create(a.context, pipelineRun)
	if err != nil {
		return nil, err
	}

	go metrics.RegisterNewIntegrationPipelineRun()

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

// getDeploymentTargetForEnvironment gets the DeploymentTarget associated with Environment, if the DeploymentTarget is not found, an error will be returned
func (a *Adapter) getDeploymentTargetForEnvironment(environment *applicationapiv1alpha1.Environment) (*applicationapiv1alpha1.DeploymentTarget, error) {
	deploymentTargetClaim, err := a.loader.GetDeploymentTargetClaimForEnvironment(a.client, a.context, environment)
	if err != nil {
		return nil, fmt.Errorf("failed to find deploymentTargetClaim defined in environment %s: %w", environment.Name, err)
	}

	deploymentTarget, err := a.loader.GetDeploymentTargetForDeploymentTargetClaim(a.client, a.context, deploymentTargetClaim)
	if err != nil {
		return nil, fmt.Errorf("failed to find deploymentTarget defined in deploymentTargetClaim %s: %w", deploymentTargetClaim.Name, err)
	}

	return deploymentTarget, nil
}

// writeTestStatusIntoSnapshot updates test status and instantly writes changes into test result annotation
func (a *Adapter) writeTestStatusIntoSnapshot(status intgteststat.IntegrationTestStatus, details string) error {
	testStatuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(a.snapshot)
	if err != nil {
		return err
	}
	testStatuses.UpdateTestStatusIfChanged(a.integrationTestScenario.Name, status, details)
	err = gitops.WriteIntegrationTestStatusesIntoSnapshot(a.snapshot, testStatuses, a.client, a.context)
	if err != nil {
		return err

	}
	return nil
}
