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

package scenario

import (
	"context"
	"reflect"

	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Adapter holds the objects needed to reconcile a Release.
type Adapter struct {
	application *applicationapiv1alpha1.Application
	scenario    *v1beta1.IntegrationTestScenario
	logger      h.IntegrationLogger
	loader      loader.ObjectLoader
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(application *applicationapiv1alpha1.Application, scenario *v1beta1.IntegrationTestScenario, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		application: application,
		scenario:    scenario,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

// EnsureCreatedScenarioIsValid is an operation that ensures created IntegrationTestScenario is valid
// in case it is, set its owner reference
func (a *Adapter) EnsureCreatedScenarioIsValid() (controller.OperationResult, error) {
	if a.scenario.DeletionTimestamp != nil {
		return controller.ContinueProcessing()
	}

	// Check if application exists or not
	if a.application == nil {
		a.logger.Info("Application for scenario was not found.")

		patch := client.MergeFrom(a.scenario.DeepCopy())
		SetScenarioIntegrationStatusAsInvalid(a.scenario, "Failed to get application for scenario.")
		err := a.client.Status().Patch(a.context, a.scenario, patch)
		if err != nil {
			a.logger.Error(err, "Failed to update Scenario")
			return controller.RequeueWithError(err)
		}
		return controller.ContinueProcessing()
	}

	// application exist, always log it
	a.logger = a.logger.WithApp(*a.application)
	// Checks if scenario has ownerReference assigned to it
	if a.scenario.OwnerReferences == nil {
		patch := client.MergeFrom(a.scenario.DeepCopy())
		err := ctrl.SetControllerReference(a.application, a.scenario, a.client.Scheme())
		if err != nil {
			a.logger.Error(err, "Error setting owner reference.")
			return controller.RequeueWithError(err)
		}
		err = a.client.Patch(a.context, a.scenario, patch)
		if err != nil {
			a.logger.Error(err, "Failed to update Scenario")
			return controller.RequeueWithError(err)
		}

	}
	// Checks if scenario has environment defined
	if reflect.ValueOf(a.scenario.Spec.Environment).IsZero() {
		a.logger.Info("IntegrationTestScenario has no environment defined")
	} else {
		//Same as function getEnvironmentFromIntegrationTestScenario - this could be later changed to call only that function
		ITSEnv := &applicationapiv1alpha1.Environment{}

		err := a.client.Get(a.context, types.NamespacedName{
			Namespace: a.application.Namespace,
			Name:      a.scenario.Spec.Environment.Name,
		}, ITSEnv)

		if err != nil {
			a.logger.Info("Environment doesn't exist in same namespace as IntegrationTestScenario.",
				"environment.Name:", a.scenario.Spec.Environment.Name)
			patch := client.MergeFrom(a.scenario.DeepCopy())
			SetScenarioIntegrationStatusAsInvalid(a.scenario, "Environment "+a.scenario.Spec.Environment.Name+" is located in different namespace than scenario.")
			err = a.client.Status().Patch(a.context, a.scenario, patch)
			if err != nil {
				a.logger.Error(err, "Failed to update Scenario")
				return controller.RequeueWithError(err)
			}
			a.logger.LogAuditEvent("IntegrationTestScenario marked as Invalid. Environment "+a.scenario.Spec.Environment.Name+" is located in different namespace than scenario. ",
				a.scenario, h.LogActionUpdate)
			return controller.ContinueProcessing()
		}

	}

	// If the scenario status IntegrationTestScenarioValid condition is not defined or false, we set it to Valid
	if reflect.ValueOf(a.scenario.Status).IsZero() || !meta.IsStatusConditionTrue(a.scenario.Status.Conditions, gitops.IntegrationTestScenarioValid) {
		patch := client.MergeFrom(a.scenario.DeepCopy())
		SetScenarioIntegrationStatusAsValid(a.scenario, "Integration test scenario is Valid.")
		err := a.client.Status().Patch(a.context, a.scenario, patch)
		if err != nil {
			a.logger.Error(err, "Failed to update Scenario")
			return controller.RequeueWithError(err)
		}
		a.logger.LogAuditEvent("IntegrationTestScenario marked as Valid", a.scenario, h.LogActionUpdate)
	}

	err := h.AddFinalizerToScenario(a.client, a.logger, a.context, a.scenario, h.IntegrationTestScenarioFinalizer)
	if err != nil {
		a.logger.Error(err, "Failed to add finalizer to IntegrationTestScenario")
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// EnsureDeletedScenarioResourcesAreCleanedUp is an operation that ensures that all resources related to the
// deleted IntegrationTestScenario are cleaned up.
func (a *Adapter) EnsureDeletedScenarioResourcesAreCleanedUp() (controller.OperationResult, error) {
	if a.scenario.DeletionTimestamp == nil {
		return controller.ContinueProcessing()
	}

	environmentsForScenario, err := a.loader.GetAllEnvironmentsForScenario(a.client, a.context, a.scenario)
	if err != nil {
		a.logger.Error(err, "Failed to find all Environments for IntegrationTestScenario")
		return controller.RequeueWithError(err)
	}
	for _, testEnvironment := range *environmentsForScenario {
		if h.IsEnvironmentEphemeral(&testEnvironment) {
			dtc, err := a.loader.GetDeploymentTargetClaimForEnvironment(a.client, a.context, &testEnvironment)
			if err != nil || dtc == nil {
				a.logger.Error(err, "Failed to find deploymentTargetClaim defined in environment", "environment.Name", &testEnvironment.Name)
				return controller.RequeueWithError(err)
			}

			err = h.CleanUpEphemeralEnvironments(a.client, &a.logger, a.context, &testEnvironment, dtc)
			if err != nil {
				a.logger.Error(err, "Failed to delete the Ephemeral Environment")
				return controller.RequeueWithError(err)
			}
		}
	}

	// Remove the finalizer from the scenario since the cleanup has been handled
	err = h.RemoveFinalizerFromScenario(a.client, a.logger, a.context, a.scenario, h.IntegrationTestScenarioFinalizer)
	if err != nil {
		a.logger.Error(err, "Failed to remove the finalizer from the Scenario")
		return controller.RequeueWithError(err)
	}
	return controller.ContinueProcessing()
}

// SetScenarioIntegrationStatusAsInvalid sets the IntegrationTestScenarioValid status condition for the Scenario to invalid.
func SetScenarioIntegrationStatusAsInvalid(scenario *v1beta1.IntegrationTestScenario, message string) {
	meta.SetStatusCondition(&scenario.Status.Conditions, metav1.Condition{
		Type:    gitops.IntegrationTestScenarioValid,
		Status:  metav1.ConditionFalse,
		Reason:  gitops.AppStudioIntegrationStatusInvalid,
		Message: message,
	})
}

// SetScenarioIntegrationStatusAsValid sets the IntegrationTestScenarioValid integration status condition for the Scenario to valid.
func SetScenarioIntegrationStatusAsValid(scenario *v1beta1.IntegrationTestScenario, message string) {
	meta.SetStatusCondition(&scenario.Status.Conditions, metav1.Condition{
		Type:    gitops.IntegrationTestScenarioValid,
		Status:  metav1.ConditionTrue,
		Reason:  gitops.AppStudioIntegrationStatusValid,
		Message: message,
	})
}
