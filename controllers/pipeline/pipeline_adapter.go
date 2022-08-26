/*
Copyright 2022.

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

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-logr/logr"
	hasv1alpha1 "github.com/redhat-appstudio/application-service/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/controllers/results"
	"github.com/redhat-appstudio/integration-service/tekton"
	appstudioshared "github.com/redhat-appstudio/managed-gitops/appstudio-shared/apis/appstudio.redhat.com/v1alpha1"
	tektonv1beta1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

// Adapter holds the objects needed to reconcile a Release.
type Adapter struct {
	pipelineRun *tektonv1beta1.PipelineRun
	component   *hasv1alpha1.Component
	application *hasv1alpha1.Application
	logger      logr.Logger
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(pipelineRun *tektonv1beta1.PipelineRun, component *hasv1alpha1.Component, application *hasv1alpha1.Application, logger logr.Logger, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		pipelineRun: pipelineRun,
		component:   component,
		application: application,
		logger:      logger,
		client:      client,
		context:     context,
	}
}

// EnsureApplicationSnapshotExists is an operation that will ensure that an pipeline ApplicationSnapshot associated
// to the PipelineRun being processed exists. Otherwise, it will create a new pipeline ApplicationSnapshot.
func (a *Adapter) EnsureApplicationSnapshotExists() (results.OperationResult, error) {
	if !tekton.IsBuildPipelineRun(a.pipelineRun) {
		return results.ContinueProcessing()
	} else {
		existingApplicationSnapshot, err := a.findMatchingApplicationSnapshot()
		if err != nil {
			return results.RequeueWithError(err)
		}

		if existingApplicationSnapshot != nil {
			a.logger.Info("Found existing ApplicationSnapshot",
				"Application.Name", a.application.Name,
				"ApplicationSnapshot.Name", existingApplicationSnapshot.Name,
				"ApplicationSnapshot.Spec.Components", existingApplicationSnapshot.Spec.Components)
			return results.ContinueProcessing()
		}

		newApplicationSnapshot, err := a.createApplicationSnapshotForPipelineRun(a.pipelineRun, a.component, a.application)
		if err != nil {
			a.logger.Error(err, "Failed to create ApplicationSnapshot",
				"Application.Name", a.application.Name, "Application.Namespace", a.application.Namespace)
			return results.RequeueOnErrorOrStop(a.updateStatus())
		}

		a.logger.Info("Created new ApplicationSnapshot",
			"Application.Name", a.application.Name,
			"ApplicationSnapshot.Name", newApplicationSnapshot.Name,
			"ApplicationSnapshot.Spec.Components", newApplicationSnapshot.Spec.Components)

		return results.ContinueProcessing()
	}
}

// EnsureApplicationSnapshotPassedAllTests is an operation that will ensure that a pipeline ApplicationSnapshot
// to the PipelineRun being processed passed all tests for all defined IntegrationTestScenarios.
func (a *Adapter) EnsureApplicationSnapshotPassedAllTests() (results.OperationResult, error) {
	if !tekton.IsIntegrationPipelineRun(a.pipelineRun) {
		return results.ContinueProcessing()
	}

	pipelineType, err := tekton.GetTypeFromPipelineRun(a.pipelineRun)
	if err != nil {
		return results.RequeueWithError(err)
	}
	existingApplicationSnapshot, err := a.getApplicationSnapshotFromPipelineRun(a.pipelineRun, pipelineType)
	if err != nil {
		return results.RequeueWithError(err)
	}
	if existingApplicationSnapshot != nil {
		a.logger.Info("Found existing ApplicationSnapshot",
			"Application.Name", a.application.Name,
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name,
			"ApplicationSnapshot.Spec.Components", existingApplicationSnapshot.Spec.Components)
	}

	// Get all integrationTestScenarios for the Application and then find the latest Succeeded Integration PipelineRuns
	// for the ApplicationSnapshot
	integrationTestScenarios, err := a.getRequiredIntegrationTestScenariosForApplication(a.application)
	if err != nil {
		return results.RequeueWithError(err)
	}
	integrationPipelineRuns, err := a.getAllPipelineRunsForApplicationSnapshot(existingApplicationSnapshot, integrationTestScenarios)
	if err != nil {
		a.logger.Error(err, "Failed to get Integration PipelineRuns",
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name)
		return results.RequeueOnErrorOrStop(a.updateStatus())
	}

	// Skip doing anything if not all Integration PipelineRuns were found for all integrationTestScenarios
	if len(*integrationTestScenarios) != len(*integrationPipelineRuns) {
		a.logger.Info("Not all required Integration PipelineRuns finished",
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name,
			"ApplicationSnapshot.Spec.Components", existingApplicationSnapshot.Spec.Components)
		return results.ContinueProcessing()
	}

	// Go into the individual PipelineRun task results for each Integration PipelineRun
	// and determine if all of them passed (or were skipped)
	allIntegrationPipelineRunsPassed, err := a.determineIfAllIntegrationPipelinesPassed(integrationPipelineRuns)
	if err != nil {
		a.logger.Error(err, "Failed to determine outcomes for Integration PipelineRuns",
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name)
		return results.RequeueOnErrorOrStop(a.updateStatus())
	}
	if allIntegrationPipelineRunsPassed {
		existingApplicationSnapshot, err = a.markSnapshotAsPassed(existingApplicationSnapshot, "All Integration Pipeline tests passed")
		if err != nil {
			a.logger.Error(err, "Failed to Update ApplicationSnapshot HACBSTestSucceeded status")
			return results.RequeueOnErrorOrStop(a.updateStatus())
		}
		a.logger.Info("All Integration PipelineRuns succeeded, marking ApplicationSnapshot as succeeded",
			"Application.Name", a.application.Name,
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name,
			"ApplicationSnapshot Stage", existingApplicationSnapshot.Labels[""])
	} else {
		existingApplicationSnapshot, err = a.markSnapshotAsFailed(existingApplicationSnapshot, "Some Integration pipeline tests failed")
		if err != nil {
			a.logger.Error(err, "Failed to Update ApplicationSnapshot HACBSTestSucceeded status")
			return results.RequeueOnErrorOrStop(a.updateStatus())
		}
		a.logger.Info("Some tests within Integration PipelineRuns failed, marking ApplicationSnapshot as failed",
			"Application.Name", a.application.Name,
			"ApplicationSnapshot.Name", existingApplicationSnapshot.Name)
	}

	return results.ContinueProcessing()
}

// getComponentFromPipelineRun loads from the cluster the Component referenced in the given PipelineRun. If the PipelineRun doesn't
// specify a Component or this is not found in the cluster, an error will be returned.
func (a *Adapter) getApplicationSnapshotFromPipelineRun(pipelineRun *tektonv1beta1.PipelineRun, pipelineType string) (*appstudioshared.ApplicationSnapshot, error) {
	snapshotLabel := fmt.Sprintf("%s.appstudio.openshift.io/snapshot", pipelineType)
	if snapshotName, found := pipelineRun.Labels[snapshotLabel]; found {
		snapshot := &appstudioshared.ApplicationSnapshot{}
		err := a.client.Get(a.context, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      snapshotName,
		}, snapshot)

		if err != nil {
			return nil, err
		}

		return snapshot, nil
	}

	return nil, fmt.Errorf("the pipeline has no snapshot associated with it")
}

// getApplicationSnapshot returns the all ApplicationSnapshots in the Application's namespace nil if it's not found.
// In the case the List operation fails, an error will be returned.
func (a *Adapter) getAllApplicationSnapshots() (*[]appstudioshared.ApplicationSnapshot, error) {
	applicationSnapshots := &appstudioshared.ApplicationSnapshotList{}
	opts := []client.ListOption{
		client.InNamespace(a.application.Namespace),
		client.MatchingFields{"spec.application": a.application.Name},
	}

	err := a.client.List(a.context, applicationSnapshots, opts...)
	if err != nil {
		return nil, err
	}

	return &applicationSnapshots.Items, nil
}

// compareApplicationSnapshots compares two ApplicationSnapshots and returns boolean true if their images match exactly.
func (a *Adapter) compareApplicationSnapshots(expectedApplicationSnapshot *appstudioshared.ApplicationSnapshot, foundApplicationSnapshot *appstudioshared.ApplicationSnapshot) bool {
	snapshotsHaveSameNumberOfImages :=
		len(expectedApplicationSnapshot.Spec.Components) == len(foundApplicationSnapshot.Spec.Components)
	allImagesMatch := true
	for _, component1 := range expectedApplicationSnapshot.Spec.Components {
		foundImage := false
		for _, component2 := range foundApplicationSnapshot.Spec.Components {
			if component2 == component1 {
				foundImage = true
			}
		}
		if !foundImage {
			allImagesMatch = false
		}

	}
	return snapshotsHaveSameNumberOfImages && allImagesMatch
}

// findMatchingApplicationSnapshot tries to find the expected ApplicationSnapshot with the same set of images.
func (a *Adapter) findMatchingApplicationSnapshot() (*appstudioshared.ApplicationSnapshot, error) {
	var allApplicationSnapshots *[]appstudioshared.ApplicationSnapshot
	expectedApplicationSnapshot, err := a.prepareApplicationSnapshotForPipelineRun(a.pipelineRun, a.component, a.application)
	if err != nil {
		return nil, err
	}
	allApplicationSnapshots, err = a.getAllApplicationSnapshots()
	if err != nil {
		return nil, err
	}
	for _, foundApplicationSnapshot := range *allApplicationSnapshots {
		foundApplicationSnapshot := foundApplicationSnapshot
		if a.compareApplicationSnapshots(expectedApplicationSnapshot, &foundApplicationSnapshot) {
			return &foundApplicationSnapshot, nil
		}
	}
	return nil, nil
}

// getAllApplicationComponents loads from the cluster all Components associated with the given Application.
// If the Application doesn't have any Components or this is not found in the cluster, an error will be returned.
func (a *Adapter) getAllApplicationComponents(application *hasv1alpha1.Application) (*[]hasv1alpha1.Component, error) {
	applicationComponents := &hasv1alpha1.ComponentList{}
	opts := []client.ListOption{
		client.InNamespace(application.Namespace),
		client.MatchingFields{"spec.application": application.Name},
	}

	err := a.client.List(a.context, applicationComponents, opts...)
	if err != nil {
		return nil, err
	}

	return &applicationComponents.Items, nil
}

// getImagePullSpecFromPipelineRun gets the full image pullspec from the given build PipelineRun,
// In case the Image pullspec can't be can't be composed, an error will be returned.
func (a *Adapter) getImagePullSpecFromPipelineRun(pipelineRun *tektonv1beta1.PipelineRun) (string, error) {
	var err error
	outputImage, err := tekton.GetOutputImage(pipelineRun)
	if err != nil {
		return "", err
	}
	imageDigest, err := tekton.GetOutputImageDigest(pipelineRun)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s@%s", strings.Split(outputImage, ":")[0], imageDigest), nil
}

// determineIfAllIntegrationPipelinesFinished checks all Integration pipelines passed all of their test tasks.
// Returns an error if it can't get the PipelineRun outcomes
func (a *Adapter) determineIfAllIntegrationPipelinesPassed(integrationPipelineRuns *[]tektonv1beta1.PipelineRun) (bool, error) {
	allIntegrationPipelineRunsPassed := true
	for _, integrationPipelineRun := range *integrationPipelineRuns {
		integrationPipelineRun := integrationPipelineRun
		pipelineRunOutcome, err := a.calculateIntegrationPipelineRunOutcome(&integrationPipelineRun)
		if err != nil {
			a.logger.Error(err, "Failed to get Integration PipelineRun outcome",
				"PipelineRun.Name", integrationPipelineRun.Name, "PipelineRun.Namespace", integrationPipelineRun.Namespace)
			return false, err
		}
		if !pipelineRunOutcome {
			a.logger.Info("Integration PipelineRun did not pass all tests",
				"PipelineRun.Name", integrationPipelineRun.Name, "PipelineRun.Namespace", integrationPipelineRun.Namespace)
			allIntegrationPipelineRunsPassed = false
		}
	}
	return allIntegrationPipelineRunsPassed, nil
}

// getAllPipelineRunsForApplicationSnapshot loads from the cluster all Integration PipelineRuns for each IntegrationTestScenario
// associated with the ApplicationSnapshot. If the Application doesn't have any IntegrationTestScenarios associated with it,
// an error will be returned.
func (a *Adapter) getAllPipelineRunsForApplicationSnapshot(applicationSnapshot *appstudioshared.ApplicationSnapshot, integrationTestScenarios *[]v1alpha1.IntegrationTestScenario) (*[]tektonv1beta1.PipelineRun, error) {
	var integrationPipelineRuns []tektonv1beta1.PipelineRun
	for _, integrationTestScenario := range *integrationTestScenarios {
		integrationTestScenario := integrationTestScenario
		if a.pipelineRun.Labels["test.appstudio.openshift.io/scenario"] != integrationTestScenario.Name {
			integrationPipelineRun, err := a.getLatestPipelineRunForApplicationSnapshotAndScenario(applicationSnapshot, &integrationTestScenario)
			if err != nil {
				return nil, err
			}
			if integrationPipelineRun != nil {
				a.logger.Info("Found existing integrationPipelineRun",
					"IntegrationTestScenario.Name", integrationTestScenario.Name,
					"integrationPipelineRun.Name", integrationPipelineRun.Name)
				integrationPipelineRuns = append(integrationPipelineRuns, *integrationPipelineRun)
			}
		} else {
			integrationPipelineRuns = append(integrationPipelineRuns, *a.pipelineRun)
			a.logger.Info("The current integrationPipelineRun matches the integration test scenario",
				"IntegrationTestScenario.Name", integrationTestScenario.Name,
				"integrationPipelineRun.Name", a.pipelineRun.Name)
		}
	}

	return &integrationPipelineRuns, nil
}

// getLatestPipelineRunForApplicationSnapshotAndScenario returns the latest Integration PipelineRun for the
// associated ApplicationSnapshot and IntegrationTestScenario. In the case the List operation fails,
// an error will be returned.
func (a *Adapter) getLatestPipelineRunForApplicationSnapshotAndScenario(applicationSnapshot *appstudioshared.ApplicationSnapshot, integrationTestScenario *v1alpha1.IntegrationTestScenario) (*tektonv1beta1.PipelineRun, error) {
	integrationPipelineRuns := &tektonv1beta1.PipelineRunList{}
	var latestIntegrationPipelineRun = &tektonv1beta1.PipelineRun{}
	opts := []client.ListOption{
		client.InNamespace(a.application.Namespace),
		client.MatchingLabels{
			"pipelines.appstudio.openshift.io/type": "test",
			"test.appstudio.openshift.io/snapshot":  applicationSnapshot.Name,
			"test.appstudio.openshift.io/scenario":  integrationTestScenario.Name,
		},
	}

	err := a.client.List(a.context, integrationPipelineRuns, opts...)
	if err != nil {
		return nil, err
	}

	latestIntegrationPipelineRun = nil
	for _, pipelineRun := range integrationPipelineRuns.Items {
		pipelineRun := pipelineRun
		if pipelineRun.Status.GetCondition(apis.ConditionSucceeded).IsTrue() {
			if latestIntegrationPipelineRun == nil {
				latestIntegrationPipelineRun = &pipelineRun
			} else {
				if pipelineRun.Status.CompletionTime.Time.After(latestIntegrationPipelineRun.Status.CompletionTime.Time) {
					latestIntegrationPipelineRun = &pipelineRun
				}
			}
		}
	}
	if latestIntegrationPipelineRun != nil {
		return latestIntegrationPipelineRun, nil
	}

	return nil, err
}

// prepareApplicationSnapshotForPipelineRun prepares the ApplicationSnapshot for a given PipelineRun,
// component and application. In case the ApplicationSnapshot can't be created, an error will be returned.
func (a *Adapter) prepareApplicationSnapshotForPipelineRun(pipelineRun *tektonv1beta1.PipelineRun,
	component *hasv1alpha1.Component, application *hasv1alpha1.Application) (*appstudioshared.ApplicationSnapshot, error) {
	applicationComponents, err := a.getAllApplicationComponents(application)
	if err != nil {
		return nil, fmt.Errorf("failed to get all Application Components for Application %s", a.application.Name)
	}
	var components []appstudioshared.ApplicationSnapshotComponent

	for _, applicationComponent := range *applicationComponents {
		pullSpec := applicationComponent.Status.ContainerImage
		if applicationComponent.Name == component.Name {
			pullSpec, err = a.getImagePullSpecFromPipelineRun(pipelineRun)
			if err != nil {
				return nil, err
			}
		}
		components = append(components, appstudioshared.ApplicationSnapshotComponent{
			Name:           applicationComponent.Name,
			ContainerImage: pullSpec,
		})
	}

	applicationSnapshot := &appstudioshared.ApplicationSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: application.Name + "-",
			Namespace:    application.Namespace,
		},
		Spec: appstudioshared.ApplicationSnapshotSpec{
			Application: application.Name,
			Components:  components,
		},
	}
	if applicationSnapshot.Labels == nil {
		applicationSnapshot.Labels = map[string]string{
			"component": a.component.Name,
		}
	} else {
		applicationSnapshot.Labels["component"] = a.component.Name
	}

	return applicationSnapshot, nil
}

// createApplicationSnapshotForPipelineRun creates the ApplicationSnapshot for a given PipelineRun
// In case the ApplicationSnapshot can't be created, an error will be returned.
func (a *Adapter) createApplicationSnapshotForPipelineRun(pipelineRun *tektonv1beta1.PipelineRun,
	component *hasv1alpha1.Component, application *hasv1alpha1.Application) (*appstudioshared.ApplicationSnapshot, error) {
	applicationSnapshot, err := a.prepareApplicationSnapshotForPipelineRun(pipelineRun, component, application)
	if err != nil {
		return nil, err
	}
	err = a.client.Create(a.context, applicationSnapshot)
	if err != nil {
		return nil, err
	}

	return applicationSnapshot, nil
}

// calculateIntegrationPipelineRunOutcome checks the tekton results for a given PipelineRun and calculates the overall outcome.
// If any of the tasks with the HACBS_TEST_OUTPUT result don't have the `result` field set to SUCCESS, it returns false
func (a *Adapter) calculateIntegrationPipelineRunOutcome(pipelineRun *tektonv1beta1.PipelineRun) (bool, error) {
	for _, taskRun := range pipelineRun.Status.TaskRuns {
		for _, taskRunResult := range taskRun.Status.TaskRunResults {
			if taskRunResult.Name == "HACBS_TEST_OUTPUT" {
				var testOutput map[string]interface{}
				err := json.Unmarshal([]byte(taskRunResult.Value), &testOutput)
				if err != nil {
					return false, err
				}
				a.logger.Info("Found a task with HACBS_TEST_OUTPUT",
					"taskRun.Name", taskRun.PipelineTaskName,
					"taskRun Result", testOutput["result"])
				if testOutput["result"] != "SUCCESS" && testOutput["result"] != "SKIPPED" {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

// markSnapshotAsPassed updates the result label for the ApplicationSnapshot
// If the update command fails, an error will be returned
func (a *Adapter) markSnapshotAsPassed(applicationSnapshot *appstudioshared.ApplicationSnapshot, message string) (*appstudioshared.ApplicationSnapshot, error) {
	patch := client.MergeFrom(applicationSnapshot.DeepCopy())
	meta.SetStatusCondition(&applicationSnapshot.Status.Conditions, metav1.Condition{
		Type:    "HACBSTestSucceeded",
		Status:  metav1.ConditionTrue,
		Reason:  "Passed",
		Message: message,
	})
	err := a.client.Status().Patch(a.context, applicationSnapshot, patch)
	//err := a.client.Status().Update(a.context, applicationSnapshot)
	if err != nil {
		return nil, err
	}
	return applicationSnapshot, nil
}

// markSnapshotAsFailed updates the result label for the ApplicationSnapshot
// If the update command fails, an error will be returned
func (a *Adapter) markSnapshotAsFailed(applicationSnapshot *appstudioshared.ApplicationSnapshot, message string) (*appstudioshared.ApplicationSnapshot, error) {
	patch := client.MergeFrom(applicationSnapshot.DeepCopy())
	meta.SetStatusCondition(&applicationSnapshot.Status.Conditions, metav1.Condition{
		Type:    "HACBSTestSucceeded",
		Status:  metav1.ConditionFalse,
		Reason:  "Failed",
		Message: message,
	})
	err := a.client.Status().Patch(a.context, applicationSnapshot, patch)
	if err != nil {
		return nil, err
	}
	return applicationSnapshot, nil
}

// getRequiredIntegrationTestScenariosForApplication returns the IntegrationTestScenarios used by the application being processed.
// A IntegrationTestScenarios will only be returned if it has the
// release.appstudio.openshift.io/optional label set to true or if it is missing the label entirely.
func (a *Adapter) getRequiredIntegrationTestScenariosForApplication(application *hasv1alpha1.Application) (*[]v1alpha1.IntegrationTestScenario, error) {
	labelSelector := labels.NewSelector()
	integrationList := &v1alpha1.IntegrationTestScenarioList{}
	labelRequirement, err := labels.NewRequirement("test.appstudio.openshift.io/optional", selection.NotIn, []string{"false"})
	if err != nil {
		return nil, err
	}
	labelSelector = labelSelector.Add(*labelRequirement)

	opts := &client.ListOptions{
		Namespace:     application.Namespace,
		FieldSelector: fields.OneTermEqualSelector("spec.application", application.Name),
		LabelSelector: labelSelector,
	}

	err = a.client.List(a.context, integrationList, opts)
	if err != nil {
		return nil, err
	}

	return &integrationList.Items, nil
}

// updateStatus updates the status of the PipelineRun being processed.
func (a *Adapter) updateStatus() error {
	return a.client.Status().Update(a.context, a.pipelineRun)
}
