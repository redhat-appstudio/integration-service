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

package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/go-logr/logr"
	"github.com/santhosh-tekuri/jsonschema/v5"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
)

const (

	//TestOutputName is the name of the standardized Test output Tekton task result
	TestOutputName = "TEST_OUTPUT"

	//LegacyTestOutputName is the previous name of the standardized AppStudio Test output Tekton task result
	LegacyTestOutputName = "HACBS_TEST_OUTPUT"

	// AppStudioTestOutputSuccess is the result that's set when the AppStudio test succeeds.
	AppStudioTestOutputSuccess = "SUCCESS"

	// AppStudioTestOutputFailure is the result that's set when the AppStudio test fails.
	AppStudioTestOutputFailure = "FAILURE"

	// AppStudioTestOutputWarning is the result that's set when the AppStudio test passes with a warning.
	AppStudioTestOutputWarning = "WARNING"

	// AppStudioTestOutputSkipped is the result that's set when the AppStudio test gets skipped.
	AppStudioTestOutputSkipped = "SKIPPED"

	// AppStudioTestOutputError is the result that's set when the AppStudio test produces an error.
	AppStudioTestOutputError = "ERROR"
)

// AppStudioTestResult matches AppStudio TaskRun result contract
type AppStudioTestResult struct {
	Result    string `json:"result"`
	Namespace string `json:"namespace"`
	Timestamp string `json:"timestamp"`
	Note      string `json:"note"`
	Successes int    `json:"successes"`
	Failures  int    `json:"failures"`
	Warnings  int    `json:"warnings"`
}

// IntegrationTestTaskResult provides results from integration test task
// including metadata about validity of results
type IntegrationTestTaskResult struct {
	TestOutput      *AppStudioTestResult
	ValidationError error
}

var testResultSchema = `{
  "$schema": "http://json-schema.org/draft/2020-12/schema#",
  "type": "object",
  "properties": {
    "result": {
      "type": "string",
      "enum": ["SUCCESS", "FAILURE", "WARNING", "SKIPPED", "ERROR"]
    },
    "namespace": {
      "type": "string"
    },
    "timestamp": {
      "type": "string",
      "pattern": "^[0-9]{10}$"
    },
    "successes": {
      "type": "integer",
      "minimum": 0
    },
    "note": {
      "type": "string"
    },
    "failures": {
      "type": "integer",
      "minimum": 0
    },
    "warnings": {
      "type": "integer",
      "minimum": 0
    }
  },
  "required": ["result", "timestamp", "successes", "failures", "warnings"]
}`

// TaskRun is an integration specific wrapper around the status of a Tekton TaskRun.
type TaskRun struct {
	pipelineTaskName string
	trStatus         *tektonv1.TaskRunStatus
	testResult       *IntegrationTestTaskResult
}

// NewTaskRunFromTektonTaskRun creates and returns am integration TaskRun from the TaskRunStatus.
func NewTaskRunFromTektonTaskRun(pipelineTaskName string, status *tektonv1.TaskRunStatus) *TaskRun {
	return &TaskRun{pipelineTaskName: pipelineTaskName, trStatus: status}
}

// GetPipelineTaskName returns the name of the PipelineTask.
func (t *TaskRun) GetPipelineTaskName() string {
	return t.pipelineTaskName
}

// GetStartTime returns the start time of the TaskRun.
// If the start time is unknown, the zero start time is returned.
func (t *TaskRun) GetStartTime() time.Time {
	if t.trStatus.StartTime == nil {
		return time.Time{}
	}
	return t.trStatus.StartTime.Time
}

// GetDuration returns the time it took to execute the Task.
// If the start or end times are unknown, a duration of 0 is returned.
func (t *TaskRun) GetDuration() time.Duration {
	var end time.Time
	start := t.GetStartTime()
	if t.trStatus.CompletionTime != nil {
		end = t.trStatus.CompletionTime.Time
	} else {
		end = start
	}
	return end.Sub(start)
}

// GetTestResult returns a IntegrationTestTaskResult if the TaskRun produced the result. It will return nil otherwise.
func (t *TaskRun) GetTestResult() (*IntegrationTestTaskResult, error) {
	// Check for an already parsed result.
	if t.testResult != nil {
		return t.testResult, nil
	}
	// load schema for test validation
	sch, err := jsonschema.CompileString("schema.json", testResultSchema)
	if err != nil {
		return nil, fmt.Errorf("error while compiling json data for schema validation: %w", err)
	}

	for _, taskRunResult := range t.trStatus.TaskRunStatusFields.Results {
		if taskRunResult.Name == LegacyTestOutputName || taskRunResult.Name == TestOutputName {
			var testOutput AppStudioTestResult
			var testResult IntegrationTestTaskResult = IntegrationTestTaskResult{}
			var v interface{}

			if err := json.Unmarshal([]byte(taskRunResult.Value.StringVal), &testOutput); err != nil {
				testResult.ValidationError = fmt.Errorf("error while mapping json data from task %s result %s to AppStudioTestResult: %w", t.GetPipelineTaskName(), taskRunResult.Name, err)
			} else if err := json.Unmarshal([]byte(taskRunResult.Value.StringVal), &v); err != nil {
				testResult.ValidationError = fmt.Errorf("error while mapping json data from task %s result %s: %w", t.GetPipelineTaskName(), taskRunResult.Name, err)
			} else if err = sch.Validate(v); err != nil {
				testResult.ValidationError = fmt.Errorf("error validating schema of results from task %s result %s: %w", t.GetPipelineTaskName(), taskRunResult.Name, err)
			} else {
				testResult.TestOutput = &testOutput
			}
			t.testResult = &testResult
			return &testResult, nil
		}
	}
	return nil, nil
}

// SortTaskRunsByStartTime can sort TaskRuns by their start time. It implements sort.Interface.
type SortTaskRunsByStartTime []*TaskRun

// Len returns the length of the slice being sorted.
func (s SortTaskRunsByStartTime) Len() int {
	return len(s)
}

// Swap switches the position of two elements in the slice.
func (s SortTaskRunsByStartTime) Swap(i int, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less determines if TaskRun in position i started before TaskRun in position j.
func (s SortTaskRunsByStartTime) Less(i int, j int) bool {
	return s[i].GetStartTime().Before(s[j].GetStartTime())
}

// IntegrationPipelineRunOutcome is struct for pipeline outcome metadata
type IntegrationPipelineRunOutcome struct {
	pipelineRunSucceeded bool
	pipelineRun          *tektonv1.PipelineRun
	// map: task name to results
	results map[string]*IntegrationTestTaskResult
}

// HasPipelineRunSucceeded returns true when pipeline in outcome succeeded
func (ipro *IntegrationPipelineRunOutcome) HasPipelineRunSucceeded() bool {
	return ipro.pipelineRunSucceeded
}

// HasPipelineRunValidTestOutputs returns false when we failed to parse results of TEST_OUTPUT in tasks
func (ipro *IntegrationPipelineRunOutcome) HasPipelineRunValidTestOutputs() bool {
	for _, result := range ipro.results {
		if result.ValidationError != nil {
			return false
		}
	}
	return true
}

// GetValidationErrorsList returns validation error messages for each invalid task result in a list.
func (ipro *IntegrationPipelineRunOutcome) GetValidationErrorsList() []string {
	var errors []string
	for _, result := range ipro.results {
		if result.ValidationError != nil {
			errors = append(errors, fmt.Sprintf("Invalid result: %s", result.ValidationError))
		}
	}
	return errors
}

// HasPipelineRunPassedTesting returns general outcome
// If any of the tasks with the TEST_OUTPUT result don't have the `result` field set to SUCCESS or SKIPPED, it returns false.
func (ipro *IntegrationPipelineRunOutcome) HasPipelineRunPassedTesting() bool {
	if !ipro.HasPipelineRunSucceeded() {
		return false
	}
	if !ipro.HasPipelineRunValidTestOutputs() {
		return false
	}
	for _, result := range ipro.results {
		if result.TestOutput.Result != AppStudioTestOutputSuccess &&
			result.TestOutput.Result != AppStudioTestOutputSkipped &&
			result.TestOutput.Result != AppStudioTestOutputWarning {
			return false
		}
	}
	return true
}

// LogResults writes tasks names with results into given logger, each task on separate line
func (ipro *IntegrationPipelineRunOutcome) LogResults(logger logr.Logger) {
	for k, v := range ipro.results {
		if v.TestOutput != nil {
			logger.Info(fmt.Sprintf("Found task results for pipeline run %s", ipro.pipelineRun.Name),
				"pipelineRun.Name", ipro.pipelineRun.Name,
				"pipelineRun.Namespace", ipro.pipelineRun.Namespace,
				"task.Name", k, "task.TestOutput", v.TestOutput)
		} else if v.ValidationError != nil {
			logger.Info(fmt.Sprintf("Invalid task results for pipeline run %s", ipro.pipelineRun.Name),
				"pipelineRun.Name", ipro.pipelineRun.Name,
				"pipelineRun.Namespace", ipro.pipelineRun.Namespace,
				"task.Name", k, "task.ValidationError", v.ValidationError.Error())
		}
	}
}

// GetIntegrationPipelineRunOutcome returns the IntegrationPipelineRunOutcome
// which can be used for further inspection of the results and general outcome
// This function must be called on the finished pipeline
func GetIntegrationPipelineRunOutcome(adapterClient client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*IntegrationPipelineRunOutcome, error) {

	// Check if the pipelineRun failed from the conditions of status
	if !HasPipelineRunSucceeded(pipelineRun) {
		return &IntegrationPipelineRunOutcome{
			pipelineRunSucceeded: false,
			pipelineRun:          pipelineRun,
			results:              map[string]*IntegrationTestTaskResult{},
		}, nil
	}
	// Check if the pipelineRun.Status contains the childReferences to TaskRuns
	if !reflect.ValueOf(pipelineRun.Status.ChildReferences).IsZero() {
		// If the pipelineRun.Status contains the childReferences, parse them in the new way by querying for TaskRuns
		results, err := GetIntegrationTestTaskResultsFromPipelineRunWithChildReferences(adapterClient, ctx, pipelineRun)
		if err != nil {
			return nil, fmt.Errorf("error while getting test results from pipelineRun %s: %w", pipelineRun.Name, err)
		}
		return &IntegrationPipelineRunOutcome{
			pipelineRunSucceeded: true,
			pipelineRun:          pipelineRun,
			results:              results,
		}, nil
	}

	// PLR passed but no results were found
	return &IntegrationPipelineRunOutcome{
		pipelineRunSucceeded: true,
		pipelineRun:          pipelineRun,
		results:              map[string]*IntegrationTestTaskResult{},
	}, nil
}

// GetIntegrationTestTaskResultsFromPipelineRunWithChildReferences finds all TaskRuns from childReferences of the PipelineRun
// that also contain a TEST_OUTPUT result and returns the parsed data or validation error
// returns map taskName: result
func GetIntegrationTestTaskResultsFromPipelineRunWithChildReferences(adapterClient client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (map[string]*IntegrationTestTaskResult, error) {
	taskRuns, err := GetAllChildTaskRunsForPipelineRun(adapterClient, ctx, pipelineRun)
	if err != nil {
		return nil, err
	}

	results := map[string]*IntegrationTestTaskResult{}
	for _, tr := range taskRuns {
		r, err := tr.GetTestResult()
		if err != nil {
			return nil, err
		}
		if r != nil {
			results[tr.GetPipelineTaskName()] = r
		}
	}
	return results, nil
}

// GetAllChildTaskRunsForPipelineRun finds all Child TaskRuns for a given PipelineRun and
// returns integration TaskRun wrappers for them sorted by start time.
func GetAllChildTaskRunsForPipelineRun(adapterClient client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) ([]*TaskRun, error) {
	taskRuns := []*TaskRun{}
	// If there are no childReferences, skip trying to get tasks
	if reflect.ValueOf(pipelineRun.Status.ChildReferences).IsZero() {
		return nil, nil
	}
	for _, childReference := range pipelineRun.Status.ChildReferences {
		pipelineTaskRun := &tektonv1.TaskRun{}
		err := adapterClient.Get(ctx, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      childReference.Name,
		}, pipelineTaskRun)
		if err != nil {
			return nil, fmt.Errorf("error while getting the child taskRun %s from pipelineRun: %w", childReference.Name, err)
		}

		integrationTaskRun := NewTaskRunFromTektonTaskRun(childReference.PipelineTaskName, &pipelineTaskRun.Status)
		taskRuns = append(taskRuns, integrationTaskRun)
	}
	sort.Sort(SortTaskRunsByStartTime(taskRuns))
	return taskRuns, nil
}

// HasPipelineRunSucceeded returns a boolean indicating whether the PipelineRun succeeded or not.
// If the object passed to this function is not a PipelineRun, the function will return false.
func HasPipelineRunSucceeded(object client.Object) bool {
	if pr, ok := object.(*tektonv1.PipelineRun); ok {
		return pr.Status.GetCondition(apis.ConditionSucceeded).IsTrue()
	}

	return false
}

// HasPipelineRunFinished returns a boolean indicating whether the PipelineRun finished or not.
// If the object passed to this function is not a PipelineRun, the function will return false.
func HasPipelineRunFinished(object client.Object) bool {
	if pr, ok := object.(*tektonv1.PipelineRun); ok {
		return !pr.Status.GetCondition(apis.ConditionSucceeded).IsUnknown()
	}

	return false
}

func IsEnvironmentEphemeral(testEnvironment *applicationapiv1alpha1.Environment) bool {
	isEphemeral := false
	for _, tag := range testEnvironment.Spec.Tags {
		if tag == "ephemeral" {
			isEphemeral = true
			break
		}
	}
	return isEphemeral
}

func CleanUpEphemeralEnvironments(client client.Client, logger *IntegrationLogger, ctx context.Context, env *applicationapiv1alpha1.Environment, dtc *applicationapiv1alpha1.DeploymentTargetClaim) error {
	logger.Info("Deleting deploymentTargetClaim", "deploymentTargetClaim.Name", dtc.Name)
	err := client.Delete(ctx, dtc)
	if err != nil {
		logger.Error(err, "Failed to delete the deploymentTargetClaim")
		return err
	}
	logger.LogAuditEvent("DeploymentTargetClaim deleted", dtc, LogActionDelete)

	logger.Info("Deleting environment", "environment.Name", env.Name)
	err = client.Delete(ctx, env)
	if err != nil {
		logger.Error(err, "Failed to delete the test ephemeral environment and its owning snapshotEnvironmentBinding", "environment.Name", env.Name)
		return err
	}
	logger.LogAuditEvent("Ephemeral environment is deleted and its owning SnapshotEnvironmentBinding is in the process of being deleted", env, LogActionDelete)
	return nil
}

// RemoveFinalizerFromAllIntegrationPipelineRunsOfSnapshot fetches all the Integration
// PipelineRuns associated with the given Snapshot. After fetching them, it removes the
// finalizer from the PipelineRun, and returns error if any.
func RemoveFinalizerFromAllIntegrationPipelineRunsOfSnapshot(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, snapshot applicationapiv1alpha1.Snapshot, finalizer string) error {
	integrationPipelineRuns := &tektonv1.PipelineRunList{}
	opts := []client.ListOption{
		client.InNamespace(snapshot.Namespace),
		client.MatchingLabels{
			"appstudio.openshift.io/snapshot": snapshot.Name,
		},
	}

	err := adapterClient.List(ctx, integrationPipelineRuns, opts...)
	if err != nil {
		return err
	}

	// Remove finalizer from each of the PipelineRuns
	for _, pipelineRun := range integrationPipelineRuns.Items {
		pipelineRun := pipelineRun
		err = RemoveFinalizerFromPipelineRun(adapterClient, logger, ctx, &pipelineRun, finalizer)
		if err != nil {
			return err
		}
	}

	return nil
}

// RemoveFinalizerFromPipelineRun removes the finalizer from the PipelineRun.
// If finalizer was not removed successfully, a non-nil error is returned.
func RemoveFinalizerFromPipelineRun(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, pipelineRun *tektonv1.PipelineRun, finalizer string) error {
	if !controllerutil.ContainsFinalizer(pipelineRun, finalizer) {
		return nil
	}

	patch := client.MergeFrom(pipelineRun.DeepCopy())
	if ok := controllerutil.RemoveFinalizer(pipelineRun, finalizer); ok {
		err := adapterClient.Patch(ctx, pipelineRun, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated PipelineRun after finalizer removal: %w", err)
		}

		logger.LogAuditEvent("Removed Finalizer from the PipelineRun", pipelineRun, LogActionUpdate, "finalizer", finalizer)
	}

	return nil
}

// AddFinalizerToPipelineRun adds the finalizer to the PipelineRun.
// If finalizer was not added successfully, a non-nil error is returned.
func AddFinalizerToPipelineRun(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, pipelineRun *tektonv1.PipelineRun, finalizer string) error {
	patch := client.MergeFrom(pipelineRun.DeepCopy())
	if ok := controllerutil.AddFinalizer(pipelineRun, finalizer); ok {
		err := adapterClient.Patch(ctx, pipelineRun, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated PipelineRun after finalizer addition: %w", err)
		}

		logger.LogAuditEvent("Added Finalizer to the PipelineRun", pipelineRun, LogActionUpdate, "finalizer", finalizer)
	}
	return nil
}

// AddFinalizerToComponent adds the finalizer to the component.
// If finalizer was not added successfully, a non-nil error is returned.
func AddFinalizerToComponent(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, component *applicationapiv1alpha1.Component, finalizer string) error {
	patch := client.MergeFrom(component.DeepCopy())
	if ok := controllerutil.AddFinalizer(component, finalizer); ok {
		err := adapterClient.Patch(ctx, component, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated component after finalizer addition: %w", err)
		}

		logger.LogAuditEvent("Added Finalizer to the Component", component, LogActionUpdate, "finalizer", finalizer)
	}

	return nil
}

// RemoveFinalizerFromComponent removes the finalizer from the Component.
// If finalizer was not removed successfully, a non-nil error is returned.
func RemoveFinalizerFromComponent(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, component *applicationapiv1alpha1.Component, finalizer string) error {
	patch := client.MergeFrom(component.DeepCopy())
	if ok := controllerutil.RemoveFinalizer(component, finalizer); ok {
		err := adapterClient.Patch(ctx, component, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated Component after finalizer removal: %w", err)
		}

		logger.LogAuditEvent("Removed Finalizer from the Component", component, LogActionDelete, "finalizer", finalizer)
	}

	return nil
}

// RemoveFinalizerFromScenario removes the finalizer from the IntegrationTestScenario.
// If finalizer was not removed successfully, a non-nil error is returned.
func RemoveFinalizerFromScenario(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, scenario *v1beta1.IntegrationTestScenario, finalizer string) error {
	patch := client.MergeFrom(scenario.DeepCopy())
	if ok := controllerutil.RemoveFinalizer(scenario, finalizer); ok {
		err := adapterClient.Patch(ctx, scenario, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated IntegrationTestScenario after finalizer removal: %w", err)
		}

		logger.LogAuditEvent("Removed Finalizer from the IntegrationTestScenario", scenario, LogActionUpdate, "finalizer", finalizer)
	}

	return nil
}

// AddFinalizerToScenario adds the finalizer to the IntegrationTestScenario.
// If finalizer was not added successfully, a non-nil error is returned.
func AddFinalizerToScenario(adapterClient client.Client, logger IntegrationLogger, ctx context.Context, scenario *v1beta1.IntegrationTestScenario, finalizer string) error {
	patch := client.MergeFrom(scenario.DeepCopy())
	if ok := controllerutil.AddFinalizer(scenario, finalizer); ok {
		err := adapterClient.Patch(ctx, scenario, patch)
		if err != nil {
			return fmt.Errorf("error occurred while patching the updated IntegrationTestScenario after finalizer addition: %w", err)
		}

		logger.LogAuditEvent("Added Finalizer to the IntegrationTestScenario", scenario, LogActionUpdate, "finalizer", finalizer)
	}

	return nil
}

func IsObjectYoungerThanThreshold(obj metav1.Object, threshold time.Duration) bool {
	objectCreationTime := obj.GetCreationTimestamp().Time
	durationSinceObjectCreation := time.Since(objectCreationTime)

	return durationSinceObjectCreation < threshold
}
