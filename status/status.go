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

package status

//go:generate mockgen -destination mock_status.go -package status github.com/redhat-appstudio/integration-service/status StatusInterface

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/metadata"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/helpers"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NamePrefix is a common name prefix for this service.
const NamePrefix = "Red Hat Konflux"

// ScenarioReportStatus keep report status of git provider for the particular scenario
type ScenarioReportStatus struct {
	LastUpdateTime *time.Time `json:"lastUpdateTime"`
}

// SnapshotReportStatus keep report status of git provider for the snapshot
type SnapshotReportStatus struct {
	Scenarios map[string]*ScenarioReportStatus `json:"scenarios"`
	dirty     bool
}

// SetLastUpdateTime updates the last udpate time of the given scenario to the given time
func (srs *SnapshotReportStatus) SetLastUpdateTime(scenarioName string, t time.Time) {
	srs.dirty = true
	if scenario, ok := srs.Scenarios[scenarioName]; ok {
		scenario.LastUpdateTime = &t
		return
	}

	srs.Scenarios[scenarioName] = &ScenarioReportStatus{
		LastUpdateTime: &t,
	}
}

// IsNewer returns true if given scenario has newer time than the last updated
func (srs *SnapshotReportStatus) IsNewer(scenarioName string, t time.Time) bool {
	if scenario, ok := srs.Scenarios[scenarioName]; ok {
		return scenario.LastUpdateTime.Before(t)
	}

	// no record, it's new
	return true
}

// ToAnnotationString exports data in format for annotation
func (srs *SnapshotReportStatus) ToAnnotationString() (string, error) {
	byteVar, err := json.Marshal(srs)
	if err != nil {
		return "", fmt.Errorf("failed to marshal report status metadata: %w", err)
	}
	return string(byteVar), nil
}

// IsDirty returns true if there are new changes to be written
func (srs *SnapshotReportStatus) IsDirty() bool {
	return srs.dirty
}

// ResetDirty marks changes as synced to snapshot
func (srs *SnapshotReportStatus) ResetDirty() {
	srs.dirty = false
}

// NewSnapshotReportStatus creates new object
func NewSnapshotReportStatus(jsondata string) (*SnapshotReportStatus, error) {
	srs := SnapshotReportStatus{
		Scenarios: map[string]*ScenarioReportStatus{},
	}
	if jsondata != "" {
		err := json.Unmarshal([]byte(jsondata), &srs)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal json '%s': %w", jsondata, err)
		}
	}
	return &srs, nil
}

// NewSnapshotReportStatusFromSnapshot creates new SnapshotTestStatus struct from snapshot annotation
func NewSnapshotReportStatusFromSnapshot(s *applicationapiv1alpha1.Snapshot) (*SnapshotReportStatus, error) {
	var (
		statusAnnotation string
		ok               bool
	)

	if statusAnnotation, ok = s.ObjectMeta.GetAnnotations()[gitops.SnapshotStatusReportAnnotation]; !ok {
		statusAnnotation = ""
	}

	srs, err := NewSnapshotReportStatus(statusAnnotation)
	if err != nil {
		return nil, fmt.Errorf("failed to get integration tests report status metadata from snapshot %s (annotation: %s): %w", s.Name, gitops.SnapshotStatusReportAnnotation, err)
	}

	return srs, nil
}

// WriteSnapshotReportStatus writes report status
func WriteSnapshotReportStatus(ctx context.Context, c client.Client, s *applicationapiv1alpha1.Snapshot, srs *SnapshotReportStatus) error {
	if !srs.IsDirty() {
		return nil // nothing to update
	}
	patch := client.MergeFrom(s.DeepCopy())

	value, err := srs.ToAnnotationString()
	if err != nil {
		return fmt.Errorf("failed to marshal test results into JSON: %w", err)
	}

	if err := metadata.SetAnnotation(&s.ObjectMeta, gitops.SnapshotStatusReportAnnotation, value); err != nil {
		return fmt.Errorf("failed to add annotations: %w", err)
	}

	err = c.Patch(ctx, s, patch)
	if err != nil {
		// don't return wrapped err, so we can use RetryOnConflict
		return err
	}

	srs.ResetDirty()
	return nil
}

// MigrateSnapshotToReportStatus migrates old way of keeping updates sync to the new way by updating annotations in snapshot
func MigrateSnapshotToReportStatus(s *applicationapiv1alpha1.Snapshot, testStatuses []*intgteststat.IntegrationTestStatusDetail) {
	if s.ObjectMeta.GetAnnotations() == nil {
		return // nothing to migrate
	}
	annotations := s.ObjectMeta.GetAnnotations()
	_, ok := annotations[gitops.SnapshotPRLastUpdate]
	if !ok {
		return // nothing to migrate
	}

	oldLastUpdateTime, err := gitops.GetLatestUpdateTime(s)
	delete(annotations, gitops.SnapshotPRLastUpdate) // we don't care at this point
	if err != nil {
		return // something happen, cancel migration and start with fresh data
	}

	srs, err := NewSnapshotReportStatus("")
	if err != nil {
		return // something happen, cancel migration and start with fresh data
	}

	for _, testStatDetail := range testStatuses {
		srs.SetLastUpdateTime(testStatDetail.ScenarioName, oldLastUpdateTime)
	}

	annotations[gitops.SnapshotStatusReportAnnotation], _ = srs.ToAnnotationString()
}

type StatusInterface interface {
	GetReporter(*applicationapiv1alpha1.Snapshot) ReporterInterface
	ReportSnapshotStatus(context.Context, ReporterInterface, *applicationapiv1alpha1.Snapshot) error
}

type Status struct {
	logger logr.Logger
	client client.Client
}

// check if interface has been implemented correctly
var _ StatusInterface = (*Status)(nil)

func NewStatus(logger logr.Logger, client client.Client) *Status {
	return &Status{
		logger: logger,
		client: client,
	}
}

// GetReporter returns reporter to process snapshot using the right git provider, nil means no suitable reporter found
func (s *Status) GetReporter(snapshot *applicationapiv1alpha1.Snapshot) ReporterInterface {
	githubReporter := NewGitHubReporter(s.logger, s.client)
	if githubReporter.Detect(snapshot) {
		return githubReporter
	}

	gitlabReporter := NewGitLabReporter(s.logger, s.client)
	if gitlabReporter.Detect(snapshot) {
		return gitlabReporter
	}

	return nil
}

// ReportSnapshotStatus reports status of all integration tests into Pull Request
func (s *Status) ReportSnapshotStatus(ctx context.Context, reporter ReporterInterface, snapshot *applicationapiv1alpha1.Snapshot) error {

	statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshot)
	if err != nil {
		s.logger.Error(err, "failed to get test status annotations from snapshot",
			"snapshot.Namespace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
		return err
	}

	integrationTestStatusDetails := statuses.GetStatuses()
	if len(integrationTestStatusDetails) == 0 {
		// no tests to report, skip
		s.logger.Info("No test result to report to GitHub, skipping",
			"snapshot.Namespace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
		return nil
	}

	if err := reporter.Initialize(ctx, snapshot); err != nil {
		s.logger.Error(err, "Failed to initialize reporter", "reporter", reporter.GetReporterName())
		return fmt.Errorf("failed to initialize reporter: %w", err)
	}
	s.logger.Info("Reporter initialized", "reporter", reporter.GetReporterName())

	MigrateSnapshotToReportStatus(snapshot, integrationTestStatusDetails)

	srs, err := NewSnapshotReportStatusFromSnapshot(snapshot)
	if err != nil {
		s.logger.Error(err, "failed to get latest snapshot write metadata annotation for snapshot",
			"snapshot.NameSpace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
		srs, _ = NewSnapshotReportStatus("")
	}

	for _, integrationTestStatusDetail := range integrationTestStatusDetails {
		if srs.IsNewer(integrationTestStatusDetail.ScenarioName, integrationTestStatusDetail.LastUpdateTime) {
			s.logger.Info("Integration Test contains new status updates", "scenario.Name", integrationTestStatusDetail.ScenarioName)
		} else {
			//integration test contains no changes
			continue
		}
		testReport, err := s.generateTestReport(ctx, *integrationTestStatusDetail, snapshot)
		if err != nil {
			_ = WriteSnapshotReportStatus(ctx, s.client, snapshot, srs) // try to write what was already written
			return fmt.Errorf("failed to generate test report: %w", err)
		}
		if err := reporter.ReportStatus(ctx, *testReport); err != nil {
			_ = WriteSnapshotReportStatus(ctx, s.client, snapshot, srs) // try to write what was already written
			return fmt.Errorf("failed to update status: %w", err)
		}
		srs.SetLastUpdateTime(integrationTestStatusDetail.ScenarioName, integrationTestStatusDetail.LastUpdateTime)

	}
	if err := WriteSnapshotReportStatus(ctx, s.client, snapshot, srs); err != nil {
		return fmt.Errorf("failed to write snapshot report status metadata: %w", err)
	}

	return nil
}

// generateTestReport generates TestReport to be used by all reporters
func (s *Status) generateTestReport(ctx context.Context, detail intgteststat.IntegrationTestStatusDetail, snapshot *applicationapiv1alpha1.Snapshot) (*TestReport, error) {
	text, err := s.generateText(ctx, detail, snapshot.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to generate text message: %w", err)
	}

	summary, err := GenerateSummary(detail.Status, snapshot.Name, detail.ScenarioName)
	if err != nil {
		return nil, fmt.Errorf("failed to generate summary message: %w", err)
	}

	fullName := fmt.Sprintf("%s / %s", NamePrefix, detail.ScenarioName)
	if snapshot.Labels[gitops.SnapshotComponentLabel] != "" {
		fullName = fmt.Sprintf("%s / %s", fullName, snapshot.Labels[gitops.SnapshotComponentLabel])
	}

	report := TestReport{
		Text:           text,
		FullName:       fullName,
		ScenarioName:   detail.ScenarioName,
		SnapshotName:   snapshot.Name,
		ComponentName:  snapshot.Labels[gitops.SnapshotComponentLabel],
		Status:         detail.Status,
		Summary:        summary,
		StartTime:      detail.StartTime,
		CompletionTime: detail.CompletionTime,
	}
	return &report, nil
}

// generateText generates a text with details for the given state
func (s *Status) generateText(ctx context.Context, integrationTestStatusDetail intgteststat.IntegrationTestStatusDetail, namespace string) (string, error) {
	if integrationTestStatusDetail.Status == intgteststat.IntegrationTestStatusTestPassed || integrationTestStatusDetail.Status == intgteststat.IntegrationTestStatusTestFail {
		pipelineRunName := integrationTestStatusDetail.TestPipelineRunName
		pipelineRun := &tektonv1.PipelineRun{}
		err := s.client.Get(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      pipelineRunName,
		}, pipelineRun)

		if err != nil {
			if errors.IsNotFound(err) {
				s.logger.Error(err, "Failed to fetch pipelineRun", "pipelineRun.Name", pipelineRunName)
				text := fmt.Sprintf("%s\n\n\n(Failed to fetch test result details.)", integrationTestStatusDetail.Details)
				return text, nil
			}

			return "", fmt.Errorf("error while getting the pipelineRun %s: %w", pipelineRunName, err)
		}

		taskRuns, err := helpers.GetAllChildTaskRunsForPipelineRun(s.client, ctx, pipelineRun)
		if err != nil {
			return "", fmt.Errorf("error while getting all child taskRuns from pipelineRun %s: %w", pipelineRunName, err)
		}
		text, err := FormatTestsSummary(taskRuns, pipelineRunName, namespace, s.logger)
		if err != nil {
			return "", err
		}
		return text, nil
	} else {
		text := integrationTestStatusDetail.Details
		return text, nil
	}
}

// GenerateSummary returns summary for the given state, snapshotName and scenarioName
func GenerateSummary(state intgteststat.IntegrationTestStatus, snapshotName, scenarioName string) (string, error) {
	var summary string
	var statusDesc string

	switch state {
	case intgteststat.IntegrationTestStatusPending:
		statusDesc = "is pending"
	case intgteststat.IntegrationTestStatusInProgress:
		statusDesc = "is in progress"
	case intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated:
		statusDesc = "experienced an error when provisioning environment"
	case intgteststat.IntegrationTestStatusDeploymentError_Deprecated:
		statusDesc = "experienced an error when deploying snapshotEnvironmentBinding"
	case intgteststat.IntegrationTestStatusDeleted:
		statusDesc = "was deleted before the pipelineRun could finish"
	case intgteststat.IntegrationTestStatusTestPassed:
		statusDesc = "has passed"
	case intgteststat.IntegrationTestStatusTestFail:
		statusDesc = "has failed"
	case intgteststat.IntegrationTestStatusTestInvalid:
		statusDesc = "is invalid"
	default:
		return summary, fmt.Errorf("unknown status")
	}

	summary = fmt.Sprintf("Integration test for snapshot %s and scenario %s %s", snapshotName, scenarioName, statusDesc)

	return summary, nil
}
