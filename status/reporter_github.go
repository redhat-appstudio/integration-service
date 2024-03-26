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

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	ghapi "github.com/google/go-github/v45/github"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/git/github"
	"github.com/redhat-appstudio/integration-service/gitops"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"

	"github.com/konflux-ci/operator-toolkit/metadata"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Used by statusReport to get pipelines-as-code-secret under NS integration-service
const (
	integrationNS       = "integration-service"
	PACSecret           = "pipelines-as-code-secret"
	gitHubApplicationID = "github-application-id"
	gitHubPrivateKey    = "github-private-key"
)

// StatusUpdater is common interface used by status reporter to update PR status
type StatusUpdater interface {
	// Authentication of client
	Authenticate(ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) error
	// Update status of PR
	UpdateStatus(ctx context.Context, report TestReport) error
}

// CheckRunStatusUpdater updates PR status using CheckRuns (when application integration is enabled in repo)
type CheckRunStatusUpdater struct {
	ghClient          github.ClientInterface
	k8sClient         client.Client
	logger            *logr.Logger
	owner             string
	repo              string
	sha               string
	snapshot          *applicationapiv1alpha1.Snapshot
	creds             *appCredentials
	allCheckRunsCache []*ghapi.CheckRun
}

// NewCheckRunStatusUpdater returns a pointer to initialized CheckRunStatusUpdater
func NewCheckRunStatusUpdater(
	ghClient github.ClientInterface,
	k8sClient client.Client,
	logger *logr.Logger,
	owner string,
	repo string,
	sha string,
	snapshot *applicationapiv1alpha1.Snapshot,
) *CheckRunStatusUpdater {
	return &CheckRunStatusUpdater{
		ghClient:  ghClient,
		k8sClient: k8sClient,
		logger:    logger,
		owner:     owner,
		repo:      repo,
		sha:       sha,
		snapshot:  snapshot,
	}
}

func (cru *CheckRunStatusUpdater) getAppCredentials(ctx context.Context, object client.Object) (*appCredentials, error) {
	var err error
	var found bool
	appInfo := appCredentials{}

	appInfo.InstallationID, err = strconv.ParseInt(object.GetAnnotations()[gitops.PipelineAsCodeInstallationIDAnnotation], 10, 64)
	if err != nil {
		return nil, err
	}

	// Get the global pipelines as code secret
	pacSecret := v1.Secret{}
	err = cru.k8sClient.Get(ctx, types.NamespacedName{Namespace: integrationNS, Name: PACSecret}, &pacSecret)
	if err != nil {
		return nil, err
	}

	// Get the App ID from the secret
	ghAppIDBytes, found := pacSecret.Data[gitHubApplicationID]
	if !found {
		return nil, errors.New("failed to find github-application-id secret key")
	}

	appInfo.AppID, err = strconv.ParseInt(string(ghAppIDBytes), 10, 64)
	if err != nil {
		return nil, err
	}

	// Get the App's private key from the secret
	appInfo.PrivateKey, found = pacSecret.Data[gitHubPrivateKey]
	if !found {
		return nil, errors.New("failed to find github-private-key secret key")
	}

	return &appInfo, nil
}

// Authenticate Github Client with application credentials
func (cru *CheckRunStatusUpdater) Authenticate(ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) error {
	creds, err := cru.getAppCredentials(ctx, snapshot)
	cru.creds = creds

	if err != nil {
		cru.logger.Error(err, "failed to get app credentials from Snapshot",
			"snapshot.NameSpace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
		return err
	}

	token, err := cru.ghClient.CreateAppInstallationToken(ctx, creds.AppID, creds.InstallationID, creds.PrivateKey)
	if err != nil {
		cru.logger.Error(err, "failed to create app installation token",
			"creds.AppID", creds.AppID, "creds.InstallationID", creds.InstallationID)
		return err
	}

	cru.ghClient.SetOAuthToken(ctx, token)

	return nil
}

func (cru *CheckRunStatusUpdater) getAllCheckRuns(ctx context.Context) ([]*ghapi.CheckRun, error) {
	if len(cru.allCheckRunsCache) == 0 {
		allCheckRuns, err := cru.ghClient.GetAllCheckRunsForRef(ctx, cru.owner, cru.repo, cru.sha, cru.creds.AppID)
		if err != nil {
			cru.logger.Error(err, "failed to get all checkruns for ref",
				"owner", cru.owner, "repo", cru.repo, "creds.AppID", cru.creds.AppID)
			return nil, err
		}
		cru.allCheckRunsCache = allCheckRuns
	}
	return cru.allCheckRunsCache, nil
}

// createCheckRunAdapterForSnapshot create a CheckRunAdapter for given snapshot, integrationTestStatusDetail, owner, repo and sha to create a checkRun
// https://docs.github.com/en/rest/checks/runs?apiVersion=2022-11-28#create-a-check-run
func (cru *CheckRunStatusUpdater) createCheckRunAdapterForSnapshot(report TestReport) (*github.CheckRunAdapter, error) {
	snapshot := cru.snapshot

	conclusion, err := generateCheckRunConclusion(report.Status)
	if err != nil {
		return nil, fmt.Errorf("unknown status %s for integrationTestScenario %s and snapshot %s/%s", report.Status, report.ScenarioName, snapshot.Namespace, snapshot.Name)
	}

	title, err := generateCheckRunTitle(report.Status)
	if err != nil {
		return nil, fmt.Errorf("unknown status %s for integrationTestScenario %s and snapshot %s/%s", report.Status, report.ScenarioName, snapshot.Namespace, snapshot.Name)
	}

	externalID := report.ScenarioName
	if report.ComponentName != "" {
		externalID = fmt.Sprintf("%s-%s", report.ScenarioName, report.ComponentName)
	}

	cra := &github.CheckRunAdapter{
		Owner:      cru.owner,
		Repository: cru.repo,
		Name:       report.FullName,
		SHA:        cru.sha,
		ExternalID: externalID,
		Conclusion: conclusion,
		Title:      title,
		Summary:    report.Summary,
		Text:       report.Text,
	}

	if start := report.StartTime; start != nil {
		cra.StartTime = *start
	}

	if complete := report.CompletionTime; complete != nil {
		cra.CompletionTime = *complete
	}

	return cra, nil
}

// UpdateStatus updates CheckRun status of PR
func (cru *CheckRunStatusUpdater) UpdateStatus(ctx context.Context, report TestReport) error {
	if cru.creds == nil {
		panic("authenticate first")
	}
	allCheckRuns, err := cru.getAllCheckRuns(ctx)

	if err != nil {
		return err
	}

	checkRun, err := cru.createCheckRunAdapterForSnapshot(report)
	if err != nil {
		cru.logger.Error(err, "failed to create checkRunAdapter for scenario, skipping update",
			"snapshot.NameSpace", cru.snapshot.Namespace, "snapshot.Name", cru.snapshot.Name,
			"scenario.Name", report.ScenarioName,
		)
		return nil
	}

	existingCheckrun := cru.ghClient.GetExistingCheckRun(allCheckRuns, checkRun)

	if existingCheckrun == nil {
		cru.logger.Info("creating checkrun for scenario test status of snapshot",
			"snapshot.NameSpace", cru.snapshot.Namespace, "snapshot.Name", cru.snapshot.Name, "scenarioName", report.ScenarioName)
		_, err = cru.ghClient.CreateCheckRun(ctx, checkRun)
		if err != nil {
			cru.logger.Error(err, "failed to create checkrun",
				"checkRun", checkRun)
			return err
		}
	} else {
		cru.logger.Info("found existing checkrun", "existingCheckRun", existingCheckrun)
		err = cru.ghClient.UpdateCheckRun(ctx, *existingCheckrun.ID, checkRun)
		if err != nil {
			cru.logger.Error(err, "failed to update checkrun",
				"checkRun", checkRun)
			return err
		}

	}
	return nil
}

// CommitStatusUpdater updates PR using Commit/RepoStatus (without application integration enabled)
type CommitStatusUpdater struct {
	ghClient               github.ClientInterface
	k8sClient              client.Client
	logger                 *logr.Logger
	owner                  string
	repo                   string
	sha                    string
	snapshot               *applicationapiv1alpha1.Snapshot
	allCommitStatusesCache []*ghapi.RepoStatus
}

// NewCommitStatusUpdater returns a pointer to initialized CommitStatusUpdater
func NewCommitStatusUpdater(
	ghClient github.ClientInterface,
	k8sClient client.Client,
	logger *logr.Logger,
	owner string,
	repo string,
	sha string,
	snapshot *applicationapiv1alpha1.Snapshot,
) *CommitStatusUpdater {
	return &CommitStatusUpdater{
		ghClient:  ghClient,
		k8sClient: k8sClient,
		logger:    logger,
		owner:     owner,
		repo:      repo,
		sha:       sha,
		snapshot:  snapshot,
	}
}

func (csu *CommitStatusUpdater) getAllCommitStatuses(ctx context.Context) ([]*ghapi.RepoStatus, error) {
	if len(csu.allCommitStatusesCache) == 0 {
		allCommitStatuses, err := csu.ghClient.GetAllCommitStatusesForRef(ctx, csu.owner, csu.repo, csu.sha)
		if err != nil {
			csu.logger.Error(err, "failed to get all commitStatuses for snapshot",
				"snapshot.NameSpace", csu.snapshot.Namespace, "snapshot.Name", csu.snapshot.Name)
			return nil, err
		}
		csu.allCommitStatusesCache = allCommitStatuses
	}
	return csu.allCommitStatusesCache, nil
}

// Authenticate Github Client with token secret ref defined in snapshot
func (csu *CommitStatusUpdater) Authenticate(ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) error {
	token, err := GetPACGitProviderToken(ctx, csu.k8sClient, snapshot)
	if err != nil {
		csu.logger.Error(err, "failed to get token from snapshot",
			"snapshot.NameSpace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
		return err
	}

	csu.ghClient.SetOAuthToken(ctx, token)
	return nil
}

// createCommitStatusAdapterForSnapshot create a commitStatusAdapter used to create commitStatus on GitHub
// https://docs.github.com/en/rest/commits/statuses?apiVersion=2022-11-28#create-a-commit-status
func (csu *CommitStatusUpdater) createCommitStatusAdapterForSnapshot(report TestReport) (*github.CommitStatusAdapter, error) {
	snapshot := csu.snapshot

	state, err := generateGithubCommitState(report.Status)
	if err != nil {
		return nil, fmt.Errorf("unknown status %s for integrationTestScenario %s and snapshot %s/%s", report.Status, report.ScenarioName, snapshot.Namespace, snapshot.Name)
	}

	return &github.CommitStatusAdapter{
		Owner:       csu.owner,
		Repository:  csu.repo,
		SHA:         csu.sha,
		State:       state,
		Description: report.Summary,
		Context:     report.FullName,
	}, nil
}

// updateStatusInComment will create/update a comment in PR which creates snapshot
func (csu *CommitStatusUpdater) updateStatusInComment(ctx context.Context, report TestReport) error {
	issueNumberStr, found := csu.snapshot.GetAnnotations()[gitops.PipelineAsCodePullRequestAnnotation]
	if !found {
		return fmt.Errorf("pull-request annotation not found %q", gitops.PipelineAsCodePullRequestAnnotation)
	}

	issueNumber, err := strconv.Atoi(issueNumberStr)
	if err != nil {
		return err
	}

	comment, err := FormatComment(report.Summary, report.Text)
	if err != nil {
		return fmt.Errorf("failed to generate comment for pull-request %d: %w", issueNumber, err)
	}

	allComments, err := csu.ghClient.GetAllCommentsForPR(ctx, csu.owner, csu.repo, issueNumber)
	if err != nil {
		return fmt.Errorf("error while getting all comments for pull-request %s: %w", issueNumberStr, err)
	}
	existingCommentId := csu.ghClient.GetExistingCommentID(allComments, csu.snapshot.Name, report.ScenarioName)
	if existingCommentId == nil {
		_, err = csu.ghClient.CreateComment(ctx, csu.owner, csu.repo, issueNumber, comment)
		if err != nil {
			return fmt.Errorf("error while creating comment for pull-request %s: %w", issueNumberStr, err)
		}
	} else {
		_, err = csu.ghClient.EditComment(ctx, csu.owner, csu.repo, *existingCommentId, comment)
		if err != nil {
			return fmt.Errorf("error while updating comment for pull-request %s: %w", issueNumberStr, err)
		}
	}

	return nil
}

// UpdateStatus updates commit status in PR
func (csu *CommitStatusUpdater) UpdateStatus(ctx context.Context, report TestReport) error {

	allCommitStatuses, err := csu.getAllCommitStatuses(ctx)
	if err != nil {
		return err
	}

	commitStatus, err := csu.createCommitStatusAdapterForSnapshot(report)
	if err != nil {
		csu.logger.Error(err, "failed to create CommitStatusAdapter for scenario, skipping update",
			"snapshot.NameSpace", csu.snapshot.Namespace, "snapshot.Name", csu.snapshot.Name,
			"scenario.Name", report.ScenarioName,
		)
		return nil
	}

	commitStatusExist, err := csu.ghClient.CommitStatusExists(allCommitStatuses, commitStatus)
	if err != nil {
		return err
	}

	if !commitStatusExist {
		csu.logger.Info("creating commit status for scenario test status of snapshot",
			"snapshot.NameSpace", csu.snapshot.Namespace, "snapshot.Name", csu.snapshot.Name, "scenarioName", report.ScenarioName)
		_, err = csu.ghClient.CreateCommitStatus(ctx, commitStatus.Owner, commitStatus.Repository, commitStatus.SHA, commitStatus.State, commitStatus.Description, commitStatus.Context)
		if err != nil {
			return err
		}
		// Create a comment when integration test is neither pending nor inprogress since comment for pending/inprogress is less meaningful and there is commitStatus for all statuses
		if report.Status != intgteststat.IntegrationTestStatusPending && report.Status != intgteststat.IntegrationTestStatusInProgress {
			err = csu.updateStatusInComment(ctx, report)
			if err != nil {
				return err
			}
		}
	} else {
		csu.logger.Info("found existing commitStatus for scenario test status of snapshot, no need to create new commit status",
			"snapshot.NameSpace", csu.snapshot.Namespace, "snapshot.Name", csu.snapshot.Name, "scenarioName", report.ScenarioName)
	}

	return nil
}

// GitHubReporter reports status back to GitHub for a Snapshot.
type GitHubReporter struct {
	logger    *logr.Logger
	k8sClient client.Client
	client    github.ClientInterface
	updater   StatusUpdater
}

// check if interface has been correctly implemented
var _ ReporterInterface = (*GitHubReporter)(nil)

// GitHubReporterOption is used to extend GitHubReporter with optional parameters.
type GitHubReporterOption = func(r *GitHubReporter)

func WithGitHubClient(client github.ClientInterface) GitHubReporterOption {
	return func(r *GitHubReporter) {
		r.client = client
	}
}

// NewGitHubReporter returns a struct implementing the Reporter interface for GitHub
func NewGitHubReporter(logger logr.Logger, k8sClient client.Client, opts ...GitHubReporterOption) *GitHubReporter {
	reporter := GitHubReporter{
		logger:    &logger,
		k8sClient: k8sClient,
		client:    github.NewClient(logger),
	}

	for _, opt := range opts {
		opt(&reporter)
	}

	return &reporter
}

type appCredentials struct {
	AppID          int64
	InstallationID int64
	PrivateKey     []byte
}

// generateTitle generate a Title of checkRun for the given state
func generateCheckRunTitle(state intgteststat.IntegrationTestStatus) (string, error) {
	var title string

	switch state {
	case intgteststat.IntegrationTestStatusPending:
		title = "Pending"
	case intgteststat.IntegrationTestStatusInProgress:
		title = "In Progress"
	case intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated,
		intgteststat.IntegrationTestStatusDeploymentError_Deprecated,
		intgteststat.IntegrationTestStatusTestInvalid:
		title = "Errored"
	case intgteststat.IntegrationTestStatusDeleted:
		title = "Deleted"
	case intgteststat.IntegrationTestStatusTestPassed:
		title = "Succeeded"
	case intgteststat.IntegrationTestStatusTestFail:
		title = "Failed"
	default:
		return title, fmt.Errorf("unknown status")
	}

	return title, nil
}

// generateCheckRunConclusion generate a conclusion as the conclusion of CheckRun
// Can be one of: action_required, cancelled, failure, neutral, success, skipped, stale, timed_out
// https://docs.github.com/en/rest/checks/runs?apiVersion=2022-11-28#create-a-check-run
func generateCheckRunConclusion(state intgteststat.IntegrationTestStatus) (string, error) {
	var conclusion string

	switch state {
	case intgteststat.IntegrationTestStatusTestFail, intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated,
		intgteststat.IntegrationTestStatusDeploymentError_Deprecated, intgteststat.IntegrationTestStatusDeleted,
		intgteststat.IntegrationTestStatusTestInvalid:
		conclusion = gitops.IntegrationTestStatusFailureGithub
	case intgteststat.IntegrationTestStatusTestPassed:
		conclusion = gitops.IntegrationTestStatusSuccessGithub
	case intgteststat.IntegrationTestStatusPending, intgteststat.IntegrationTestStatusInProgress:
		conclusion = ""
	default:
		return conclusion, fmt.Errorf("unknown status")
	}

	return conclusion, nil
}

// generateGithubCommitState generate state of CommitStatus
// Can be one of: error, failure, pending, success
// https://docs.github.com/en/rest/commits/statuses?apiVersion=2022-11-28#create-a-commit-status
func generateGithubCommitState(state intgteststat.IntegrationTestStatus) (string, error) {
	var commitState string

	switch state {
	case intgteststat.IntegrationTestStatusTestFail:
		commitState = gitops.IntegrationTestStatusFailureGithub
	case intgteststat.IntegrationTestStatusEnvironmentProvisionError_Deprecated, intgteststat.IntegrationTestStatusDeploymentError_Deprecated,
		intgteststat.IntegrationTestStatusDeleted, intgteststat.IntegrationTestStatusTestInvalid:
		commitState = gitops.IntegrationTestStatusErrorGithub
	case intgteststat.IntegrationTestStatusTestPassed:
		commitState = gitops.IntegrationTestStatusSuccessGithub
	case intgteststat.IntegrationTestStatusPending, intgteststat.IntegrationTestStatusInProgress:
		commitState = gitops.IntegrationTestStatusPendingGithub
	default:
		return commitState, fmt.Errorf("unknown status")
	}

	return commitState, nil
}

// Detect if GitHubReporter can be used
func (r *GitHubReporter) Detect(snapshot *applicationapiv1alpha1.Snapshot) bool {
	return metadata.HasLabelWithValue(snapshot, gitops.PipelineAsCodeGitProviderLabel, gitops.PipelineAsCodeGitHubProviderType)
}

// Initialize github reporter. Must be called before updating status
func (r *GitHubReporter) Initialize(ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) error {
	labels := snapshot.GetLabels()

	owner, found := labels[gitops.PipelineAsCodeURLOrgLabel]
	if !found {
		return fmt.Errorf("org label not found %q", gitops.PipelineAsCodeURLOrgLabel)
	}

	repo, found := labels[gitops.PipelineAsCodeURLRepositoryLabel]
	if !found {
		return fmt.Errorf("repository label not found %q", gitops.PipelineAsCodeURLRepositoryLabel)
	}

	sha, found := labels[gitops.PipelineAsCodeSHALabel]
	if !found {
		return fmt.Errorf("sha label not found %q", gitops.PipelineAsCodeSHALabel)
	}

	// Existence of the Pipelines as Code installation ID annotation signals configuration using GitHub App integration.
	// If it doesn't exist, GitHub webhook integration is configured.
	if metadata.HasAnnotation(snapshot, gitops.PipelineAsCodeInstallationIDAnnotation) {
		r.updater = NewCheckRunStatusUpdater(r.client, r.k8sClient, r.logger, owner, repo, sha, snapshot)
	} else {
		r.updater = NewCommitStatusUpdater(r.client, r.k8sClient, r.logger, owner, repo, sha, snapshot)
	}

	if err := r.updater.Authenticate(ctx, snapshot); err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	return nil
}

// Return reporter name
func (r *GitHubReporter) GetReporterName() string {
	return "GithubReporter"
}

// Update status in Github
func (r *GitHubReporter) ReportStatus(ctx context.Context, report TestReport) error {
	if r.updater == nil {
		return fmt.Errorf("reporter is not initialized")
	}

	if err := r.updater.UpdateStatus(ctx, report); err != nil {
		return fmt.Errorf("failed to update status: %w", err)
	}
	return nil
}
