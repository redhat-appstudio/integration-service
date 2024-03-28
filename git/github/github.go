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

package github

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/go-logr/logr"
	ghapi "github.com/google/go-github/v45/github"
	"golang.org/x/oauth2"
)

// CheckRunAdapter is an abstraction for the github.CheckRun struct.
type CheckRunAdapter struct {
	Owner          string
	Repository     string
	Name           string
	SHA            string
	ExternalID     string
	DetailsURL     string
	Conclusion     string
	Title          string
	Summary        string
	Text           string
	StartTime      time.Time
	CompletionTime time.Time
}

// CommitStatusAdapter is an abstraction for the github.CommiStatus struct.
type CommitStatusAdapter struct {
	Owner       string
	Repository  string
	SHA         string
	State       string
	Description string
	Context     string
	TargetURL   string
}

// GetStatus returns the appropriate status based on conclusion and start time.
func (s *CheckRunAdapter) GetStatus() string {
	if s.Conclusion == "success" || s.Conclusion == "failure" {
		return "completed"
	} else if s.StartTime.IsZero() {
		return "queued"
	}
	return "in_progress"
}

// AppsService defines the methods used in the github Apps service.
type AppsService interface {
	CreateInstallationToken(ctx context.Context, id int64, opts *ghapi.InstallationTokenOptions) (*ghapi.InstallationToken, *ghapi.Response, error)
}

// ChecksService defines the methods used in the github Checks service.
type ChecksService interface {
	CreateCheckRun(ctx context.Context, owner string, repo string, opts ghapi.CreateCheckRunOptions) (*ghapi.CheckRun, *ghapi.Response, error)
	ListCheckRunsForRef(ctx context.Context, owner string, repo string, ref string, opts *ghapi.ListCheckRunsOptions) (*ghapi.ListCheckRunsResults, *ghapi.Response, error)
	UpdateCheckRun(ctx context.Context, owner string, repo string, checkRunID int64, opts ghapi.UpdateCheckRunOptions) (*ghapi.CheckRun, *ghapi.Response, error)
}

// IssuesService defines the methods used in the github Issues service.
type IssuesService interface {
	CreateComment(ctx context.Context, owner string, repo string, number int, comment *ghapi.IssueComment) (*ghapi.IssueComment, *ghapi.Response, error)
	ListComments(ctx context.Context, owner string, repo string, number int, opts *ghapi.IssueListCommentsOptions) ([]*ghapi.IssueComment, *ghapi.Response, error)
	EditComment(ctx context.Context, owner string, repo string, id int64, comment *ghapi.IssueComment) (*ghapi.IssueComment, *ghapi.Response, error)
}

// RepositoriesService defines the methods used in the github Repositories service.
type RepositoriesService interface {
	CreateStatus(ctx context.Context, owner string, repo string, ref string, status *ghapi.RepoStatus) (*ghapi.RepoStatus, *ghapi.Response, error)
	ListStatuses(ctx context.Context, owner, repo, ref string, opts *ghapi.ListOptions) ([]*ghapi.RepoStatus, *ghapi.Response, error)
}

// ClientInterface defines the methods that should be implemented by a GitHub client
type ClientInterface interface {
	CreateAppInstallationToken(ctx context.Context, appID int64, installationID int64, privateKey []byte) (string, error)
	SetOAuthToken(ctx context.Context, token string)
	CreateCheckRun(ctx context.Context, cra *CheckRunAdapter) (*int64, error)
	UpdateCheckRun(ctx context.Context, checkRunID int64, cra *CheckRunAdapter) error
	GetCheckRunID(ctx context.Context, owner string, repo string, SHA string, externalID string, appID int64) (*int64, error)
	CreateComment(ctx context.Context, owner string, repo string, issueNumber int, body string) (int64, error)
	CreateCommitStatus(ctx context.Context, owner string, repo string, SHA string, state string, description string, statusContext string, targetURL string) (int64, error)
	GetAllCheckRunsForRef(ctx context.Context, owner string, repo string, SHA string, appID int64) ([]*ghapi.CheckRun, error)
	GetExistingCheckRun(checkRuns []*ghapi.CheckRun, newCheckRun *CheckRunAdapter) *ghapi.CheckRun
	GetAllCommitStatusesForRef(ctx context.Context, owner, repo, sha string) ([]*ghapi.RepoStatus, error)
	GetAllCommentsForPR(ctx context.Context, owner string, repo string, pr int) ([]*ghapi.IssueComment, error)
	CommitStatusExists(res []*ghapi.RepoStatus, commitStatus *CommitStatusAdapter) (bool, error)
	GetExistingCommentID(comments []*ghapi.IssueComment, snapshotName, scenarioName string) *int64
	EditComment(ctx context.Context, owner string, repo string, commentID int64, body string) (int64, error)
}

// Client is an abstraction around the API client.
type Client struct {
	logger logr.Logger
	gh     *ghapi.Client
	apps   AppsService
	checks ChecksService
	issues IssuesService
	repos  RepositoriesService
}

// GetAppsService returns either the default or custom Apps service.
func (c *Client) GetAppsService() AppsService {
	if c.apps == nil {
		return c.gh.Apps
	}
	return c.apps
}

// GetChecksService returns either the default or custom Checks service.
func (c *Client) GetChecksService() ChecksService {
	if c.checks == nil {
		return c.gh.Checks
	}
	return c.checks
}

// GetIssuesService returns either the default or custom Issues service.
func (c *Client) GetIssuesService() IssuesService {
	if c.issues == nil {
		return c.gh.Issues
	}
	return c.issues
}

// GetRepositoriesService returns either the default or custom Repositories service.
func (c *Client) GetRepositoriesService() RepositoriesService {
	if c.repos == nil {
		return c.gh.Repositories
	}
	return c.repos
}

// ClientOption is used to extend Client with optional parameters.
type ClientOption = func(c *Client)

// WithAppsService is an option which allows for overriding the github client's default Apps service.
func WithAppsService(svc AppsService) ClientOption {
	return func(c *Client) {
		c.apps = svc
	}
}

// WithChecksService is an option which allows for overriding the github client's default Checks service.
func WithChecksService(svc ChecksService) ClientOption {
	return func(c *Client) {
		c.checks = svc
	}
}

// WithIssuesService is an option which allows for overriding the github client's default Issues service.
func WithIssuesService(svc IssuesService) ClientOption {
	return func(c *Client) {
		c.issues = svc
	}
}

// WithRepositoriesService is an option which allows for overriding the github client's default Issues service.
func WithRepositoriesService(svc RepositoriesService) ClientOption {
	return func(c *Client) {
		c.repos = svc
	}
}

// NewClient constructs a new Client.
func NewClient(logger logr.Logger, opts ...ClientOption) *Client {
	client := Client{
		logger: logger,
	}

	for _, opt := range opts {
		opt(&client)
	}

	return &client
}

// CreateAppInstallationToken creates an installation token for a GitHub App.
func (c *Client) CreateAppInstallationToken(ctx context.Context, appID int64, installationID int64, privateKey []byte) (string, error) {
	transport, err := ghinstallation.NewAppsTransport(http.DefaultTransport, appID, privateKey)
	if err != nil {
		return "", err
	}

	c.gh = ghapi.NewClient(&http.Client{Transport: transport})

	installToken, _, err := c.GetAppsService().CreateInstallationToken(
		ctx,
		installationID,
		&ghapi.InstallationTokenOptions{},
	)

	if err != nil {
		return "", err
	}

	return installToken.GetToken(), nil
}

// SetOAuthToken configures the client with a GitHub OAuth token.
func (c *Client) SetOAuthToken(ctx context.Context, token string) {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)

	c.gh = ghapi.NewClient(oauth2.NewClient(ctx, ts))
}

// CreateCheckRun creates a new CheckRun via the GitHub API.
func (c *Client) CreateCheckRun(ctx context.Context, cra *CheckRunAdapter) (*int64, error) {
	status := cra.GetStatus()

	options := ghapi.CreateCheckRunOptions{
		Name:       cra.Name,
		HeadSHA:    cra.SHA,
		ExternalID: &cra.ExternalID,
		DetailsURL: &cra.DetailsURL,
		Status:     &status,
		Output: &ghapi.CheckRunOutput{
			Title:   &cra.Title,
			Summary: &cra.Summary,
			Text:    &cra.Text,
		},
	}

	if cra.Conclusion != "" {
		options.Conclusion = &cra.Conclusion
	}

	if !cra.StartTime.IsZero() {
		options.StartedAt = &ghapi.Timestamp{Time: cra.StartTime}
	}

	if !cra.CompletionTime.IsZero() {
		options.CompletedAt = &ghapi.Timestamp{Time: cra.CompletionTime}

	}

	if cra.DetailsURL != "" {
		options.DetailsURL = &cra.DetailsURL
	}

	cr, _, err := c.GetChecksService().CreateCheckRun(ctx, cra.Owner, cra.Repository, options)

	if err != nil {
		return nil, fmt.Errorf("failed to create check run for for owner/repo/Ref %s/%s/%s: %w", cra.Owner, cra.Repository, cra.SHA, err)
	}

	c.logger.Info("Created CheckRun",
		"ID", cr.ID,
		"CheckName", cr.Name,
		"Status", cr.Status,
		"Conclusion", cr.Conclusion,
	)

	return cr.ID, nil
}

// UpdateCheckRun updates an existing CheckRun via the GitHub API.
func (c *Client) UpdateCheckRun(ctx context.Context, checkRunID int64, cra *CheckRunAdapter) error {
	status := cra.GetStatus()

	options := ghapi.UpdateCheckRunOptions{
		Name:   cra.Name,
		Status: &status,
		Output: &ghapi.CheckRunOutput{
			Title:   &cra.Title,
			Summary: &cra.Summary,
			Text:    &cra.Text,
		},
	}

	if cra.Conclusion != "" {
		options.Conclusion = &cra.Conclusion
	}

	if !cra.CompletionTime.IsZero() {
		options.CompletedAt = &ghapi.Timestamp{Time: cra.CompletionTime}
	}

	cr, _, err := c.GetChecksService().UpdateCheckRun(ctx, cra.Owner, cra.Repository, checkRunID, options)

	if err != nil {
		return err
	}

	c.logger.Info("Updated CheckRun",
		"ID", cr.ID,
		"CheckName", cr.Name,
		"Status", cr.Status,
		"Conclusion", cr.Conclusion,
	)
	return nil

}

// GetCheckRunID returns an existing GitHub CheckRun ID if a match is found for the SHA, externalID and appID.
func (c *Client) GetCheckRunID(ctx context.Context, owner string, repo string, SHA string, externalID string, appID int64) (*int64, error) {
	filter := "all"

	res, _, err := c.GetChecksService().ListCheckRunsForRef(
		ctx,
		owner,
		repo,
		SHA,
		&ghapi.ListCheckRunsOptions{
			AppID:  &appID,
			Filter: &filter,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to list all checks run for GitHub owner/repo/Ref %s/%s/%s: %w", owner, repo, SHA, err)
	}

	if *res.Total == 0 {
		c.logger.Info("Found no CheckRuns for the ref", "SHA", SHA)
		return nil, nil
	}

	for _, cr := range res.CheckRuns {
		if *cr.ExternalID == externalID {
			return cr.ID, nil
		}
	}
	c.logger.Info("Found no CheckRuns with a matching ExternalID", "ExternalID", externalID)

	return nil, nil
}

// GetAllCheckRunsForRef returns all existing GitHub CheckRuns if a match for the Owner, Repo, SHA, and appID.
func (c *Client) GetAllCheckRunsForRef(ctx context.Context, owner string, repo string, SHA string, appID int64) ([]*ghapi.CheckRun, error) {
	filter := "all"

	res, _, err := c.GetChecksService().ListCheckRunsForRef(
		ctx,
		owner,
		repo,
		SHA,
		&ghapi.ListCheckRunsOptions{
			AppID:  &appID,
			Filter: &filter,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get all check runs for GitHub owner/repo/Ref %s/%s/%s: %w", owner, repo, SHA, err)
	}

	if *res.Total == 0 {
		c.logger.Info("Found no CheckRuns for the ref", "SHA", SHA)
		return nil, nil
	}

	return res.CheckRuns, nil
}

// GetExistingCheckRun returns existing GitHub CheckRun for the ExternalID in checkRunAdapter.
func (c *Client) GetExistingCheckRun(checkRuns []*ghapi.CheckRun, newCheckRun *CheckRunAdapter) *ghapi.CheckRun {
	for _, cr := range checkRuns {
		if *cr.ExternalID == newCheckRun.ExternalID {
			c.logger.Info("found CheckRun with a matching ExternalID", "ExternalID", newCheckRun.ExternalID)
			return cr
		}
	}
	c.logger.Info("found no CheckRuns with a matching ExternalID", "ExternalID", newCheckRun.ExternalID)
	return nil
}

// GetExistingComment returns existing GitHub comment for the scenario of ref.
func (c *Client) GetExistingCommentID(comments []*ghapi.IssueComment, snapshotName, scenarioName string) *int64 {
	for _, comment := range comments {
		if strings.Contains(*comment.Body, snapshotName) && strings.Contains(*comment.Body, scenarioName) {
			c.logger.Info("found comment ID with a matching scenarioName", "scenarioName", scenarioName)
			return comment.ID
		}
	}
	c.logger.Info("found no comment with a matching scenarioName", "scenarioName", scenarioName)
	return nil
}

// GetAllCommitStatusesForRef returns all existing GitHub CommitStatuses if a match for the Owner, Repo, and SHA.
func (c *Client) GetAllCommitStatusesForRef(ctx context.Context, owner, repo, sha string) ([]*ghapi.RepoStatus, error) {
	res, _, err := c.GetRepositoriesService().ListStatuses(ctx, owner, repo, sha, &ghapi.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get all commit statuses for GitHub owner/repo/Ref %s/%s/%s: %w", owner, repo, sha, err)
	}

	if len(res) == 0 {
		c.logger.Info("Found no commitStatus for the ref", "SHA", sha)
		return nil, nil
	}

	return res, nil
}

// GetAllCommentsForPR returns all existing comment if a match for the Owner, Repo, and PR.
func (c *Client) GetAllCommentsForPR(ctx context.Context, owner string, repo string, number int) ([]*ghapi.IssueComment, error) {
	res, _, err := c.GetIssuesService().ListComments(ctx, owner, repo, number, &ghapi.IssueListCommentsOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get all comments for GitHub owner/repo/PR %s/%s/%d: %w", owner, repo, number, err)
	}

	if len(res) == 0 {
		c.logger.Info("Found no comments for PR", "PR", number)
		return nil, nil
	}

	return res, nil
}

// CommitStatusExists returns if a match is found for the SHA, state, context and decription.
func (c *Client) CommitStatusExists(res []*ghapi.RepoStatus, commitStatus *CommitStatusAdapter) (bool, error) {
	for _, cs := range res {
		if *cs.State == commitStatus.State && *cs.Description == commitStatus.Description && *cs.Context == commitStatus.Context && *cs.TargetURL == commitStatus.TargetURL {
			c.logger.Info("Found CommitStatus with matching conditions", "CommitStatus.State", commitStatus.State, "CommitStatus.Description", commitStatus.Description, "CommitStatus.Context", commitStatus.Context, "CommitStatus.TargetURL", commitStatus.TargetURL)
			return true, nil
		}
	}
	c.logger.Info("Found no CommitStatus with matching conditions", "CommitStatus.State", commitStatus.State, "CommitStatus.Description", commitStatus.Description, "CommitStatus.Context", commitStatus.Context, "CommitStatus.TargetURL", commitStatus.TargetURL)

	return false, nil
}

// CreateComment creates a new issue comment via the GitHub API.
func (c *Client) CreateComment(ctx context.Context, owner string, repo string, issueNumber int, body string) (int64, error) {
	comment, _, err := c.GetIssuesService().CreateComment(ctx, owner, repo, issueNumber, &ghapi.IssueComment{Body: &body})
	if err != nil {
		return 0, fmt.Errorf("failed to create a comment for GitHub owner/repo/PR %s/%s/%d: %w", owner, repo, issueNumber, err)
	}

	c.logger.Info("Created comment",
		"ID", comment.ID,
		"Owner", owner,
		"Repository", repo,
		"IssueNumber", issueNumber,
	)
	return *comment.ID, nil
}

// EditComment edits an existing issue comment via the GitHub API.
func (c *Client) EditComment(ctx context.Context, owner string, repo string, commentID int64, body string) (int64, error) {
	comment, _, err := c.GetIssuesService().EditComment(ctx, owner, repo, commentID, &ghapi.IssueComment{Body: &body})
	if err != nil {
		return 0, fmt.Errorf("failed to edit an existing comment for GitHub owner/repo/comment %s/%s/%d: %w", owner, repo, commentID, err)
	}

	c.logger.Info("Edited comment",
		"ID", comment.ID,
		"Owner", owner,
		"Repository", repo,
		"commentID", commentID,
	)
	return *comment.ID, nil
}

// CreateCommitStatus creates a repository commit status via the GitHub API.
func (c *Client) CreateCommitStatus(ctx context.Context, owner string, repo string, SHA string, state string, description string, statusContext string, targetURL string) (int64, error) {
	status, _, err := c.GetRepositoriesService().CreateStatus(ctx, owner, repo, SHA, &ghapi.RepoStatus{State: &state, Description: &description, Context: &statusContext, TargetURL: &targetURL})
	if err != nil {
		return 0, fmt.Errorf("failed to create an existing commitStatus for GitHub owner/repo/ref %s/%s/%s: %w", owner, repo, SHA, err)
	}

	c.logger.Info("Created commit status",
		"ID", status.ID,
		"Owner", owner,
		"Repository", repo,
		"SHA", SHA,
		"State", state,
	)
	return *status.ID, nil
}
