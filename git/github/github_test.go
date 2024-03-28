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

package github_test

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	ghapi "github.com/google/go-github/v45/github"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redhat-appstudio/integration-service/git/github"
)

const samplePrivateKey = `-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDFuZgVJy0ZUPMa8WerKv/bY9uyBfIJVAdJHf8S1tT2hhLd0hTr
gRTwPdfQOPTpoBZwzDDDjaeztgGCHxDtc4MI06mRJ/bdKKbWOseybONHMRRAC02X
Wql/QhOT49n77FrculoKSZ9P7M9mHOiwWZgqcZCHNfvE0zzDtazbAz71FwIDAQAB
AoGANkwCHIT2mIYnTFXQnyueuVORyDBjx/YATi7jlfvS3aPx2GJRhl6bLmH9Otv5
PZLNtsoL9heXXv1zKpV3Y42eBLkAsIIyD7H2o74KSRCj8w1mOpvzLgS8fjy7Cve8
NUVmhaNfDvrDck16yXkYQ3tF68DFRbs0an7lrstqBf4Bp6kCQQD+j1qhqoxz00Jo
nES3Ba8PVGBvOuXK7IKD/ul7QZdalC0bChM5WM3tKcNNXQvlcffjqUyv55jabzax
I/MstVIbAkEAxtfu7xF/hOYor5bZ51pOPozg0PLm7u+n2NGoa5gbt3XDAw88+uAl
jdWzmPDmKefD7x0eUuWImRVGj9pg2KI4tQJBAIwUlusfzupt9r1aQPX2Sr9Ez0xm
PM3OGIRKYnFRRtixcaDPioSkOa2orbCE42b/sEm3gFvMNnD9gjs4bTjNDsECQQCq
3D1pnAWRMxxe1Sbkj0qQoQlFQpOBWNlHi9BGs+uNF1m8tUlW4Wgjmi+7CbUc6RQJ
0WGPJcDcmdWKIsH5JFvJAkEAyNNOsbLp1DjZE0rMS5W8YHPR5USzlOSprsrsN8F/
WGhfkh9h9Db8xSx4boFcEqwyyHd2E1Hjbp5q3fzp06XecA==
-----END RSA PRIVATE KEY-----`

type MockAppsService struct{}

// CreateInstallationToken implements github.AppsService
func (MockAppsService) CreateInstallationToken(
	ctx context.Context, id int64, opts *ghapi.InstallationTokenOptions,
) (*ghapi.InstallationToken, *ghapi.Response, error) {
	token := "example-token"
	return &ghapi.InstallationToken{Token: &token}, nil, nil
}

type MockChecksService struct {
	ListCheckRunsForRefResult []*ghapi.CheckRun
}

// CreateCheckRun implements github.ChecksService
func (MockChecksService) CreateCheckRun(
	ctx context.Context, owner string, repo string, opts ghapi.CreateCheckRunOptions,
) (*ghapi.CheckRun, *ghapi.Response, error) {
	var id int64 = 10
	return &ghapi.CheckRun{ID: &id}, nil, nil
}

// ListCheckRunsForRef implements github.ChecksService
func (MockChecksService) ListCheckRunsForRef(
	ctx context.Context, owner string, repo string, ref string, opts *ghapi.ListCheckRunsOptions,
) (*ghapi.ListCheckRunsResults, *ghapi.Response, error) {
	var id int64 = 20
	var externalID string = "example-external-id"
	var text string = "example-text-update"
	var checkRunOutput = ghapi.CheckRunOutput{Text: &text}
	conclusion := "failure"
	checkRuns := []*ghapi.CheckRun{{ID: &id, ExternalID: &externalID, Conclusion: &conclusion, Output: &checkRunOutput}}
	total := len(checkRuns)
	return &ghapi.ListCheckRunsResults{Total: &total, CheckRuns: checkRuns}, nil, nil
}

// GetAllCheckRunsForRef implements github.ChecksService
func (MockChecksService) GetAllCheckRunsForRef(
	ctx context.Context, owner string, repo string, ref string, appID int64,
) ([]*ghapi.CheckRun, error) {
	var id int64 = 20
	var externalID string = "example-external-id"
	var text string = "example-text-update"
	var checkRunOutput = ghapi.CheckRunOutput{Text: &text}
	conclusion := "failure"
	checkRuns := []*ghapi.CheckRun{{ID: &id, ExternalID: &externalID, Conclusion: &conclusion, Output: &checkRunOutput}}
	return checkRuns, nil
}

// UpdateCheckRun implements github.ChecksService
func (MockChecksService) UpdateCheckRun(
	ctx context.Context, owner string, repo string, checkRunID int64, opts ghapi.UpdateCheckRunOptions,
) (*ghapi.CheckRun, *ghapi.Response, error) {
	var id int64 = 30
	return &ghapi.CheckRun{ID: &id}, nil, nil
}

type MockIssuesService struct{}

// CreateComment implements github.IssuesService
func (MockIssuesService) CreateComment(
	ctx context.Context, owner string, repo string, number int, comment *ghapi.IssueComment,
) (*ghapi.IssueComment, *ghapi.Response, error) {
	var id int64 = 40
	return &ghapi.IssueComment{ID: &id}, nil, nil
}

// ListComments implements github.IssuesService
func (MockIssuesService) ListComments(ctx context.Context, owner string, repo string,
	number int, opts *ghapi.IssueListCommentsOptions) ([]*ghapi.IssueComment, *ghapi.Response, error) {
	var id int64 = 40
	var body string = "Integration test for snapshot snapshotName and scenario scenarioName"
	issueComments := []*ghapi.IssueComment{{ID: &id, Body: &body}}
	return issueComments, nil, nil
}

// EditComment implements github.IssuesService
func (MockIssuesService) EditComment(ctx context.Context, owner string, repo string, number int64, comment *ghapi.IssueComment,
) (*ghapi.IssueComment, *ghapi.Response, error) {
	return &ghapi.IssueComment{ID: &number}, nil, nil
}

type MockRepositoriesService struct{}

// CreateStatus implements github.RepositoriesService
func (MockRepositoriesService) CreateStatus(
	ctx context.Context, owner string, repo string, ref string, status *ghapi.RepoStatus,
) (*ghapi.RepoStatus, *ghapi.Response, error) {
	var id int64 = 50
	var state = "success"
	return &ghapi.RepoStatus{ID: &id, State: &state}, nil, nil
}

// ListStatuses implements github.RepositoriesService
func (MockRepositoriesService) ListStatuses(
	ctx context.Context, owner string, repo string, ref string, opts *ghapi.ListOptions,
) ([]*ghapi.RepoStatus, *ghapi.Response, error) {
	var id int64 = 60
	var state = "success"
	var description = "example-description"
	var context = "example-context"
	var target_url = "https://example.com"
	repoStatus := &ghapi.RepoStatus{ID: &id, State: &state, Description: &description, Context: &context, TargetURL: &target_url}
	return []*ghapi.RepoStatus{repoStatus}, nil, nil
}

var _ = Describe("CheckRunAdapter", func() {
	It("can compute status", func() {
		adapter := &github.CheckRunAdapter{Conclusion: "success", StartTime: time.Time{}}
		Expect(adapter.GetStatus()).To(Equal("completed"))
		adapter.Conclusion = "failure"
		Expect(adapter.GetStatus()).To(Equal("completed"))
		adapter.Conclusion = ""
		Expect(adapter.GetStatus()).To(Equal("queued"))
		adapter.StartTime = time.Now()
		Expect(adapter.GetStatus()).To(Equal("in_progress"))
	})
})

var _ = Describe("Client", func() {

	var (
		client        *github.Client
		mockAppsSvc   MockAppsService
		mockChecksSvc MockChecksService
		mockIssuesSvc MockIssuesService
		mockReposSvc  MockRepositoriesService
	)

	var checkRunAdapter = &github.CheckRunAdapter{
		Name:           "example-name",
		Owner:          "example-owner",
		Repository:     "example-repo",
		SHA:            "abcdef1",
		ExternalID:     "example-external-id",
		DetailsURL:     "https://example.com",
		Conclusion:     "Passed",
		Title:          "example-title",
		Summary:        "example-summary",
		Text:           "example-text",
		StartTime:      time.Now(),
		CompletionTime: time.Now(),
	}

	var commitStatusAdapter = &github.CommitStatusAdapter{
		Owner:       "example-owner",
		Repository:  "example-repo",
		SHA:         "abcdef1",
		State:       "success",
		Description: "example-description",
		Context:     "example-context",
		TargetURL:   "https://example.com",
	}

	BeforeEach(func() {
		mockAppsSvc = MockAppsService{}
		mockChecksSvc = MockChecksService{}
		mockIssuesSvc = MockIssuesService{}
		mockReposSvc = MockRepositoriesService{}
		client = github.NewClient(
			logr.Discard(),
			github.WithAppsService(mockAppsSvc),
			github.WithChecksService(mockChecksSvc),
			github.WithIssuesService(mockIssuesSvc),
			github.WithRepositoriesService(mockReposSvc),
		)
	})

	It("can create app installation tokens", func() {
		token, err := client.CreateAppInstallationToken(context.TODO(), 1, 1, []byte(samplePrivateKey))
		Expect(err).To(BeNil())
		Expect(token).To(Equal("example-token"))
	})

	It("accepts an OAuth token", func() {
		client.SetOAuthToken(context.TODO(), "example-token")
		Expect(client.GetAppsService()).To(Equal(mockAppsSvc))
		Expect(client.GetChecksService()).To(Equal(mockChecksSvc))
		Expect(client.GetIssuesService()).To(Equal(mockIssuesSvc))
		Expect(client.GetRepositoriesService()).To(Equal(mockReposSvc))

		client = github.NewClient(logr.Discard())
		client.SetOAuthToken(context.TODO(), "example-token")
		Expect(client.GetAppsService()).ToNot(Equal(mockAppsSvc))
		Expect(client.GetChecksService()).ToNot(Equal(mockChecksSvc))
		Expect(client.GetIssuesService()).ToNot(Equal(mockIssuesSvc))
		Expect(client.GetRepositoriesService()).ToNot(Equal(mockReposSvc))
	})

	It("can create comments", func() {
		id, err := client.CreateComment(context.TODO(), "", "", 1, "example-comment")
		Expect(err).To(BeNil())
		Expect(id).To(Equal(int64(40)))
	})

	It("can create commit statuses", func() {
		id, err := client.CreateCommitStatus(context.TODO(), "", "", "", "", "", "", "")
		Expect(err).To(BeNil())
		Expect(id).To(Equal(int64(50)))
	})

	It("can set and get details URL", func() {
		detilsURL := "https://example.com/details"

		checkRunAdapter.DetailsURL = detilsURL
		Expect(checkRunAdapter.DetailsURL).To(Equal(detilsURL))
	})

	It("can create check runs", func() {
		checkRunID, err := client.CreateCheckRun(context.TODO(), checkRunAdapter)
		Expect(err).To(BeNil())
		Expect(checkRunID).ToNot(BeNil())
		Expect(*checkRunID).To(Equal(int64(10)))
	})

	It("can update check runs", func() {
		err := client.UpdateCheckRun(context.TODO(), 1, checkRunAdapter)
		Expect(err).To(BeNil())
	})

	It("can get a check run ID", func() {
		checkRunID, err := client.GetCheckRunID(context.TODO(), "", "", "", "example-external-id", 1)
		Expect(err).To(BeNil())
		Expect(checkRunID).ToNot(BeNil())
		Expect(*checkRunID).To(Equal(int64(20)))

		checkRunID, err = client.GetCheckRunID(context.TODO(), "", "", "", "unknown-external-id", 1)
		Expect(err).To(BeNil())
		Expect(checkRunID).To(BeNil())
	})

	It("can check if check run updated is needed", func() {
		var checkRunAdapter = &github.CheckRunAdapter{
			Name:           "example-name",
			Owner:          "example-owner",
			Repository:     "example-repo",
			SHA:            "abcdef1",
			ExternalID:     "example-external-id",
			Conclusion:     "success",
			Title:          "example-title",
			Summary:        "example-summary",
			Text:           "example-text",
			StartTime:      time.Now(),
			CompletionTime: time.Now(),
		}

		allCheckRuns, err := client.GetAllCheckRunsForRef(context.TODO(), "", "", "", 1)
		Expect(err).To(BeNil())
		Expect(allCheckRuns).NotTo(BeEmpty())

		existingCheckRun := client.GetExistingCheckRun(allCheckRuns, checkRunAdapter)
		Expect(existingCheckRun).NotTo(BeNil())

	})

	It("can check if creating a new commit status is needed", func() {
		commitStatuses, err := client.GetAllCommitStatusesForRef(context.TODO(), "", "", "")
		Expect(err).To(BeNil())
		Expect(commitStatuses).NotTo(BeEmpty())

		commitStatusExist, err := client.CommitStatusExists(commitStatuses, commitStatusAdapter)
		Expect(commitStatusExist).To(BeTrue())
		Expect(err).To(BeNil())

		commitStatusAdapter = &github.CommitStatusAdapter{
			Owner:       "example-owner",
			Repository:  "example-repo",
			SHA:         "abcdef1",
			State:       "failure",
			Description: "example-description",
			Context:     "example-context",
			TargetURL:   "https://example.com",
		}
		commitStatusExist, err = client.CommitStatusExists(commitStatuses, commitStatusAdapter)
		Expect(commitStatusExist).To(BeFalse())
		Expect(err).To(BeNil())
	})

	It("can get existing comment id", func() {
		comments, err := client.GetAllCommentsForPR(context.TODO(), "", "", 1)
		Expect(err).To(BeNil())
		Expect(comments).NotTo(BeEmpty())

		commentID := client.GetExistingCommentID(comments, "snapshotName", "scenarioName")
		Expect(*commentID).To(Equal(int64(40)))
	})

	It("can edit comments", func() {
		id, err := client.EditComment(context.TODO(), "", "", 1, "example-comment")
		Expect(err).To(BeNil())
		Expect(id).To(Equal(int64(1)))
	})
})
