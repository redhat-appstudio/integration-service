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

package buildpipeline

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/integration-service/metrics"
	"github.com/redhat-appstudio/integration-service/tekton"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Adapter holds the objects needed to reconcile a build PipelineRun.
type Adapter struct {
	pipelineRun *tektonv1.PipelineRun
	component   *applicationapiv1alpha1.Component
	application *applicationapiv1alpha1.Application
	loader      loader.ObjectLoader
	logger      h.IntegrationLogger
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(pipelineRun *tektonv1.PipelineRun, component *applicationapiv1alpha1.Component, application *applicationapiv1alpha1.Application, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		pipelineRun: pipelineRun,
		component:   component,
		application: application,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

// EnsureSnapshotExists is an operation that will ensure that a pipeline Snapshot associated
// to the build PipelineRun being processed exists. Otherwise, it will create a new pipeline Snapshot.
func (a *Adapter) EnsureSnapshotExists() (result controller.OperationResult, err error) {
	// a marker if we should remove finalizer from build PLR
	var canRemoveFinalizer bool

	defer func() {
		err = tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(a.context, a.pipelineRun, a.client, err)
		if err != nil {
			a.logger.Error(err, "Could not add create snapshot annotation to build pipelineRun", h.CreateSnapshotAnnotationName, a.pipelineRun)
			// requeue when err is not nil
			result, err = controller.RequeueWithError(err)
		}

		if canRemoveFinalizer {
			err = h.RemoveFinalizerFromPipelineRun(a.client, a.logger, a.context, a.pipelineRun, h.IntegrationPipelineRunFinalizer)
			if err != nil {
				a.logger.Error(err, "Failed to remove finalizer from build pipelineRun")
				// requeue when err is not nil
				result, err = controller.RequeueWithError(err)
			}
		}
	}()
	if !h.HasPipelineRunSucceeded(a.pipelineRun) {
		if h.HasPipelineRunFinished(a.pipelineRun) || a.pipelineRun.GetDeletionTimestamp() != nil {
			// The pipeline run has failed
			// OR pipeline has been deleted but it's still in running state (tekton bug/feature?)
			a.logger.Info("Finished processing of unsuccessful build PLR",
				"statusCondition", a.pipelineRun.GetStatusCondition(),
				"deletionTimestamp", a.pipelineRun.GetDeletionTimestamp(),
			)
			canRemoveFinalizer = true
			return controller.ContinueProcessing()
		}
		// The build pipeline run has not finished yet
		return controller.ContinueProcessing()
	}

	if _, found := a.pipelineRun.ObjectMeta.Annotations[tekton.SnapshotNameLabel]; found {
		a.logger.Info("The build pipelineRun is already associated with existing Snapshot via annotation",
			"snapshot.Name", a.pipelineRun.ObjectMeta.Annotations[tekton.SnapshotNameLabel])
		canRemoveFinalizer = true
		return controller.ContinueProcessing()
	}

	existingSnapshots, err := a.loader.GetAllSnapshotsForBuildPipelineRun(a.client, a.context, a.pipelineRun)
	if err != nil {
		a.logger.Error(err, "Failed to fetch Snapshots for the build pipelineRun")
		return controller.RequeueWithError(err)
	}
	if len(*existingSnapshots) > 0 {
		if len(*existingSnapshots) == 1 {
			existingSnapshot := (*existingSnapshots)[0]
			a.logger.Info("There is an existing Snapshot associated with this build pipelineRun, but the pipelineRun is not yet annotated",
				"snapshot.Name", existingSnapshot.Name)
			err := a.annotateBuildPipelineRunWithSnapshot(a.pipelineRun, &existingSnapshot)
			if err != nil {
				a.logger.Error(err, "Failed to update the build pipelineRun with snapshot name",
					"pipelineRun.Name", a.pipelineRun.Name)
				return controller.RequeueWithError(err)
			}
		} else {
			a.logger.Info("The build pipelineRun is already associated with more than one existing Snapshot")
		}
		canRemoveFinalizer = true
		return controller.ContinueProcessing()
	}

	expectedSnapshot, err := a.prepareSnapshotForPipelineRun(a.pipelineRun, a.component, a.application)
	if err != nil {
		// If PipelineRun result returns cusomized error update PLR annotation and exit
		if h.IsMissingInfoInPipelineRunError(err) || h.IsInvalidImageDigestError(err) || h.IsMissingValidComponentError(err) {
			// update the build PLR annotation with the error cusomized Reason and Value
			if annotateErr := tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(a.context, a.pipelineRun, a.client, err); annotateErr != nil {
				a.logger.Error(annotateErr, "Could not add create snapshot annotation to build pipelineRun", h.CreateSnapshotAnnotationName, a.pipelineRun)
			}
			a.logger.Error(err, "Build PipelineRun failed with error, should be fixed and re-run manually", "pipelineRun.Name", a.pipelineRun.Name)
			canRemoveFinalizer = true
			return controller.ContinueProcessing()
		}

		return controller.RequeueWithError(err)
	}

	err = a.client.Create(a.context, expectedSnapshot)
	if err != nil {
		a.logger.Error(err, "Failed to create Snapshot")
		if errors.IsForbidden(err) {
			// we cannot create a snapshot (possibly because the snapshot quota is hit) and we don't want to block resources, user has to retry
			canRemoveFinalizer = true
			return controller.StopProcessing()
		}
		return controller.RequeueWithError(err)
	}
	go metrics.RegisterNewSnapshot()

	a.logger.LogAuditEvent("Created new Snapshot", expectedSnapshot, h.LogActionAdd,
		"snapshot.Name", expectedSnapshot.Name,
		"snapshot.Spec.Components", expectedSnapshot.Spec.Components)

	err = a.annotateBuildPipelineRunWithSnapshot(a.pipelineRun, expectedSnapshot)
	if err != nil {
		a.logger.Error(err, "Failed to update the build pipelineRun with new annotations",
			"pipelineRun.Name", a.pipelineRun.Name)
		return controller.RequeueWithError(err)
	}

	canRemoveFinalizer = true
	return controller.ContinueProcessing()
}

func (a *Adapter) EnsurePipelineIsFinalized() (controller.OperationResult, error) {
	if h.HasPipelineRunFinished(a.pipelineRun) || controllerutil.ContainsFinalizer(a.pipelineRun, h.IntegrationPipelineRunFinalizer) {
		return controller.ContinueProcessing()
	}

	err := h.AddFinalizerToPipelineRun(a.client, a.logger, a.context, a.pipelineRun, h.IntegrationPipelineRunFinalizer)
	if err != nil {
		a.logger.Error(err, fmt.Sprintf("Could not add finalizer %s to build pipeline %s", h.IntegrationPipelineRunFinalizer, a.pipelineRun.Name))
		return controller.RequeueWithError(err)
	}

	return controller.ContinueProcessing()
}

// getImagePullSpecFromPipelineRun gets the full image pullspec from the given build PipelineRun,
// In case the Image pullspec can't be composed, an error will be returned.
func (a *Adapter) getImagePullSpecFromPipelineRun(pipelineRun *tektonv1.PipelineRun) (string, error) {
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

// getComponentSourceFromPipelineRun gets the component Git Source for the Component built in the given build PipelineRun,
// In case the Git Source can't be composed, an error will be returned.
func (a *Adapter) getComponentSourceFromPipelineRun(pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.ComponentSource, error) {
	componentSourceGitUrl, err := tekton.GetComponentSourceGitUrl(pipelineRun)
	if err != nil {
		return nil, err
	}
	componentSourceGitCommit, err := tekton.GetComponentSourceGitCommit(pipelineRun)
	if err != nil {
		return nil, err
	}
	componentSource := applicationapiv1alpha1.ComponentSource{
		ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
			GitSource: &applicationapiv1alpha1.GitSource{
				URL:      componentSourceGitUrl,
				Revision: componentSourceGitCommit,
			},
		},
	}

	return &componentSource, nil
}

// prepareSnapshotForPipelineRun prepares the Snapshot for a given PipelineRun,
// component and application. In case the Snapshot can't be created, an error will be returned.
func (a *Adapter) prepareSnapshotForPipelineRun(pipelineRun *tektonv1.PipelineRun, component *applicationapiv1alpha1.Component, application *applicationapiv1alpha1.Application) (*applicationapiv1alpha1.Snapshot, error) {
	newContainerImage, err := a.getImagePullSpecFromPipelineRun(pipelineRun)
	if err != nil {
		return nil, err
	}
	componentSource, err := a.getComponentSourceFromPipelineRun(pipelineRun)
	if err != nil {
		return nil, err
	}

	applicationComponents, err := a.loader.GetAllApplicationComponents(a.client, a.context, application)
	if err != nil {
		return nil, err
	}

	snapshot, err := gitops.PrepareSnapshot(a.client, a.context, application, applicationComponents, component, newContainerImage, componentSource)
	if err != nil {
		return nil, err
	}

	gitops.CopySnapshotLabelsAndAnnotation(application, snapshot, a.component.Name, &pipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix, false)

	snapshot.Labels[gitops.BuildPipelineRunNameLabel] = pipelineRun.Name
	if pipelineRun.Status.CompletionTime != nil {
		snapshot.Labels[gitops.BuildPipelineRunFinishTimeLabel] = strconv.FormatInt(pipelineRun.Status.CompletionTime.Time.Unix(), 10)
	} else {
		snapshot.Labels[gitops.BuildPipelineRunFinishTimeLabel] = strconv.FormatInt(time.Now().Unix(), 10)
	}

	return snapshot, nil
}

func (a *Adapter) annotateBuildPipelineRunWithSnapshot(pipelineRun *tektonv1.PipelineRun, snapshot *applicationapiv1alpha1.Snapshot) error {
	err := tekton.AnnotateBuildPipelineRun(a.context, pipelineRun, tekton.SnapshotNameLabel, snapshot.Name, a.client)
	if err == nil {
		a.logger.LogAuditEvent("Updated build pipelineRun", pipelineRun, h.LogActionUpdate,
			"snapshot.Name", snapshot.Name)
	}
	return err
}
