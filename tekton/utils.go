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

package tekton

import (
	"fmt"

	"github.com/konflux-ci/operator-toolkit/metadata"
	h "github.com/redhat-appstudio/integration-service/helpers"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// PipelineRunTypeLabel contains the type of the PipelineRunTypeLabel.
	PipelineRunTypeLabel = "pipelines.appstudio.openshift.io/type"

	// PipelineRunBuildType is the type denoting a build PipelineRun.
	PipelineRunBuildType = "build"

	// PipelineRunTestType is the type denoting a test PipelineRun.
	PipelineRunTestType = "test"

	// PipelineRunComponentLabel is the label denoting the application.
	PipelineRunComponentLabel = "appstudio.openshift.io/component"

	// PipelineRunApplicationLabel is the label denoting the application.
	PipelineRunApplicationLabel = "appstudio.openshift.io/application"

	// PipelineRunChainsSignedAnnotation is the label added by Tekton Chains to signed PipelineRuns
	PipelineRunChainsSignedAnnotation = "chains.tekton.dev/signed"

	// PipelineRunImageUrlParamName name of image url output param
	PipelineRunImageUrlParamName = "IMAGE_URL"

	// PipelineRunImageDigestParamName name of image digest in PipelineRun result param
	PipelineRunImageDigestParamName = "IMAGE_DIGEST"

	// PipelineRunChainsGitUrlParamName name of param chains repo url
	PipelineRunChainsGitUrlParamName = "CHAINS-GIT_URL"

	// PipelineRunChainsGitCommitParamName name of param repo chains commit
	PipelineRunChainsGitCommitParamName = "CHAINS-GIT_COMMIT"
)

// IsBuildPipelineRun returns a boolean indicating whether the object passed is a PipelineRun from
// the Build service or not.
func IsBuildPipelineRun(object client.Object) bool {
	if pipelineRun, ok := object.(*tektonv1.PipelineRun); ok {
		return metadata.HasLabelWithValue(pipelineRun,
			PipelineRunTypeLabel,
			PipelineRunBuildType)
	}

	return false
}

// IsIntegrationPipelineRun returns a boolean indicating whether the object passed is an Integration
// Component PipelineRun
func IsIntegrationPipelineRun(object client.Object) bool {
	if pipelineRun, ok := object.(*tektonv1.PipelineRun); ok {
		return metadata.HasLabelWithValue(pipelineRun,
			PipelineRunTypeLabel,
			PipelineRunTestType)
	}

	return false
}

// hasPipelineRunStateChangedToFinished returns a boolean indicating whether the PipelineRun status changed to finished or not.
// If the objects passed to this function are not PipelineRuns, the function will return false.
func hasPipelineRunStateChangedToFinished(objectOld, objectNew client.Object) bool {
	if oldPipelineRun, ok := objectOld.(*tektonv1.PipelineRun); ok {
		if newPipelineRun, ok := objectNew.(*tektonv1.PipelineRun); ok {
			return oldPipelineRun.Status.GetCondition(apis.ConditionSucceeded).IsUnknown() && !newPipelineRun.Status.GetCondition(apis.ConditionSucceeded).IsUnknown()
		}
	}

	return false
}

// hasPipelineRunStateChangedToStarted returns a boolean indicating whether the PipelineRun just started.
// If the objects passed to this function are not PipelineRuns, the function will return false.
func hasPipelineRunStateChangedToStarted(objectOld, objectNew client.Object) bool {
	if oldPipelineRun, ok := objectOld.(*tektonv1.PipelineRun); ok {
		if newPipelineRun, ok := objectNew.(*tektonv1.PipelineRun); ok {
			return (oldPipelineRun.Status.StartTime == nil || oldPipelineRun.Status.StartTime.IsZero()) &&
				(newPipelineRun.Status.StartTime != nil && !newPipelineRun.Status.StartTime.IsZero())
		}
	}

	return false
}

// hasPipelineRunStateChangedToDeleting returns a boolean indicating whether the PipelineRun just got marked for deletion or not.
// If the objects passed to this function are not PipelineRuns, the function will return false.
func hasPipelineRunStateChangedToDeleting(objectOld, objectNew client.Object) bool {
	if oldPipelineRun, ok := objectOld.(*tektonv1.PipelineRun); ok {
		if newPipelineRun, ok := objectNew.(*tektonv1.PipelineRun); ok {
			return (oldPipelineRun.GetDeletionTimestamp() == nil &&
				newPipelineRun.GetDeletionTimestamp() != nil)
		}
	}

	return false
}

// isChainsDoneWithPipelineRun returns a boolean indicating whether Tekton Chains is done processing
// the PipelineRun. true is returned regardless if Chains was able to successfully sign/attest the
// artifacts produced by the PipelineRun. If the object passed to this function is not a
// PipelineRun, the function will return false.
func isChainsDoneWithPipelineRun(objectNew client.Object) bool {
	if newPipelineRun, ok := objectNew.(*tektonv1.PipelineRun); ok {
		// If the annotation value is set "true" it means Chains was able to sign. If it is set to
		// "failed", it means something prevented Chains from signing, e.g. insufficient access. In
		// either case, we want to proceed processing the PipelineRun instead of waiting
		// indefinitely for a condition that may never happen, e.g. "failed" -> "true". Let
		// downstream verification processes, e.g. Enterprise Contract, deal with the Chains
		// failure.
		return metadata.HasAnnotation(newPipelineRun, PipelineRunChainsSignedAnnotation)
	}
	return false
}

// GetTypeFromPipelineRun extracts the pipeline type from the pipelineRun labels.
func GetTypeFromPipelineRun(object client.Object) (string, error) {
	if pipelineRun, ok := object.(*tektonv1.PipelineRun); ok {
		if pipelineType, found := pipelineRun.Labels[PipelineRunTypeLabel]; found {
			return pipelineType, nil
		}
	}
	return "", fmt.Errorf("the pipelineRun has no type associated with it")
}

// GetOutputImage returns a string containing the output-image parameter value from a given PipelineRun.
func GetOutputImage(object client.Object) (string, error) {
	pipelineRun, ok := object.(*tektonv1.PipelineRun)
	if ok {
		for _, pipelineResult := range pipelineRun.Status.Results {
			if pipelineResult.Name == PipelineRunImageUrlParamName {
				return pipelineResult.Value.StringVal, nil
			}
		}
	}

	return "", h.MissingInfoInPipelineRunError(pipelineRun.Name, PipelineRunImageUrlParamName)
}

// GetOutputImageDigest returns a string containing the IMAGE_DIGEST result value from a given PipelineRun.
func GetOutputImageDigest(object client.Object) (string, error) {
	pipelineRun, ok := object.(*tektonv1.PipelineRun)
	if ok {
		for _, pipelineResult := range pipelineRun.Status.Results {
			if pipelineResult.Name == PipelineRunImageDigestParamName {
				return pipelineResult.Value.StringVal, nil
			}
		}
	}
	return "", h.MissingInfoInPipelineRunError(pipelineRun.Name, PipelineRunImageDigestParamName)
}

// GetComponentSourceGitUrl returns a string containing the CHAINS-GIT_URL result value from a given PipelineRun.
func GetComponentSourceGitUrl(object client.Object) (string, error) {
	pipelineRun, ok := object.(*tektonv1.PipelineRun)
	if ok {
		for _, pipelineResult := range pipelineRun.Status.Results {
			if pipelineResult.Name == PipelineRunChainsGitUrlParamName {
				return pipelineResult.Value.StringVal, nil
			}
		}
	}
	return "", h.MissingInfoInPipelineRunError(pipelineRun.Name, PipelineRunChainsGitUrlParamName)
}

// GetComponentSourceGitCommit returns a string containing the CHAINS-GIT_COMMIT result value from a given PipelineRun.
func GetComponentSourceGitCommit(object client.Object) (string, error) {
	pipelineRun, ok := object.(*tektonv1.PipelineRun)
	if ok {
		for _, pipelineResult := range pipelineRun.Status.Results {
			if pipelineResult.Name == PipelineRunChainsGitCommitParamName {
				return pipelineResult.Value.StringVal, nil
			}
		}
	}
	return "", h.MissingInfoInPipelineRunError(pipelineRun.Name, PipelineRunChainsGitCommitParamName)
}
