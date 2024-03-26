/*
Copyright 2024 Red Hat Inc.

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
	"context"
	"encoding/json"
	"fmt"

	"github.com/konflux-ci/operator-toolkit/metadata"
	h "github.com/redhat-appstudio/integration-service/helpers"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AnnotateBuildPipelineRun sets annotation for a build pipelineRun in defined context and returns that pipeline
func AnnotateBuildPipelineRun(ctx context.Context, pipelineRun *tektonv1.PipelineRun, key, value string, cl client.Client) error {
	patch := client.MergeFrom(pipelineRun.DeepCopy())

	_ = metadata.SetAnnotation(&pipelineRun.ObjectMeta, key, value)

	err := cl.Patch(ctx, pipelineRun, patch)
	if err != nil {
		return err
	}
	return nil
}

// AnnotateBuildPipelineRunWithCreateSnapshotAnnotation sets annotation test.appstudio.openshift.io/create-snapshot-status to build pipelineRun with
// a message that reflects either success or failure for creating a snapshot
func AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(ctx context.Context, pipelineRun *tektonv1.PipelineRun, cl client.Client, ensureSnapshotExistsErr error) error {
	message := ""
	status := ""
	if ensureSnapshotExistsErr == nil {
		if !metadata.HasAnnotation(pipelineRun, SnapshotNameLabel) {
			// do nothing for in progress build PLR
			return nil
		}
		message = fmt.Sprintf("Sucessfully created snapshot. See annotation %s for name", SnapshotNameLabel)
		status = "success"
	} else {
		message = fmt.Sprintf("Failed to create snapshot. Error: %s", ensureSnapshotExistsErr.Error())
		status = "failed"
	}

	jsonResult, err := json.Marshal(map[string]string{
		"status":  status,
		"message": message,
	})
	if err != nil {
		return err
	}
	err = AnnotateBuildPipelineRun(ctx, pipelineRun, h.CreateSnapshotAnnotationName, string(jsonResult), cl)
	return err
}
