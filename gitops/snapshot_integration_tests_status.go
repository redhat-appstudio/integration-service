/*
Copyright 2023 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions andF
limitations under the License.
*/

package gitops

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/konflux-ci/operator-toolkit/metadata"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewSnapshotIntegrationTestStatusesFromSnapshot creates new SnapshotTestStatus struct from snapshot annotation
func NewSnapshotIntegrationTestStatusesFromSnapshot(s *applicationapiv1alpha1.Snapshot) (*intgteststat.SnapshotIntegrationTestStatuses, error) {
	annotations := map[string]string{}
	if s.ObjectMeta.GetAnnotations() != nil {
		annotations = s.ObjectMeta.GetAnnotations()
	}
	statusAnnotation, ok := annotations[SnapshotTestsStatusAnnotation]
	if !ok {
		statusAnnotation = ""
	}
	sits, err := intgteststat.NewSnapshotIntegrationTestStatuses(statusAnnotation)
	if err != nil {
		return nil, fmt.Errorf("failed to get integration tests statuses from snapshot: %w", err)
	}

	return sits, nil
}

// WriteIntegrationTestStatusesIntoSnapshot writes data to snapshot by updating CR
// Data are written only when new changes are detected
func WriteIntegrationTestStatusesIntoSnapshot(s *applicationapiv1alpha1.Snapshot, sts *intgteststat.SnapshotIntegrationTestStatuses, c client.Client, ctx context.Context) error {
	if !sts.IsDirty() {
		// No updates were done, we don't need to update snapshot
		return nil
	}
	patch := client.MergeFrom(s.DeepCopy())

	value, err := json.Marshal(sts)
	if err != nil {
		return fmt.Errorf("failed to marshal test results into JSON: %w", err)
	}

	if err := metadata.SetAnnotation(&s.ObjectMeta, SnapshotTestsStatusAnnotation, string(value)); err != nil {
		return fmt.Errorf("failed to add annotations: %w", err)
	}

	err = c.Patch(ctx, s, patch)
	if err != nil {
		// don't return wrapped err, so we can use RetryOnConflict
		return err
	}
	sts.ResetDirty()
	return nil
}
