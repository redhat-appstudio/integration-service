/*
Copyright 2023.

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

package component

import (
	"context"

	"fmt"

	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	h "github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/integration-service/metrics"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Adapter holds the objects needed to reconcile a integration PipelineRun.
type Adapter struct {
	component   *applicationapiv1alpha1.Component
	application *applicationapiv1alpha1.Application
	loader      loader.ObjectLoader
	logger      h.IntegrationLogger
	client      client.Client
	context     context.Context
}

// NewAdapter creates and returns an Adapter instance.
func NewAdapter(component *applicationapiv1alpha1.Component, application *applicationapiv1alpha1.Application, logger h.IntegrationLogger, loader loader.ObjectLoader, client client.Client,
	context context.Context) *Adapter {
	return &Adapter{
		component:   component,
		application: application,
		logger:      logger,
		loader:      loader,
		client:      client,
		context:     context,
	}
}

// EnsureComponentHasFinalizer is an operation that will ensure components
// that have been created are assigned a finalizer
func (a *Adapter) EnsureComponentHasFinalizer() (controller.OperationResult, error) {
	if !isComponentMarkedForDeletion(a.component) {
		if !controllerutil.ContainsFinalizer(a.component, h.ComponentFinalizer) {
			err := h.AddFinalizerToComponent(a.client, a.logger, a.context, a.component, h.ComponentFinalizer)
			if err != nil {
				return controller.RequeueWithError(fmt.Errorf("failed to add the finalizer: %w", err))
			}
		}
	}

	return controller.ContinueProcessing()
}

// EnsureComponentIsCleanedUp is an operation that will ensure components
// marked for deletion have a snapshot created without said component
func (a *Adapter) EnsureComponentIsCleanedUp() (controller.OperationResult, error) {
	if !isComponentMarkedForDeletion(a.component) {
		return controller.ContinueProcessing()
	}

	applicationComponents, err := a.loader.GetAllApplicationComponents(a.client, a.context, a.application)
	if err != nil {
		a.logger.Error(err, "Failed to load application components")
		return controller.RequeueWithError(err)
	}

	var snapshotComponents []applicationapiv1alpha1.SnapshotComponent

	for _, individualComponent := range *applicationComponents {
		component := individualComponent
		if a.component.Name != component.Name {
			containerImage := component.Spec.ContainerImage
			componentSource := gitops.GetComponentSourceFromComponent(&component)
			snapshotComponents = append(snapshotComponents, applicationapiv1alpha1.SnapshotComponent{
				Name:           component.Name,
				ContainerImage: containerImage,
				Source:         *componentSource,
			})
		}
	}

	if len(snapshotComponents) != 0 {
		_, err = a.createUpdatedSnapshot(&snapshotComponents)
		if err != nil {
			a.logger.Error(err, "Failed to create new snapshot after component deletion")
			return controller.RequeueWithError(err)
		}
	}
	err = h.RemoveFinalizerFromComponent(a.client, a.logger, a.context, a.component, h.ComponentFinalizer)
	if err != nil {
		return controller.RequeueWithError(fmt.Errorf("failed to remove the finalizer: %w", err))
	}

	return controller.ContinueProcessing()
}

// createUpdatedSnapshot prepares a Snapshot for a given application and component(s).
// In case the Snapshot can't be created, an error will be returned.
func (a *Adapter) createUpdatedSnapshot(snapshotComponents *[]applicationapiv1alpha1.SnapshotComponent) (*applicationapiv1alpha1.Snapshot, error) {
	snapshot := gitops.NewSnapshot(a.application, snapshotComponents)
	if snapshot.Labels == nil {
		snapshot.Labels = map[string]string{}
	}
	snapshotType := gitops.SnapshotCompositeType
	if len(*snapshotComponents) == 1 {
		snapshotType = gitops.SnapshotComponentType
	}
	snapshot.Labels[gitops.SnapshotTypeLabel] = snapshotType

	err := ctrl.SetControllerReference(a.application, snapshot, a.client.Scheme())
	if err != nil {
		a.logger.Error(err, "Failed to set controller reference")
		return nil, err
	}

	err = a.client.Create(a.context, snapshot)
	if err != nil {
		a.logger.Error(err, "Failed to create new snapshot on client")
		return nil, err
	}

	go metrics.RegisterNewSnapshot()
	return snapshot, nil
}

func isComponentMarkedForDeletion(object client.Object) bool {
	if comp, ok := object.(*applicationapiv1alpha1.Component); ok {
		return !comp.ObjectMeta.DeletionTimestamp.IsZero()
	}
	return false
}

// hasComponentChangedToDeleting returns a boolean indicating whether the Component was marked for deletion.
// If the objects passed to this function is not a Component, the function will return false.
func hasComponentChangedToDeleting(objectOld, objectNew client.Object) bool {
	if oldComponent, ok := objectOld.(*applicationapiv1alpha1.Component); ok {
		if newComponent, ok := objectNew.(*applicationapiv1alpha1.Component); ok {
			return (oldComponent.GetDeletionTimestamp() == nil &&
				newComponent.GetDeletionTimestamp() != nil)
		}
	}

	return false
}
