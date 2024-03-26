/*
Copyright 2022 Red Hat Inc.

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

package snapshot

import (
	"context"
	"fmt"

	"github.com/redhat-appstudio/integration-service/cache"
	"github.com/redhat-appstudio/integration-service/tekton"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	toolkitpredicates "github.com/konflux-ci/operator-toolkit/predicates"
	toolkitutils "github.com/konflux-ci/operator-toolkit/utils"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Reconciler reconciles an Snapshot object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewSnapshotReconciler creates and returns a Reconciler.
func NewSnapshotReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("snapshot"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshots/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshotenvironmentbindings,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshotenvironmentbindings/status,verbs=get
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=components,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=components/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargetclasses,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargetclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=releases/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=releases,verbs=create;delete;get;list;patch;update;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=releaseplans,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=releaseplans/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("snapshot", req.NamespacedName)}
	loader := loader.NewLoader()

	snapshot := &applicationapiv1alpha1.Snapshot{}
	err := r.Get(ctx, req.NamespacedName, snapshot)
	if err != nil {
		logger.Error(err, "Failed to get snapshot for", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	if toolkitutils.IsObjectRestoredFromBackup(snapshot) {
		logger.Info("Snapshot restored from backup has been updated, we cannot reconcile it, skipping.")
		return ctrl.Result{}, nil
	}

	var application *applicationapiv1alpha1.Application
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		application, err = loader.GetApplicationFromSnapshot(r.Client, ctx, snapshot)
		return err
	})
	if err != nil {
		if errors.IsNotFound(err) {
			if !gitops.IsSnapshotMarkedAsInvalid(snapshot) {
				err := gitops.MarkSnapshotAsInvalid(r.Client, ctx, snapshot,
					fmt.Sprintf("The application %s owning this snapshot doesn't exist, try again after creating application", snapshot.Spec.Application))
				if err != nil {
					logger.Error(err, "Failed to update the status to Invalid for the snapshot",
						"snapshot.Namespace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
					return ctrl.Result{}, err
				}
				logger.Info("Snapshot integration status condition marked as invalid, the application owning this snapshot cannot be found",
					"snapshot.Namespace", snapshot.Namespace, "snapshot.Name", snapshot.Name)
			}
		}
		return helpers.HandleLoaderError(logger, err, "Application", "Snapshot")
	}

	logger = logger.WithApp(*application)

	var component *applicationapiv1alpha1.Component
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		component, err = loader.GetComponentFromSnapshot(r.Client, ctx, snapshot)
		return err
	})
	if err != nil {
		return helpers.HandleLoaderError(logger, err, fmt.Sprintf("Component or '%s' label", tekton.ComponentNameLabel), "Snapshot")
	}

	adapter := NewAdapter(snapshot, application, component, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureAllReleasesExist,
		adapter.EnsureGlobalCandidateImageUpdated,
		adapter.EnsureRerunPipelineRunsExist,
		adapter.EnsureSnapshotEnvironmentBindingExist,
		adapter.EnsureCreationOfEphemeralEnvironments,
		adapter.EnsureStaticIntegrationPipelineRunsExist,
	})
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureAllReleasesExist() (controller.OperationResult, error)
	EnsureRerunPipelineRunsExist() (controller.OperationResult, error)
	EnsureCreationOfEnvironment() (controller.OperationResult, error)
	EnsureAllIntegrationTestPipelinesExist() (controller.OperationResult, error)
	EnsureGlobalCandidateImageUpdated() (controller.OperationResult, error)
	EnsureSnapshotEnvironmentBindingExist() (controller.OperationResult, error)
}

// SetupController creates a new Integration controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewSnapshotReconciler(manager.GetClient(), log, manager.GetScheme()))
}

// setupCache indexes fields for each of the resources used in the release adapter in those cases where filtering by
// field is required.
func setupCache(mgr ctrl.Manager) error {
	if err := cache.SetupReleasePlanCache(mgr); err != nil {
		return err
	}

	if err := cache.SetupReleaseCache(mgr); err != nil {
		return err
	}

	if err := cache.SetupBindingEnvironmentCache(mgr); err != nil {
		return err
	}

	return cache.SetupBindingApplicationCache(mgr)
}

// setupControllerWithManager sets up the controller with the Manager which monitors new Snapshots
func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {
	err := setupCache(manager)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(manager).
		For(&applicationapiv1alpha1.Snapshot{}).
		WithEventFilter(
			predicate.And(
				toolkitpredicates.IgnoreBackups{},
				predicate.Or(
					gitops.IntegrationSnapshotChangePredicate(),
					gitops.SnapshotIntegrationTestRerunTriggerPredicate(),
				),
			),
		).
		Complete(controller)
}
