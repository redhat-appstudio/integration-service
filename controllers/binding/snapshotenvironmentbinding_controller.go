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

package binding

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/integration-service/tekton"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Reconciler reconciles a SnapshotEnvironmentBinding object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewBindingReconciler creates and returns a Reconciler.
func NewBindingReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("binding"),
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
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargets,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargetclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("snapshotEnvironmentBinding", req.NamespacedName)}
	loader := loader.NewLoader()

	snapshotEnvironmentBinding := &applicationapiv1alpha1.SnapshotEnvironmentBinding{}
	err := r.Get(ctx, req.NamespacedName, snapshotEnvironmentBinding)
	if err != nil {
		logger.Error(err, "Failed to get snapshotEnvironmentBinding for", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	if snapshotEnvironmentBinding.DeletionTimestamp != nil {
		logger.Info("SnapshotEnvironmentBinding is being deleted. Skipping reconciliation")
		return ctrl.Result{}, nil
	}

	var application *applicationapiv1alpha1.Application
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		application, err = r.getApplicationFromSnapshotEnvironmentBinding(ctx, snapshotEnvironmentBinding)
		return err
	})
	if err != nil {
		return helpers.HandleLoaderError(logger, err, "Application", "SnapshotEnvironmentBinding")
	}
	logger = logger.WithApp(*application)

	var snapshot *applicationapiv1alpha1.Snapshot
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		snapshot, err = r.getSnapshotFromSnapshotEnvironmentBinding(ctx, snapshotEnvironmentBinding)
		return err
	})
	if err != nil {
		return helpers.HandleLoaderError(logger, err, "Snapshot", "SnapshotEnvironmentBinding")
	}

	var component *applicationapiv1alpha1.Component
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		component, err = loader.GetComponentFromSnapshot(r.Client, ctx, snapshot)
		return err
	})
	if err != nil {
		return helpers.HandleLoaderError(logger, err, fmt.Sprintf("Component or '%s' label", tekton.ComponentNameLabel), "Snapshot")
	}

	environment, err := r.getEnvironmentFromSnapshotEnvironmentBinding(ctx, snapshotEnvironmentBinding)
	if err != nil {
		logger.Error(err, "Failed to get Environment from the SnapshotEnvironmentBinding")
		return ctrl.Result{}, err
	}

	integrationTestScenario, err := r.getIntegrationTestScenarioFromSnapshotEnvironmentBinding(ctx, snapshotEnvironmentBinding)
	if err != nil {
		logger.Error(err, "Failed to get IntegrationTestScenario from the SnapshotEnvironmentBinding")
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	adapter := NewAdapter(snapshotEnvironmentBinding, snapshot, environment, application, component, integrationTestScenario, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureIntegrationTestPipelineForScenarioExists,
		adapter.EnsureEphemeralEnvironmentsCleanedUp,
	})
}

// getApplicationFromSnapshotEnvironmentBinding loads from the cluster the Application referenced in the given SnapshotEnvironmentBinding.
// If the SnapshotEnvironmentBinding doesn't specify an Application or this is not found in the cluster, an error will be returned.
func (r *Reconciler) getApplicationFromSnapshotEnvironmentBinding(context context.Context, snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding) (*applicationapiv1alpha1.Application, error) {
	application := &applicationapiv1alpha1.Application{}
	err := r.Get(context, types.NamespacedName{
		Namespace: snapshotEnvironmentBinding.Namespace,
		Name:      snapshotEnvironmentBinding.Spec.Application,
	}, application)

	if err != nil {
		return nil, err
	}

	return application, nil
}

// getSnapshotFromSnapshotEnvironmentBinding loads from the cluster the Snapshot referenced in the given SnapshotEnvironmentBinding.
// If the SnapshotEnvironmentBinding doesn't specify a Snapshot or this is not found in the cluster, an error will be returned.
func (r *Reconciler) getSnapshotFromSnapshotEnvironmentBinding(context context.Context, snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding) (*applicationapiv1alpha1.Snapshot, error) {
	snapshot := &applicationapiv1alpha1.Snapshot{}
	err := r.Get(context, types.NamespacedName{
		Namespace: snapshotEnvironmentBinding.Namespace,
		Name:      snapshotEnvironmentBinding.Spec.Snapshot,
	}, snapshot)

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// getEnvironmentFromSnapshotEnvironmentBinding loads from the cluster the Environment referenced in the given SnapshotEnvironmentBinding.
// If the SnapshotEnvironmentBinding doesn't specify an Environment or this is not found in the cluster, an error will be returned.
func (r *Reconciler) getEnvironmentFromSnapshotEnvironmentBinding(context context.Context, snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding) (*applicationapiv1alpha1.Environment, error) {
	environment := &applicationapiv1alpha1.Environment{}
	err := r.Get(context, types.NamespacedName{
		Namespace: snapshotEnvironmentBinding.Namespace,
		Name:      snapshotEnvironmentBinding.Spec.Environment,
	}, environment)

	if err != nil {
		return nil, err
	}

	return environment, nil
}

// getIntegrationTestScenarioFromSnapshotEnvironmentBinding loads from the cluster the IntegrationTestScenario referenced in the given SnapshotEnvironmentBinding.
// If the SnapshotEnvironmentBinding doesn't specify an IntegrationTestScenario or this is not found in the cluster, an error will be returned.
func (r *Reconciler) getIntegrationTestScenarioFromSnapshotEnvironmentBinding(context context.Context, snapshotEnvironmentBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding) (*v1beta1.IntegrationTestScenario, error) {
	if scenarioLabel, ok := snapshotEnvironmentBinding.Labels[gitops.SnapshotTestScenarioLabel]; ok {
		integrationTestScenario := &v1beta1.IntegrationTestScenario{}
		err := r.Get(context, types.NamespacedName{
			Namespace: snapshotEnvironmentBinding.Namespace,
			Name:      scenarioLabel,
		}, integrationTestScenario)

		if err != nil {
			return nil, err
		}

		return integrationTestScenario, nil
	} else {
		return nil, nil
	}
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureIntegrationTestPipelineForScenarioExists() (controller.OperationResult, error)
	EnsureEphemeralEnvironmentsCleanedUp() (controller.OperationResult, error)
}

// SetupController creates a new Integration reconciler and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewBindingReconciler(manager.GetClient(), log, manager.GetScheme()))
}

// setupControllerWithManager sets up the controller with the Manager which monitors new SnapshotEnvironmentBindings
func setupControllerWithManager(manager ctrl.Manager, reconciler *Reconciler) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&applicationapiv1alpha1.SnapshotEnvironmentBinding{}).
		WithEventFilter(predicate.And(gitops.IntegrationSnapshotEnvironmentBindingPredicate(), predicate.Or(
			gitops.DeploymentSucceededForIntegrationBindingPredicate(), gitops.DeploymentFailedForIntegrationBindingPredicate()))).
		Complete(reconciler)
}
