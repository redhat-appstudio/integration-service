/*
Copyright 2023.

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

package statusreport

import (
	"context"

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

// NewStatusReportReconciler creates and returns a Reconciler.
func NewStatusReportReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("statusreport"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshots,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=snapshots/status,verbs=get
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("snapshot", req.NamespacedName)}
	loader := loader.NewLoader()

	logger.Info("start to process snapshot test status since there is change in annotation test.appstudio.openshift.io/status", "snapshot", req.NamespacedName)

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

	application, err := loader.GetApplicationFromSnapshot(r.Client, ctx, snapshot)
	if err != nil {
		logger.Error(err, "Failed to get Application from the Snapshot")
		return ctrl.Result{}, err
	}
	logger = logger.WithApp(*application)

	adapter := NewAdapter(snapshot, application, logger, loader, r.Client, ctx)
	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureSnapshotFinishedAllTests,
		adapter.EnsureSnapshotTestStatusReportedToGitProvider,
	})
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureSnapshotTestStatusReportedToGitHub() (controller.OperationResult, error)
	EnsureSnapshotFinishedAllTests() (controller.OperationResult, error)
}

// SetupController creates a new Integration controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewStatusReportReconciler(manager.GetClient(), log, manager.GetScheme()))
}

// setupControllerWithManager sets up the controller with the Manager which monitors new Snapshots
func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&applicationapiv1alpha1.Snapshot{}).
		WithEventFilter(
			predicate.And(
				toolkitpredicates.IgnoreBackups{},
				gitops.SnapshotTestAnnotationChangePredicate(),
			)).
		Complete(controller)
}
