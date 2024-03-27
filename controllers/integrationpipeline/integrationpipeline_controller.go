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

package integrationpipeline

import (
	"context"
	"fmt"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/cache"
	"k8s.io/client-go/util/retry"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"github.com/redhat-appstudio/integration-service/tekton"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Reconciler reconciles an integration PipelineRun object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewIntegrationReconciler creates and returns a Reconciler.
func NewIntegrationReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("integration pipeline"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargetclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=deploymenttargets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/finalizers,verbs=update
//+kubebuilder:rbac:groups=tekton.dev,resources=taskruns,verbs=get;list;watch
//+kubebuilder:rbac:groups=tekton.dev,resources=taskruns/status,verbs=get
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=pipelinesascode.tekton.dev,resources=repositories,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("pipelineRun", req.NamespacedName)}
	loader := loader.NewLoader()

	pipelineRun := &tektonv1.PipelineRun{}
	err := r.Get(ctx, req.NamespacedName, pipelineRun)
	if err != nil {
		logger.Error(err, "Failed to get integration pipelineRun for", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	var snapshot *applicationapiv1alpha1.Snapshot
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		snapshot, err = loader.GetSnapshotFromPipelineRun(r.Client, ctx, pipelineRun)
		return err
	})
	if err != nil {
		if errors.IsNotFound(err) {
			if err := helpers.RemoveFinalizerFromPipelineRun(r.Client, logger, ctx, pipelineRun, helpers.IntegrationPipelineRunFinalizer); err != nil {
				return ctrl.Result{}, err
			}
		}
		return helpers.HandleLoaderError(logger, err, "Snapshot", "PipelineRun")
	}

	application, err := loader.GetApplicationFromPipelineRun(r.Client, ctx, pipelineRun)
	if err != nil {
		logger.Error(err, "Failed to get Application from the integration pipelineRun",
			"PipelineRun.Name", pipelineRun.Name, "PipelineRun.Namespace", pipelineRun.Namespace)
		return ctrl.Result{}, err
	}

	if application == nil {
		err := fmt.Errorf("failed to get Application")
		logger.Error(err, "reconcile cannot resolve application")
		return ctrl.Result{}, err
	}
	logger = logger.WithApp(*application)

	adapter := NewAdapter(pipelineRun, application, snapshot, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureStatusReportedInSnapshot,
		adapter.EnsureEphemeralEnvironmentsCleanedUp,
	})
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureStatusReportedInSnapshot() (controller.OperationResult, error)
	EnsureEphemeralEnvironmentsCleanedUp() (controller.OperationResult, error)
}

// SetupController creates a new Integration controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewIntegrationReconciler(manager.GetClient(), log, manager.GetScheme()))
}

// setupCache indexes fields for each of the resources used in the pipeline adapter in those cases where filtering by
// field is required.
func setupCache(mgr ctrl.Manager) error {
	if err := cache.SetupSnapshotCache(mgr); err != nil {
		return err
	}

	return cache.SetupIntegrationTestScenarioCache(mgr)
}

// setupControllerWithManager sets up the controller with the Manager which monitors new PipelineRuns and filters
// out status updates.
func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {
	err := setupCache(manager)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(manager).
		For(&tektonv1.PipelineRun{}).
		WithEventFilter(predicate.Or(
			tekton.IntegrationPipelineRunPredicate())).
		Complete(controller)
}
