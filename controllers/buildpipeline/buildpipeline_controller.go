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

package buildpipeline

import (
	"context"

	"github.com/redhat-appstudio/integration-service/cache"
	"k8s.io/client-go/util/retry"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
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

// Reconciler reconciles a build PipelineRun object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewIntegrationReconciler creates and returns a Reconciler.
func NewIntegrationReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("build pipeline"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete;deletecollection
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=tekton.dev,resources=pipelineruns/finalizers,verbs=update
//+kubebuilder:rbac:groups=tekton.dev,resources=taskruns,verbs=get;list;watch
//+kubebuilder:rbac:groups=tekton.dev,resources=taskruns/status,verbs=get
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/finalizers,verbs=update
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups=pipelinesascode.tekton.dev,resources=repositories,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("buildpipelineRun", req.NamespacedName)}
	loader := loader.NewLoader()

	pipelineRun := &tektonv1.PipelineRun{}
	err := r.Get(ctx, req.NamespacedName, pipelineRun)
	if err != nil {
		logger.Error(err, "Failed to get build pipelineRun for", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	var component *applicationapiv1alpha1.Component
	err = retry.OnError(retry.DefaultRetry, func(_ error) bool { return true }, func() error {
		component, err = loader.GetComponentFromPipelineRun(r.Client, ctx, pipelineRun)
		return err
	})
	if err != nil {
		if errors.IsNotFound(err) {
			if err := helpers.RemoveFinalizerFromPipelineRun(r.Client, logger, ctx, pipelineRun, helpers.IntegrationPipelineRunFinalizer); err != nil {
				return ctrl.Result{}, err
			}
		}
		return helpers.HandleLoaderError(logger, err, "component", "pipelineRun")
	} else if component == nil {
		// if both component and error are nil then the component label for the pipeline did not exist
		// in this case we should stop reconciliation
		logger.Info("Failed to  get component for build pipeline - component label does not exist", "name", pipelineRun.Name, "namespace", pipelineRun.Namespace)
		return ctrl.Result{}, nil
	}

	application, err := loader.GetApplicationFromComponent(r.Client, ctx, component)
	if err != nil {
		logger.Error(err, "Failed to get Application from Component",
			"Component.Name ", component.Name, "Component.Namespace ", component.Namespace)
		tknErr := tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(ctx, pipelineRun, r.Client, err)
		if tknErr != nil {
			return ctrl.Result{}, tknErr
		}
		return helpers.HandleLoaderError(logger, err, "application", "component")

	}

	logger = logger.WithApp(*application)

	adapter := NewAdapter(pipelineRun, component, application, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsurePipelineIsFinalized,
		adapter.EnsureSnapshotExists,
	})
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsurePipelineIsFinalized() (controller.OperationResult, error)
	EnsureSnapshotExists() (controller.OperationResult, error)
}

// SetupController creates a new Integration controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewIntegrationReconciler(manager.GetClient(), log, manager.GetScheme()))
}

// setupCache indexes fields for each of the resources used in the build pipeline adapter in those cases where
// filtering by field is required.
func setupCache(mgr ctrl.Manager) error {
	return cache.SetupApplicationComponentCache(mgr)
}

// setupControllerWithManager sets up the controller with the Manager which monitors new build PipelineRuns and filters
// out status updates.
func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {
	err := setupCache(manager)
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(manager).
		For(&tektonv1.PipelineRun{}).
		WithEventFilter(predicate.Or(
			tekton.BuildPipelineRunSignedAndSucceededPredicate(),
			tekton.BuildPipelineRunFailedPredicate(),
			tekton.BuildPipelineRunCreatedPredicate(),
			tekton.BuildPipelineRunDeletingPredicate(),
		)).
		Complete(controller)
}
