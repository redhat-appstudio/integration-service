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

package scenario

import (
	"context"

	"github.com/redhat-appstudio/integration-service/loader"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/helpers"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reconciler reconciles an scenario object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewScenarioReconciler creates and returns a Reconciler.
func NewScenarioReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("integrationTestScenario"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=integrationtestscenarios,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=integrationtestscenarios/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments,verbs=get;list;watch;
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=environments/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=applications/status,verbs=get

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("integrationTestScenario", req.NamespacedName)}
	loader := loader.NewLoader()

	scenario := &v1beta1.IntegrationTestScenario{}
	err := r.Get(ctx, req.NamespacedName, scenario)
	if err != nil {
		logger.Error(err, "Failed to get IntegrationTestScenario from request", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	application, err := r.getApplicationFromScenario(ctx, scenario)
	if err != nil {
		logger.Info("Failed to get Application from the IntegrationTestScenario", "error:", err)
	}

	adapter := NewAdapter(application, scenario, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureCreatedScenarioIsValid,
		adapter.EnsureDeletedScenarioResourcesAreCleanedUp,
	})
}

// getApplicationFromScenario loads from the cluster the Application referenced in the given scenario.
// If the scenario doesn't specify an application or this is not found in the cluster, an error will be returned.
func (r *Reconciler) getApplicationFromScenario(context context.Context, scenario *v1beta1.IntegrationTestScenario) (*applicationapiv1alpha1.Application, error) {
	application := &applicationapiv1alpha1.Application{}
	err := r.Get(context, types.NamespacedName{
		Namespace: scenario.Namespace,
		Name:      scenario.Spec.Application,
	}, application)

	if err != nil {
		return nil, err
	}

	return application, nil
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureCreatedScenarioIsValid() (controller.OperationResult, error)
	EnsureDeletedScenarioResourcesAreCleanedUp() (controller.OperationResult, error)
}

// SetupController creates a new Integration controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewScenarioReconciler(manager.GetClient(), log, manager.GetScheme()))
}

func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {

	return ctrl.NewControllerManagedBy(manager).
		For(&v1beta1.IntegrationTestScenario{}).
		Complete(controller)
}
