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

package component

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/controller"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Reconciler reconciles a component object
type Reconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

// NewComponentReconciler creates and returns a Reconciler.
func NewComponentReconciler(client client.Client, logger *logr.Logger, scheme *runtime.Scheme) *Reconciler {
	return &Reconciler{
		Client: client,
		Log:    logger.WithName("component"),
		Scheme: scheme,
	}
}

//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=components,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=components/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=appstudio.redhat.com,resources=components/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := helpers.IntegrationLogger{Logger: r.Log.WithValues("component", req.NamespacedName)}
	loader := loader.NewLoader()

	component := &applicationapiv1alpha1.Component{}
	err := r.Get(ctx, req.NamespacedName, component)
	if err != nil {
		logger.Error(err, "Failed to get component for", "req", req.NamespacedName)
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	var application *applicationapiv1alpha1.Application
	application, err = loader.GetApplicationFromComponent(r.Client, ctx, component)
	if err != nil {
		if errors.IsNotFound(err) {
			if err := helpers.RemoveFinalizerFromComponent(r.Client, logger, ctx, component, helpers.ComponentFinalizer); err != nil {
				return ctrl.Result{}, err
			}
		}
		return helpers.HandleLoaderError(logger, err, "Application", "Component")
	}

	if application == nil {
		err := fmt.Errorf("failed to get Application")
		logger.Error(err, "reconcile cannot resolve application")
		return ctrl.Result{}, err
	}
	logger = logger.WithApp(*application)

	adapter := NewAdapter(component, application, logger, loader, r.Client, ctx)

	return controller.ReconcileHandler([]controller.Operation{
		adapter.EnsureComponentHasFinalizer,
		adapter.EnsureComponentIsCleanedUp,
	})
}

// AdapterInterface is an interface defining all the operations that should be defined in an Integration adapter.
type AdapterInterface interface {
	EnsureComponentHasFinalizer() (controller.OperationResult, error)
	EnsureComponentIsCleanedUp() (controller.OperationResult, error)
}

// SetupController creates a new Component controller and adds it to the Manager.
func SetupController(manager ctrl.Manager, log *logr.Logger) error {
	return setupControllerWithManager(manager, NewComponentReconciler(manager.GetClient(), log, manager.GetScheme()))

}

// setupControllerWithManager sets up the controller with the Manager which monitors Components and filters
// out status updates.
func setupControllerWithManager(manager ctrl.Manager, controller *Reconciler) error {
	return ctrl.NewControllerManagedBy(manager).
		For(&applicationapiv1alpha1.Component{}).
		WithEventFilter(predicate.Or(
			ComponentCreatedPredicate(),
			ComponentDeletedPredicate())).
		Complete(controller)
}
