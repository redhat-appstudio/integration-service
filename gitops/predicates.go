/*
Copyright 2023 Red Hat Inc.

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

package gitops

import (
	"github.com/konflux-ci/operator-toolkit/metadata"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DeploymentSucceededForIntegrationBindingPredicate returns a predicate which filters out update events to a
// SnapshotEnvironmentBinding whose component deployment status goes from unknown/false to true.
func DeploymentSucceededForIntegrationBindingPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(genericEvent event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return hasDeploymentSucceeded(e.ObjectOld, e.ObjectNew)
		},
	}
}

// DeploymentFailedForIntegrationBindingPredicate returns a predicate which filters out update events to a
// SnapshotEnvironmentBinding that have resulted in a deployment failure.
func DeploymentFailedForIntegrationBindingPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(genericEvent event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return hasDeploymentFailed(e.ObjectOld, e.ObjectNew)
		},
	}
}

// IntegrationSnapshotEnvironmentBindingPredicate returns a predicate which filters out update events to a
// SnapshotEnvironmentBinding associated with an IntegrationTestScenario.
func IntegrationSnapshotEnvironmentBindingPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			return false
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(genericEvent event.GenericEvent) bool {
			return false
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return metadata.HasLabel(e.ObjectNew, SnapshotTestScenarioLabel)
		},
	}
}
