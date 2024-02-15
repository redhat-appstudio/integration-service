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

// Package loader contains functions used to load resource from the cluster
package loader

import (
	"context"
	"fmt"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/tekton"
	toolkit "github.com/redhat-appstudio/operator-toolkit/loader"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ObjectLoader interface {
	GetAllEnvironments(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Environment, error)
	GetReleasesWithSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*[]releasev1alpha1.Release, error)
	GetAllApplicationComponents(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Component, error)
	GetApplicationFromSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*applicationapiv1alpha1.Application, error)
	GetComponentFromSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*applicationapiv1alpha1.Component, error)
	GetComponentFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Component, error)
	GetApplicationFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Application, error)
	GetApplicationFromComponent(c client.Client, ctx context.Context, component *applicationapiv1alpha1.Component) (*applicationapiv1alpha1.Application, error)
	GetEnvironmentFromIntegrationPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Environment, error)
	GetSnapshotFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Snapshot, error)
	GetAllIntegrationTestScenariosForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]v1beta1.IntegrationTestScenario, error)
	GetRequiredIntegrationTestScenariosForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]v1beta1.IntegrationTestScenario, error)
	GetDeploymentTargetClaimForEnvironment(c client.Client, ctx context.Context, environment *applicationapiv1alpha1.Environment) (*applicationapiv1alpha1.DeploymentTargetClaim, error)
	GetDeploymentTargetForDeploymentTargetClaim(c client.Client, ctx context.Context, dtc *applicationapiv1alpha1.DeploymentTargetClaim) (*applicationapiv1alpha1.DeploymentTarget, error)
	FindExistingSnapshotEnvironmentBinding(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application, environment *applicationapiv1alpha1.Environment) (*applicationapiv1alpha1.SnapshotEnvironmentBinding, error)
	GetAllPipelineRunsForSnapshotAndScenario(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot, integrationTestScenario *v1beta1.IntegrationTestScenario) (*[]tektonv1.PipelineRun, error)
	GetAllBuildPipelineRunsForComponent(c client.Client, ctx context.Context, component *applicationapiv1alpha1.Component) (*[]tektonv1.PipelineRun, error)
	GetAllSnapshots(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Snapshot, error)
	GetAutoReleasePlansForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]releasev1alpha1.ReleasePlan, error)
	GetScenario(c client.Client, ctx context.Context, name, namespace string) (*v1beta1.IntegrationTestScenario, error)
	GetAllEnvironmentsForScenario(c client.Client, ctx context.Context, integrationTestScenario *v1beta1.IntegrationTestScenario) (*[]applicationapiv1alpha1.Environment, error)
	GetAllSnapshotsForBuildPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*[]applicationapiv1alpha1.Snapshot, error)
	GetAllTaskRunsWithMatchingPipelineRunLabel(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*[]tektonv1.TaskRun, error)
}

type loader struct{}

func NewLoader() ObjectLoader {
	return &loader{}
}

// GetAllEnvironments gets all environments in the namespace
func (l *loader) GetAllEnvironments(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Environment, error) {

	environmentList := &applicationapiv1alpha1.EnvironmentList{}
	opts := []client.ListOption{
		client.InNamespace(application.Namespace),
	}
	err := c.List(ctx, environmentList, opts...)
	if err != nil {
		return nil, err
	}
	return &environmentList.Items, err
}

// GetReleasesWithSnapshot returns all Releases associated with the given snapshot.
// In the case the List operation fails, an error will be returned.
func (l *loader) GetReleasesWithSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*[]releasev1alpha1.Release, error) {
	releases := &releasev1alpha1.ReleaseList{}
	opts := []client.ListOption{
		client.InNamespace(snapshot.Namespace),
		client.MatchingFields{"spec.snapshot": snapshot.Name},
	}

	err := c.List(ctx, releases, opts...)
	if err != nil {
		return nil, err
	}

	return &releases.Items, nil
}

// GetAllApplicationComponents loads from the cluster all Components associated with the given Application.
// If the Application doesn't have any Components or this is not found in the cluster, an error will be returned.
func (l *loader) GetAllApplicationComponents(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Component, error) {
	applicationComponents := &applicationapiv1alpha1.ComponentList{}
	opts := []client.ListOption{
		client.InNamespace(application.Namespace),
		client.MatchingFields{"spec.application": application.Name},
	}

	err := c.List(ctx, applicationComponents, opts...)
	if err != nil {
		return nil, err
	}

	return &applicationComponents.Items, nil
}

// GetApplicationFromSnapshot loads from the cluster the Application referenced in the given Snapshot.
// If the Snapshot doesn't specify an Component or this is not found in the cluster, an error will be returned.
func (l *loader) GetApplicationFromSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*applicationapiv1alpha1.Application, error) {
	application := &applicationapiv1alpha1.Application{}
	return application, toolkit.GetObject(snapshot.Spec.Application, snapshot.Namespace, c, ctx, application)
}

// GetComponentFromSnapshot loads from the cluster the Component referenced in the given Snapshot.
// If the Snapshot doesn't specify an Application or this is not found in the cluster, an error will be returned.
func (l *loader) GetComponentFromSnapshot(c client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot) (*applicationapiv1alpha1.Component, error) {
	if componentLabel, ok := snapshot.Labels[gitops.SnapshotComponentLabel]; ok {
		component := &applicationapiv1alpha1.Component{}
		err := c.Get(ctx, types.NamespacedName{
			Namespace: snapshot.Namespace,
			Name:      componentLabel,
		}, component)

		if err != nil {
			return nil, err
		}

		return component, nil
	} else {
		return nil, nil
	}
}

// GetComponentFromPipelineRun loads from the cluster the Component referenced in the given PipelineRun. If the PipelineRun doesn't
// specify a Component or this is not found in the cluster, an error will be returned.
func (l *loader) GetComponentFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Component, error) {
	if componentName, found := pipelineRun.Labels[tekton.PipelineRunComponentLabel]; found {
		component := &applicationapiv1alpha1.Component{}
		err := c.Get(ctx, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      componentName,
		}, component)

		if err != nil {
			return nil, err
		}

		return component, nil
	}

	return nil, nil
}

// GetApplicationFromPipelineRun loads from the cluster the Application referenced in the given PipelineRun. If the PipelineRun doesn't
// specify an Application or this is not found in the cluster, an error will be returned.
func (l *loader) GetApplicationFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Application, error) {
	if applicationName, found := pipelineRun.Labels[tekton.PipelineRunApplicationLabel]; found {
		application := &applicationapiv1alpha1.Application{}
		err := c.Get(ctx, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      applicationName,
		}, application)

		if err != nil {
			return nil, err
		}

		return application, nil
	}

	return nil, nil
}

// GetApplicationFromComponent loads from the cluster the Application referenced in the given Component. If the Component doesn't
// specify an Application or this is not found in the cluster, an error will be returned.
func (l *loader) GetApplicationFromComponent(c client.Client, ctx context.Context, component *applicationapiv1alpha1.Component) (*applicationapiv1alpha1.Application, error) {
	application := &applicationapiv1alpha1.Application{}
	err := c.Get(ctx, types.NamespacedName{
		Namespace: component.Namespace,
		Name:      component.Spec.Application,
	}, application)

	if err != nil {
		return nil, err
	}

	return application, nil
}

// GetEnvironmentFromIntegrationPipelineRun loads from the cluster the Environment referenced in the given PipelineRun.
// If the PipelineRun doesn't specify an Environment or this is not found in the cluster, an error will be returned.
func (l *loader) GetEnvironmentFromIntegrationPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Environment, error) {
	if environmentLabel, ok := pipelineRun.Labels[tekton.EnvironmentNameLabel]; ok {
		environment := &applicationapiv1alpha1.Environment{}
		err := c.Get(ctx, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      environmentLabel,
		}, environment)

		if err != nil {
			return nil, err
		}

		return environment, nil
	} else {
		return nil, nil
	}
}

// GetSnapshotFromPipelineRun loads from the cluster the Snapshot referenced in the given PipelineRun.
// If the PipelineRun doesn't specify an Snapshot or this is not found in the cluster, an error will be returned.
func (l *loader) GetSnapshotFromPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*applicationapiv1alpha1.Snapshot, error) {
	if snapshotName, found := pipelineRun.Labels[tekton.SnapshotNameLabel]; found {
		snapshot := &applicationapiv1alpha1.Snapshot{}
		err := c.Get(ctx, types.NamespacedName{
			Namespace: pipelineRun.Namespace,
			Name:      snapshotName,
		}, snapshot)

		if err != nil {
			return nil, err
		}

		return snapshot, nil
	}

	return nil, fmt.Errorf("the pipeline has no snapshot associated with it")
}

// GetAllIntegrationTestScenariosForApplication returns all IntegrationTestScenarios used by the application being processed.
func (l *loader) GetAllIntegrationTestScenariosForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]v1beta1.IntegrationTestScenario, error) {
	integrationList := &v1beta1.IntegrationTestScenarioList{}

	opts := &client.ListOptions{
		Namespace:     application.Namespace,
		FieldSelector: fields.OneTermEqualSelector("spec.application", application.Name),
	}

	err := c.List(ctx, integrationList, opts)
	if err != nil {
		return nil, err
	}

	return &integrationList.Items, nil
}

// GetRequiredIntegrationTestScenariosForApplication returns the IntegrationTestScenarios used by the application being processed.
// An IntegrationTestScenarios will only be returned if it has the test.appstudio.openshift.io/optional
// label not set to true or if it is missing the label entirely.
func (l *loader) GetRequiredIntegrationTestScenariosForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]v1beta1.IntegrationTestScenario, error) {
	integrationList := &v1beta1.IntegrationTestScenarioList{}
	labelRequirement, err := labels.NewRequirement("test.appstudio.openshift.io/optional", selection.NotIn, []string{"true"})
	if err != nil {
		return nil, err
	}
	labelSelector := labels.NewSelector().Add(*labelRequirement)

	opts := &client.ListOptions{
		Namespace:     application.Namespace,
		FieldSelector: fields.OneTermEqualSelector("spec.application", application.Name),
		LabelSelector: labelSelector,
	}

	err = c.List(ctx, integrationList, opts)
	if err != nil {
		return nil, err
	}

	return &integrationList.Items, nil
}

// GetDeploymentTargetClaimForEnvironment try to find the DeploymentTargetClaim whose name is defined in Environment
// if not found, an error is returned
func (l *loader) GetDeploymentTargetClaimForEnvironment(c client.Client, ctx context.Context, environment *applicationapiv1alpha1.Environment) (*applicationapiv1alpha1.DeploymentTargetClaim, error) {
	if (environment.Spec.Configuration.Target != applicationapiv1alpha1.EnvironmentTarget{}) {
		dtcName := environment.Spec.Configuration.Target.DeploymentTargetClaim.ClaimName
		if dtcName != "" {
			deploymentTargetClaim := &applicationapiv1alpha1.DeploymentTargetClaim{}
			err := c.Get(ctx, types.NamespacedName{
				Namespace: environment.Namespace,
				Name:      dtcName,
			}, deploymentTargetClaim)

			if err != nil {
				return nil, err
			}

			return deploymentTargetClaim, nil
		}
	}

	return nil, fmt.Errorf("deploymentTargetClaim is not defined in .Spec.Configuration.Target.DeploymentTargetClaim.ClaimName for Environment: %s/%s", environment.Namespace, environment.Name)
}

// GetDeploymentTargetForDeploymentTargetClaim try to find the DeploymentTarget whose name is defined in DeploymentTargetClaim
// if not found, an error is returned
func (l *loader) GetDeploymentTargetForDeploymentTargetClaim(c client.Client, ctx context.Context, dtc *applicationapiv1alpha1.DeploymentTargetClaim) (*applicationapiv1alpha1.DeploymentTarget, error) {
	dtName := dtc.Spec.TargetName
	if dtName == "" {
		return nil, fmt.Errorf("deploymentTarget is not defined in .Spec.TargetName for deploymentTargetClaim: %s/%s", dtc.Namespace, dtc.Name)
	}

	deploymentTarget := &applicationapiv1alpha1.DeploymentTarget{}
	err := c.Get(ctx, types.NamespacedName{
		Namespace: dtc.Namespace,
		Name:      dtName,
	}, deploymentTarget)

	if err != nil {
		return nil, err
	}

	return deploymentTarget, nil
}

// FindExistingSnapshotEnvironmentBinding attempts to find a SnapshotEnvironmentBinding that's
// associated with the provided environment.
func (l *loader) FindExistingSnapshotEnvironmentBinding(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application, environment *applicationapiv1alpha1.Environment) (*applicationapiv1alpha1.SnapshotEnvironmentBinding, error) {
	snapshotEnvironmentBindingList := &applicationapiv1alpha1.SnapshotEnvironmentBindingList{}
	opts := []client.ListOption{
		client.InNamespace(application.Namespace),
		client.MatchingFields{"spec.environment": environment.Name},
	}

	err := c.List(ctx, snapshotEnvironmentBindingList, opts...)
	if err != nil {
		return nil, err
	}

	for _, binding := range snapshotEnvironmentBindingList.Items {
		if binding.Spec.Application == application.Name {
			return &binding, nil
		}
	}

	return nil, nil
}

// GetAllPipelineRunsForSnapshotAndScenario returns all Integration PipelineRun for the
// associated Snapshot and IntegrationTestScenario. In the case the List operation fails,
// an error will be returned.
func (l *loader) GetAllPipelineRunsForSnapshotAndScenario(adapterClient client.Client, ctx context.Context, snapshot *applicationapiv1alpha1.Snapshot, integrationTestScenario *v1beta1.IntegrationTestScenario) (*[]tektonv1.PipelineRun, error) {
	integrationPipelineRuns := &tektonv1.PipelineRunList{}
	opts := []client.ListOption{
		client.InNamespace(snapshot.Namespace),
		client.MatchingLabels{
			"pipelines.appstudio.openshift.io/type": "test",
			"appstudio.openshift.io/snapshot":       snapshot.Name,
			"test.appstudio.openshift.io/scenario":  integrationTestScenario.Name,
		},
	}

	err := adapterClient.List(ctx, integrationPipelineRuns, opts...)
	if err != nil {
		return nil, err
	}
	return &integrationPipelineRuns.Items, nil
}

// GetAllBuildPipelineRunsForComponent returns all PipelineRun for the
// associated component. In the case the List operation fails,
// an error will be returned.
func (l *loader) GetAllBuildPipelineRunsForComponent(c client.Client, ctx context.Context, component *applicationapiv1alpha1.Component) (*[]tektonv1.PipelineRun, error) {
	buildPipelineRuns := &tektonv1.PipelineRunList{}
	opts := []client.ListOption{
		client.InNamespace(component.Namespace),
		client.MatchingLabels{
			"pipelines.appstudio.openshift.io/type": "build",
			"appstudio.openshift.io/component":      component.Name,
		},
	}

	err := c.List(ctx, buildPipelineRuns, opts...)
	if err != nil {
		return nil, err
	}
	return &buildPipelineRuns.Items, nil
}

// GetAllSnapshots returns all Snapshots in the Application's namespace nil if it's not found.
// In the case the List operation fails, an error will be returned.
func (l *loader) GetAllSnapshots(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]applicationapiv1alpha1.Snapshot, error) {
	snapshots := &applicationapiv1alpha1.SnapshotList{}
	opts := []client.ListOption{
		client.InNamespace(application.Namespace),
		client.MatchingFields{"spec.application": application.Name},
	}

	err := c.List(ctx, snapshots, opts...)
	if err != nil {
		return nil, err
	}

	return &snapshots.Items, nil
}

// GetAutoReleasePlansForApplication returns the ReleasePlans used by the application being processed. If matching
// ReleasePlans are not found, an error will be returned. A ReleasePlan will only be returned if it has the
// release.appstudio.openshift.io/auto-release label set to true or if it is missing the label entirely.
func (l *loader) GetAutoReleasePlansForApplication(c client.Client, ctx context.Context, application *applicationapiv1alpha1.Application) (*[]releasev1alpha1.ReleasePlan, error) {
	releasePlans := &releasev1alpha1.ReleasePlanList{}
	labelRequirement, err := labels.NewRequirement("release.appstudio.openshift.io/auto-release", selection.NotIn, []string{"false"})
	if err != nil {
		return nil, err
	}
	labelSelector := labels.NewSelector().Add(*labelRequirement)

	opts := &client.ListOptions{
		Namespace:     application.Namespace,
		FieldSelector: fields.OneTermEqualSelector("spec.application", application.Name),
		LabelSelector: labelSelector,
	}

	err = c.List(ctx, releasePlans, opts)
	if err != nil {
		return nil, err
	}

	return &releasePlans.Items, nil
}

// GetScenario returns integration test scenario requested by name and namespace
func (l *loader) GetScenario(c client.Client, ctx context.Context, name, namespace string) (*v1beta1.IntegrationTestScenario, error) {
	scenario := &v1beta1.IntegrationTestScenario{}
	return scenario, toolkit.GetObject(name, namespace, c, ctx, scenario)
}

// GetAllEnvironmentsForScenario returns all Environments for the associated integrationTestScenario.
// In the case the List operation fails, an error will be returned.
func (l *loader) GetAllEnvironmentsForScenario(c client.Client, ctx context.Context, integrationTestScenario *v1beta1.IntegrationTestScenario) (*[]applicationapiv1alpha1.Environment, error) {
	environments := &applicationapiv1alpha1.EnvironmentList{}
	opts := []client.ListOption{
		client.InNamespace(integrationTestScenario.Namespace),
		client.MatchingLabels{
			"test.appstudio.openshift.io/scenario": integrationTestScenario.Name,
		},
	}

	err := c.List(ctx, environments, opts...)
	if err != nil {
		return nil, err
	}
	return &environments.Items, nil
}

// GetAllSnapshotsForBuildPipelineRun returns all Snapshots for the associated build pipelineRun.
// In the case the List operation fails, an error will be returned.
func (l *loader) GetAllSnapshotsForBuildPipelineRun(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*[]applicationapiv1alpha1.Snapshot, error) {
	snapshots := &applicationapiv1alpha1.SnapshotList{}
	opts := []client.ListOption{
		client.InNamespace(pipelineRun.Namespace),
		client.MatchingLabels{
			gitops.BuildPipelineRunNameLabel: pipelineRun.Name,
		},
	}

	err := c.List(ctx, snapshots, opts...)
	if err != nil {
		return nil, err
	}
	return &snapshots.Items, nil
}

// GetAllTaskRunsWithMatchingPipelineRunLabel finds all Child TaskRuns
// whose "tekton.dev/pipeline" label points to the given PipelineRun
func (l *loader) GetAllTaskRunsWithMatchingPipelineRunLabel(c client.Client, ctx context.Context, pipelineRun *tektonv1.PipelineRun) (*[]tektonv1.TaskRun, error) {
	taskRuns := &tektonv1.TaskRunList{}
	opts := []client.ListOption{
		client.InNamespace(pipelineRun.Namespace),
		client.MatchingLabels{
			"tekton.dev/pipelineRun": pipelineRun.Name,
		},
	}

	err := c.List(ctx, taskRuns, opts...)
	if err != nil {
		return nil, err
	}

	return &taskRuns.Items, nil
}
