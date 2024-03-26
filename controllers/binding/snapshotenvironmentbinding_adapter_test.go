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

package binding

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/tonglil/buflogr"

	ctrl "sigs.k8s.io/controller-runtime"

	toolkit "github.com/konflux-ci/operator-toolkit/loader"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
	releasemetadata "github.com/redhat-appstudio/release-service/metadata"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Binding Adapter", Ordered, func() {
	var (
		adapter *Adapter
		logger  helpers.IntegrationLogger

		testReleasePlan         *releasev1alpha1.ReleasePlan
		hasApp                  *applicationapiv1alpha1.Application
		hasComp                 *applicationapiv1alpha1.Component
		hasSnapshot             *applicationapiv1alpha1.Snapshot
		finishedSnapshot        *applicationapiv1alpha1.Snapshot
		deploymentTargetClaim   *applicationapiv1alpha1.DeploymentTargetClaim
		deploymentTarget        *applicationapiv1alpha1.DeploymentTarget
		integrationTestScenario *v1beta1.IntegrationTestScenario
		hasEnv                  *applicationapiv1alpha1.Environment
		hasBinding              *applicationapiv1alpha1.SnapshotEnvironmentBinding
	)
	const (
		SampleRepoLink = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		sampleImage    = "quay.io/redhat-appstudio/sample-image"
	)

	BeforeAll(func() {

		logger = helpers.IntegrationLogger{Logger: ctrl.Log}

		hasApp = &applicationapiv1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "application-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ApplicationSpec{
				DisplayName: "application-sample",
				Description: "This is an example application",
			},
		}

		Expect(k8sClient.Create(ctx, hasApp)).Should(Succeed())

		integrationTestScenario = &v1beta1.IntegrationTestScenario{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-pass",
				Namespace: "default",

				Labels: map[string]string{
					"test.appstudio.openshift.io/optional": "false",
				},
			},
			Spec: v1beta1.IntegrationTestScenarioSpec{
				Application: "application-sample",
				ResolverRef: v1beta1.ResolverRef{
					Resolver: "git",
					Params: []v1beta1.ResolverParameter{
						{
							Name:  "url",
							Value: "https://github.com/redhat-appstudio/integration-examples.git",
						},
						{
							Name:  "revision",
							Value: "main",
						},
						{
							Name:  "pathInRepo",
							Value: "pipelineruns/integration_pipelinerun_pass.yaml",
						},
					},
				},
				Environment: v1beta1.TestEnvironment{
					Name: "envname",
					Type: "POC",
					Configuration: &applicationapiv1alpha1.EnvironmentConfiguration{
						Env: []applicationapiv1alpha1.EnvVarPair{},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, integrationTestScenario)).Should(Succeed())

		testReleasePlan = &releasev1alpha1.ReleasePlan{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-releaseplan-",
				Namespace:    "default",
				Labels: map[string]string{
					releasemetadata.AutoReleaseLabel: "true",
				},
			},
			Spec: releasev1alpha1.ReleasePlanSpec{
				Application: hasApp.Name,
				Target:      "default",
			},
		}
		Expect(k8sClient.Create(ctx, testReleasePlan)).Should(Succeed())

		deploymentTarget = &applicationapiv1alpha1.DeploymentTarget{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "dt" + "-",
				Namespace:    "default",
			},
			Spec: applicationapiv1alpha1.DeploymentTargetSpec{
				DeploymentTargetClassName: "dtcls-name",
				KubernetesClusterCredentials: applicationapiv1alpha1.DeploymentTargetKubernetesClusterCredentials{
					DefaultNamespace:           "default",
					APIURL:                     "https://url",
					ClusterCredentialsSecret:   "secret-sample",
					AllowInsecureSkipTLSVerify: false,
				},
			},
		}
		Expect(k8sClient.Create(ctx, deploymentTarget)).Should(Succeed())

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample",
				Application:    "application-sample",
				ContainerImage: "",
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL: SampleRepoLink,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		finishedSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample-finished",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:            "component",
					gitops.SnapshotComponentLabel:       hasComp.Name,
					gitops.PipelineAsCodeEventTypeLabel: "push",
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation: "123",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           hasComp.Name,
						ContainerImage: sampleImage,
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, finishedSnapshot)).Should(Succeed())

		Eventually(func() error {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      finishedSnapshot.Name,
				Namespace: "default",
			}, finishedSnapshot)
			return err
		}, time.Second*10).ShouldNot(HaveOccurred())

		err := gitops.MarkSnapshotAsPassed(k8sClient, ctx, finishedSnapshot, "Snapshot passed")
		Expect(err == nil).To(BeTrue())
		Expect(gitops.HaveAppStudioTestsFinished(finishedSnapshot)).To(BeTrue())
	})

	BeforeEach(func() {
		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:            "component",
					gitops.SnapshotComponentLabel:       hasComp.Name,
					gitops.PipelineAsCodeEventTypeLabel: "push",
				},
				Annotations: map[string]string{
					gitops.PipelineAsCodeInstallationIDAnnotation: "123",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           "component-sample",
						ContainerImage: sampleImage,
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

		deploymentTargetClaim = &applicationapiv1alpha1.DeploymentTargetClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dtcname",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.DeploymentTargetClaimSpec{
				DeploymentTargetClassName: applicationapiv1alpha1.DeploymentTargetClassName("dtcls-name"),
				TargetName:                deploymentTarget.Name,
			},
		}
		Expect(k8sClient.Create(ctx, deploymentTargetClaim)).Should(Succeed())

		hasEnv = &applicationapiv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "envname",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.EnvironmentSpec{
				Type:               "POC",
				DisplayName:        "my-environment",
				DeploymentStrategy: applicationapiv1alpha1.DeploymentStrategy_Manual,
				ParentEnvironment:  "",
				Tags:               []string{},
				Configuration: applicationapiv1alpha1.EnvironmentConfiguration{
					Env: []applicationapiv1alpha1.EnvVarPair{
						{
							Name:  "var_name",
							Value: "test",
						},
					},
					Target: applicationapiv1alpha1.EnvironmentTarget{
						DeploymentTargetClaim: applicationapiv1alpha1.DeploymentTargetClaimConfig{
							ClaimName: deploymentTargetClaim.Name,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasEnv)).Should(Succeed())

		hasBinding = &applicationapiv1alpha1.SnapshotEnvironmentBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-binding-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTestScenarioLabel: integrationTestScenario.Name,
				},
			},
			Spec: applicationapiv1alpha1.SnapshotEnvironmentBindingSpec{
				Application: hasApp.Name,
				Snapshot:    hasSnapshot.Name,
				Environment: hasEnv.Name,
				Components:  []applicationapiv1alpha1.BindingComponent{},
			},
		}
		Expect(k8sClient.Create(ctx, hasBinding)).Should(Succeed())

		hasBinding.Status = applicationapiv1alpha1.SnapshotEnvironmentBindingStatus{
			ComponentDeploymentConditions: []metav1.Condition{
				{
					Reason:             "Completed",
					Status:             "True",
					Type:               gitops.BindingDeploymentStatusConditionType,
					LastTransitionTime: metav1.Time{Time: time.Now()},
				},
			},
		}

		Expect(k8sClient.Status().Update(ctx, hasBinding)).Should(Succeed())

		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name:      hasBinding.Name,
				Namespace: "default",
			}, hasBinding)
			return err == nil && gitops.IsBindingDeployed(hasBinding)
		}, time.Second*20).Should(BeTrue())
	})

	AfterEach(func() {
		err := k8sClient.Delete(ctx, hasBinding)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, deploymentTargetClaim)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasEnv)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, integrationTestScenario)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, testReleasePlan)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, deploymentTarget)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, finishedSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())

	})

	It("can create a new Adapter instance", func() {
		Expect(reflect.TypeOf(NewAdapter(hasBinding, hasSnapshot, hasEnv, hasApp, hasComp, integrationTestScenario, logger, loader.NewMockLoader(), k8sClient, ctx))).To(Equal(reflect.TypeOf(&Adapter{})))
	})

	It("ensures the integrationTestPipelines are created for a deployed SnapshotEnvironment binding", func() {
		adapter = NewAdapter(hasBinding, hasSnapshot, hasEnv, hasApp, hasComp, integrationTestScenario, logger, loader.NewMockLoader(), k8sClient, ctx)
		Expect(reflect.TypeOf(adapter)).To(Equal(reflect.TypeOf(&Adapter{})))
		adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
			{
				ContextKey: loader.ApplicationContextKey,
				Resource:   hasApp,
			},
			{
				ContextKey: loader.ComponentContextKey,
				Resource:   hasComp,
			},
			{
				ContextKey: loader.DeploymentTargetClaimContextKey,
				Resource:   deploymentTargetClaim,
			},
			{
				ContextKey: loader.DeploymentTargetContextKey,
				Resource:   deploymentTarget,
			},
			{
				ContextKey: loader.PipelineRunsContextKey,
				Resource:   nil,
			},
		})
		Eventually(func() bool {
			result, err := adapter.EnsureIntegrationTestPipelineForScenarioExists()
			return !result.CancelRequest && err == nil
		}, time.Second*20).Should(BeTrue())

		integrationPipelineRuns := &tektonv1.PipelineRunList{}
		opts := []client.ListOption{
			client.InNamespace(hasApp.Namespace),
			client.MatchingLabels{
				"pipelines.appstudio.openshift.io/type": "test",
				"appstudio.openshift.io/application":    hasApp.Name,
				"appstudio.openshift.io/component":      hasComp.Name,
				"appstudio.openshift.io/snapshot":       hasSnapshot.Name,
				"test.appstudio.openshift.io/scenario":  integrationTestScenario.Name,
				"appstudio.openshift.io/environment":    hasEnv.Name,
			},
		}
		Eventually(func() bool {
			err := k8sClient.List(adapter.context, integrationPipelineRuns, opts...)
			return len(integrationPipelineRuns.Items) > 0 && err == nil
		}, time.Second*20).Should(BeTrue())

		integrationPipelineRun := integrationPipelineRuns.Items[0]
		fmt.Fprintf(GinkgoWriter, "*******integrationPipelineRun: %v\n", integrationPipelineRun)
		Expect(integrationPipelineRun.Labels["appstudio.openshift.io/application"]).To(Equal(hasApp.Name))
		Expect(integrationPipelineRun.Labels["appstudio.openshift.io/component"]).To(Equal(hasComp.Name))
		Expect(integrationPipelineRun.Labels["appstudio.openshift.io/environment"]).To(Equal(hasEnv.Name))
		Expect(integrationPipelineRun.Spec.Workspaces).NotTo(BeNil())
		Expect(integrationPipelineRun.Spec.Workspaces).NotTo(BeEmpty())
		Expect(integrationPipelineRun.Spec.Params).NotTo(BeEmpty())

		Expect(k8sClient.Delete(ctx, &integrationPipelineRuns.Items[0])).Should(Succeed())

	})

	It("ensures ephemeral environment is deleted for the given pipelineRun", func() {
		var buf bytes.Buffer
		log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
		hasEnv.Spec.Tags = append(hasEnv.Spec.Tags, "ephemeral")
		hasBinding.Status = applicationapiv1alpha1.SnapshotEnvironmentBindingStatus{
			BindingConditions: []metav1.Condition{
				{
					Reason:             "ErrorOccurred",
					Status:             "True",
					Type:               gitops.BindingErrorOccurredStatusConditionType,
					LastTransitionTime: metav1.Time{Time: time.Time{}},
				},
			},
		}

		adapter = NewAdapter(hasBinding, hasSnapshot, hasEnv, hasApp, hasComp, integrationTestScenario, log, loader.NewMockLoader(), k8sClient, ctx)
		adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
			{
				ContextKey: loader.ApplicationContextKey,
				Resource:   hasApp,
			},
			{
				ContextKey: loader.EnvironmentContextKey,
				Resource:   hasEnv,
			},
			{
				ContextKey: loader.DeploymentTargetContextKey,
				Resource:   deploymentTarget,
			},
		})

		dtc, _ := adapter.loader.GetDeploymentTargetClaimForEnvironment(k8sClient, adapter.context, hasEnv)
		Expect(dtc).NotTo(BeNil())

		dt, _ := adapter.loader.GetDeploymentTargetForDeploymentTargetClaim(k8sClient, adapter.context, dtc)
		Expect(dt).NotTo(BeNil())

		result, err := adapter.EnsureEphemeralEnvironmentsCleanedUp()
		Expect(!result.CancelRequest && err == nil).To(BeTrue())

		Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeFalse())

		expectedLogEntry := "DeploymentTargetClaim deleted"
		Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

		expectedLogEntry = "Ephemeral environment is deleted and its owning SnapshotEnvironmentBinding is in the process of being deleted"
		Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
	})

	It("Requeues the environment cleanup if the binding is younger than the threshold", func() {
		var buf bytes.Buffer
		var dtc *applicationapiv1alpha1.DeploymentTargetClaim

		log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
		hasEnv.Spec.Tags = append(hasEnv.Spec.Tags, "ephemeral")
		hasBinding.Status = applicationapiv1alpha1.SnapshotEnvironmentBindingStatus{
			BindingConditions: []metav1.Condition{
				{
					Reason:             "ErrorOccurred",
					Status:             "True",
					Type:               gitops.BindingErrorOccurredStatusConditionType,
					LastTransitionTime: metav1.Time{Time: time.Now()},
				},
			},
		}

		adapter = NewAdapter(hasBinding, hasSnapshot, hasEnv, hasApp, hasComp, integrationTestScenario, log, loader.NewMockLoader(), k8sClient, ctx)
		adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
			{
				ContextKey: loader.ApplicationContextKey,
				Resource:   hasApp,
			},
			{
				ContextKey: loader.ComponentContextKey,
				Resource:   hasComp,
			},
			{
				ContextKey: loader.EnvironmentContextKey,
				Resource:   hasEnv,
			},
			{
				ContextKey: loader.SnapshotContextKey,
				Resource:   hasSnapshot,
			},
			{
				ContextKey: loader.DeploymentTargetContextKey,
				Resource:   deploymentTarget,
			},
			{
				ContextKey: loader.DeploymentTargetClaimContextKey,
				Resource:   deploymentTargetClaim,
			},
			{
				ContextKey: loader.SnapshotEnvironmentBindingContextKey,
				Resource:   hasBinding,
			},
		})

		dtc, _ = adapter.loader.GetDeploymentTargetClaimForEnvironment(k8sClient, adapter.context, hasEnv)
		Expect(dtc).NotTo(BeNil())

		dt, _ := adapter.loader.GetDeploymentTargetForDeploymentTargetClaim(k8sClient, adapter.context, dtc)
		Expect(dt).NotTo(BeNil())

		result, err := adapter.EnsureEphemeralEnvironmentsCleanedUp()
		Expect(!result.CancelRequest && err == nil && result.RequeueDelay != 0).To(BeTrue())

		Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeFalse())

		// Check the log that the environment and DTC to not have been deleted
		deleteLogEntry := "DeploymentTargetClaim deleted"
		Expect(buf.String()).ShouldNot(ContainSubstring(deleteLogEntry))

		deleteLogEntry = "Ephemeral environment and its owning snapshotEnvironmentBinding deleted"
		Expect(buf.String()).ShouldNot(ContainSubstring(deleteLogEntry))

		dtc, _ = adapter.loader.GetDeploymentTargetClaimForEnvironment(k8sClient, adapter.context, hasEnv)
		Expect(dtc).NotTo(BeNil())

		environments, _ := adapter.loader.GetAllEnvironments(k8sClient, adapter.context, hasApp)
		Expect(*environments).To(ContainElement(HaveField("ObjectMeta.Name", "envname")))
	})

	When("binding deployment failed", func() {
		var (
			failedBinding *applicationapiv1alpha1.SnapshotEnvironmentBinding
		)

		BeforeEach(func() {
			failedBinding = &applicationapiv1alpha1.SnapshotEnvironmentBinding{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "snapshot-binding-sample-failed",
					Namespace: "default",
					Labels: map[string]string{
						gitops.SnapshotTestScenarioLabel: integrationTestScenario.Name,
					},
				},
				Spec: applicationapiv1alpha1.SnapshotEnvironmentBindingSpec{
					Application: hasApp.Name,
					Snapshot:    hasSnapshot.Name,
					Environment: hasEnv.Name,
					Components:  []applicationapiv1alpha1.BindingComponent{},
				},
			}
			Expect(k8sClient.Create(ctx, failedBinding)).Should(Succeed())

			failedBinding.Status = applicationapiv1alpha1.SnapshotEnvironmentBindingStatus{
				BindingConditions: []metav1.Condition{
					{
						Reason: "Failed",
						Status: "True",
						Type:   gitops.BindingErrorOccurredStatusConditionType,
						LastTransitionTime: metav1.Time{
							// time set after timeout so cleanup is called
							Time: time.Now().Add(time.Duration(-1*SnapshotEnvironmentBindingErrorTimeoutSeconds*float64(time.Second) + 1)),
						},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, failedBinding)).Should(Succeed())
		})

		AfterEach(func() {
			err := k8sClient.Delete(ctx, failedBinding)
			Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		})

		It("Failed binding test status is reported into snapshot", func() {
			adapter = NewAdapter(failedBinding, hasSnapshot, hasEnv, hasApp, hasComp, integrationTestScenario, logger, loader.NewMockLoader(), k8sClient, ctx)
			Expect(reflect.TypeOf(adapter)).To(Equal(reflect.TypeOf(&Adapter{})))

			Eventually(func() bool {
				result, err := adapter.EnsureEphemeralEnvironmentsCleanedUp()
				return !result.CancelRequest && err == nil
			}, time.Second*20).Should(BeTrue())

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			detail, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
			Expect(ok).To(BeTrue())
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusDeploymentError_Deprecated))
		})
	})
})
