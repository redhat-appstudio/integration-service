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

package integrationpipeline

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	"github.com/redhat-appstudio/integration-service/gitops"
	"github.com/redhat-appstudio/integration-service/helpers"
	"github.com/redhat-appstudio/integration-service/loader"
	intgteststat "github.com/redhat-appstudio/integration-service/pkg/integrationteststatus"
	toolkit "github.com/redhat-appstudio/operator-toolkit/loader"

	"knative.dev/pkg/apis"
	v1 "knative.dev/pkg/apis/duck/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tonglil/buflogr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Pipeline Adapter", Ordered, func() {
	var (
		adapter *Adapter
		logger  helpers.IntegrationLogger

		successfulTaskRun                     *tektonv1.TaskRun
		failedTaskRun                         *tektonv1.TaskRun
		integrationPipelineRunComponent       *tektonv1.PipelineRun
		integrationPipelineRunComponentFailed *tektonv1.PipelineRun
		intgPipelineRunWithDeletionTimestamp  *tektonv1.PipelineRun
		hasComp                               *applicationapiv1alpha1.Component
		hasComp2                              *applicationapiv1alpha1.Component
		hasApp                                *applicationapiv1alpha1.Application
		hasSnapshot                           *applicationapiv1alpha1.Snapshot
		snapshotPREvent                       *applicationapiv1alpha1.Snapshot
		hasEnv                                *applicationapiv1alpha1.Environment
		deploymentTarget                      *applicationapiv1alpha1.DeploymentTarget
		deploymentTargetClaim                 *applicationapiv1alpha1.DeploymentTargetClaim
		deploymentTargetClass                 *applicationapiv1alpha1.DeploymentTargetClass
		snapshotEnvironmentBinding            *applicationapiv1alpha1.SnapshotEnvironmentBinding
		integrationTestScenario               *v1beta1.IntegrationTestScenario
		integrationTestScenarioFailed         *v1beta1.IntegrationTestScenario
	)
	const (
		SampleRepoLink           = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		SampleCommit             = "a2ba645d50e471d5f084b"
		SampleDigest             = "sha256:841328df1b9f8c4087adbdcfec6cc99ac8308805dea83f6d415d6fb8d40227c1"
		SampleImageWithoutDigest = "quay.io/redhat-appstudio/sample-image"
		SampleImage              = SampleImageWithoutDigest + "@" + SampleDigest
	)

	BeforeAll(func() {
		hasEnv = &applicationapiv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-env",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.EnvironmentSpec{
				Type:               "POC",
				DisplayName:        "my-environment",
				DeploymentStrategy: applicationapiv1alpha1.DeploymentStrategy_Manual,
				ParentEnvironment:  "",
				Tags:               []string{"ephemeral"},
				Configuration: applicationapiv1alpha1.EnvironmentConfiguration{
					Env: []applicationapiv1alpha1.EnvVarPair{
						{
							Name:  "var_name",
							Value: "test",
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasEnv)).Should(Succeed())

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

		logger = helpers.IntegrationLogger{Logger: ctrl.Log}.WithApp(*hasApp)

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample",
				Application:    "application-sample",
				ContainerImage: "invalidImage",
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL:      SampleRepoLink,
							Revision: SampleCommit,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		hasComp2 = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "another-component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "another-component-sample",
				Application:    "application-sample",
				ContainerImage: "",
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL:      SampleRepoLink,
							Revision: SampleCommit,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp2)).Should(Succeed())

		integrationTestScenario = &v1beta1.IntegrationTestScenario{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-pass",
				Namespace: "default",

				Labels: map[string]string{
					"test.appstudio.openshift.io/optional": "false",
				},
			},
			Spec: v1beta1.IntegrationTestScenarioSpec{
				Application: hasApp.Name,
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

		successfulTaskRun = &tektonv1.TaskRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-taskrun-pass",
				Namespace: "default",
			},
			Spec: tektonv1.TaskRunSpec{
				TaskRef: &tektonv1.TaskRef{
					Name: "test-taskrun-pass",
					ResolverRef: tektonv1.ResolverRef{
						Resolver: "bundle",
						Params: tektonv1.Params{
							{
								Name:  "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
							},
							{
								Name:  "name",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, successfulTaskRun)).Should(Succeed())

		now := time.Now()
		successfulTaskRun.Status = tektonv1.TaskRunStatus{
			TaskRunStatusFields: tektonv1.TaskRunStatusFields{
				StartTime:      &metav1.Time{Time: now},
				CompletionTime: &metav1.Time{Time: now.Add(5 * time.Minute)},
				Results: []tektonv1.TaskRunResult{
					{
						Name: "TEST_OUTPUT",
						Value: *tektonv1.NewStructuredValues(`{
											"result": "SUCCESS",
											"timestamp": "1665405318",
											"failures": 0,
											"successes": 10,
											"warnings": 0
										}`),
					},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, successfulTaskRun)).Should(Succeed())

		failedTaskRun = &tektonv1.TaskRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-taskrun-fail",
				Namespace: "default",
			},
			Spec: tektonv1.TaskRunSpec{
				TaskRef: &tektonv1.TaskRef{
					Name: "test-taskrun-fail",
					ResolverRef: tektonv1.ResolverRef{
						Resolver: "bundle",
						Params: tektonv1.Params{
							{
								Name:  "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
							},
							{
								Name:  "name",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
							},
						},
					},
				},
			},
		}

		Expect(k8sClient.Create(ctx, failedTaskRun)).Should(Succeed())

		failedTaskRun.Status = tektonv1.TaskRunStatus{
			TaskRunStatusFields: tektonv1.TaskRunStatusFields{
				StartTime:      &metav1.Time{Time: now},
				CompletionTime: &metav1.Time{Time: now.Add(5 * time.Minute)},
				Results: []tektonv1.TaskRunResult{
					{
						Name: "TEST_OUTPUT",
						Value: *tektonv1.NewStructuredValues(`{
											"result": "FAILURE",
											"timestamp": "1665405317",
											"failures": 1,
											"successes": 0,
											"warnings": 0
										}`),
					},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, failedTaskRun)).Should(Succeed())
	})

	BeforeEach(func() {
		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:      "component",
					gitops.SnapshotComponentLabel: hasComp.Name,
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
						ContainerImage: SampleImage,
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

		integrationPipelineRunComponent = &tektonv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pipelinerun-component-sample",
				Namespace: "default",
				Labels: map[string]string{
					"pipelines.appstudio.openshift.io/type":           "test",
					"pac.test.appstudio.openshift.io/url-org":         "redhat-appstudio",
					"pac.test.appstudio.openshift.io/original-prname": "build-service-on-push",
					"pac.test.appstudio.openshift.io/url-repository":  "build-service",
					"pac.test.appstudio.openshift.io/repository":      "build-service-pac",
					"appstudio.openshift.io/snapshot":                 hasSnapshot.Name,
					"test.appstudio.openshift.io/scenario":            integrationTestScenario.Name,
					"appstudio.openshift.io/environment":              hasEnv.Name,
					"appstudio.openshift.io/application":              hasApp.Name,
					"appstudio.openshift.io/component":                hasComp.Name,
				},
				Annotations: map[string]string{
					"pac.test.appstudio.openshift.io/on-target-branch": "[main]",
				},
			},
			Spec: tektonv1.PipelineRunSpec{
				PipelineRef: &tektonv1.PipelineRef{
					Name: "component-pipeline-pass",
					ResolverRef: tektonv1.ResolverRef{
						Resolver: "bundle",
						Params: tektonv1.Params{
							{
								Name:  "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/kpavic/test-bundle:component-pipeline-pass"},
							},
							{
								Name:  "name",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, integrationPipelineRunComponent)).Should(Succeed())

		integrationPipelineRunComponent.Status = tektonv1.PipelineRunStatus{
			PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
				CompletionTime: &metav1.Time{Time: time.Now()},
				ChildReferences: []tektonv1.ChildStatusReference{
					{
						Name:             successfulTaskRun.Name,
						PipelineTaskName: "task1",
					},
				},
			},
			Status: v1.Status{
				Conditions: v1.Conditions{
					apis.Condition{
						Reason: "Completed",
						Status: "True",
						Type:   apis.ConditionSucceeded,
					},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, integrationPipelineRunComponent)).Should(Succeed())
	})

	AfterEach(func() {
		err := k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, integrationPipelineRunComponent)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, integrationTestScenario)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, successfulTaskRun)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasEnv)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, failedTaskRun)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
	})

	When("NewAdapter is called", func() {
		It("creates and return a new adapter", func() {
			Expect(reflect.TypeOf(NewAdapter(integrationPipelineRunComponent, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx))).To(Equal(reflect.TypeOf(&Adapter{})))
		})
	})

	When("Snapshot already exists", func() {
		BeforeEach(func() {
			adapter = NewAdapter(integrationPipelineRunComponent, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
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
					ContextKey: loader.SnapshotContextKey,
					Resource:   hasSnapshot,
				},
				{
					ContextKey: loader.TaskRunContextKey,
					Resource:   successfulTaskRun,
				},
				{
					ContextKey: loader.EnvironmentContextKey,
					Resource:   hasEnv,
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.AllTaskRunsWithMatchingPipelineRunLabelContextKey,
					Resource:   []tektonv1.TaskRun{*successfulTaskRun},
				},
			})
			existingSnapshot, err := adapter.loader.GetSnapshotFromPipelineRun(adapter.client, adapter.context, integrationPipelineRunComponent)
			Expect(err).ToNot(HaveOccurred())
			Expect(existingSnapshot).ToNot(BeNil())
		})

		It("ensures test status in snapshot is updated to passed", func() {
			controllerutil.AddFinalizer(integrationPipelineRunComponent, "test.appstudio.openshift.io/pipelinerun")
			result, err := adapter.EnsureStatusReportedInSnapshot()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
			Expect(err).ToNot(HaveOccurred())

			detail, ok := statuses.GetScenarioStatus(integrationTestScenario.Name)
			Expect(ok).To(BeTrue())
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusTestPassed))
		})

		When("integration pipeline failed", func() {

			BeforeEach(func() {
				//Create one failed scenario and its failed pipelineRun
				integrationTestScenarioFailed = &v1beta1.IntegrationTestScenario{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "example-fail",
						Namespace: "default",

						Labels: map[string]string{
							"test.appstudio.openshift.io/optional": "false",
						},
					},
					Spec: v1beta1.IntegrationTestScenarioSpec{
						Application: hasApp.Name,
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
				Expect(k8sClient.Create(ctx, integrationTestScenarioFailed)).Should(Succeed())

				integrationPipelineRunComponentFailed = &tektonv1.PipelineRun{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pipelinerun-component-sample-failed",
						Namespace: "default",
						Labels: map[string]string{
							"pipelines.appstudio.openshift.io/type":           "test",
							"pac.test.appstudio.openshift.io/url-org":         "redhat-appstudio",
							"pac.test.appstudio.openshift.io/original-prname": "build-service-on-push",
							"pac.test.appstudio.openshift.io/url-repository":  "build-service",
							"pac.test.appstudio.openshift.io/repository":      "build-service-pac",
							"appstudio.openshift.io/snapshot":                 hasSnapshot.Name,
							"test.appstudio.openshift.io/scenario":            integrationTestScenarioFailed.Name,
							"appstudio.openshift.io/environment":              hasEnv.Name,
							"appstudio.openshift.io/application":              hasApp.Name,
							"appstudio.openshift.io/component":                hasComp.Name,
						},
						Annotations: map[string]string{
							"pac.test.appstudio.openshift.io/on-target-branch": "[main]",
						},
					},
					Spec: tektonv1.PipelineRunSpec{
						PipelineRef: &tektonv1.PipelineRef{
							Name: "component-pipeline-fail",
							ResolverRef: tektonv1.ResolverRef{
								Resolver: "bundle",
								Params: tektonv1.Params{
									{
										Name:  "bundle",
										Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:component-pipeline-fail"},
									},
									{
										Name:  "name",
										Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
									},
								},
							},
						},
					},
				}

				Expect(k8sClient.Create(ctx, integrationPipelineRunComponentFailed)).Should(Succeed())

				integrationPipelineRunComponentFailed.Status = tektonv1.PipelineRunStatus{
					PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
						CompletionTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
						ChildReferences: []tektonv1.ChildStatusReference{
							{
								Name:             failedTaskRun.Name,
								PipelineTaskName: "task1",
							},
						},
					},
					Status: v1.Status{
						Conditions: v1.Conditions{
							apis.Condition{
								Reason: "Failed",
								Status: "False",
								Type:   apis.ConditionSucceeded,
							},
						},
					},
				}
				Expect(k8sClient.Status().Update(ctx, integrationPipelineRunComponentFailed)).Should(Succeed())

				adapter = NewAdapter(integrationPipelineRunComponentFailed, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
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
						ContextKey: loader.SnapshotContextKey,
						Resource:   hasSnapshot,
					},
					{
						ContextKey: loader.PipelineRunsContextKey,
						Resource:   []tektonv1.PipelineRun{*integrationPipelineRunComponent, *integrationPipelineRunComponentFailed},
					},
					{
						ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
						Resource:   []v1beta1.IntegrationTestScenario{*integrationTestScenario, *integrationTestScenarioFailed},
					},
					{
						ContextKey: loader.ApplicationComponentsContextKey,
						Resource:   []applicationapiv1alpha1.Component{*hasComp},
					},
					{
						ContextKey: loader.AllTaskRunsWithMatchingPipelineRunLabelContextKey,
						Resource:   []tektonv1.TaskRun{*failedTaskRun},
					},
				})
			})

			AfterEach(func() {
				err := k8sClient.Delete(ctx, integrationPipelineRunComponentFailed)
				Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
				err = k8sClient.Delete(ctx, integrationTestScenarioFailed)
				Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
			})

			It("ensures test status in snapshot is updated to failed", func() {
				controllerutil.AddFinalizer(integrationPipelineRunComponentFailed, "test.appstudio.openshift.io/pipelinerun")
				result, err := adapter.EnsureStatusReportedInSnapshot()
				Expect(!result.CancelRequest && err == nil).To(BeTrue())

				statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(hasSnapshot)
				Expect(err).ToNot(HaveOccurred())

				detail, ok := statuses.GetScenarioStatus(integrationTestScenarioFailed.Name)
				Expect(ok).To(BeTrue())
				Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusTestFail))
				Expect(detail.TestPipelineRunName).To(Equal(integrationPipelineRunComponentFailed.Name))

			})
		})

	})

	When("EnsureEphemeralEnvironmentsCleanedUp is called", func() {
		BeforeEach(func() {
			deploymentTargetClass = &applicationapiv1alpha1.DeploymentTargetClass{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "dtcls" + "-",
				},
				Spec: applicationapiv1alpha1.DeploymentTargetClassSpec{
					Provisioner: applicationapiv1alpha1.Provisioner_Devsandbox,
				},
			}
			Expect(k8sClient.Create(ctx, deploymentTargetClass)).Should(Succeed())

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

			deploymentTargetClaim = &applicationapiv1alpha1.DeploymentTargetClaim{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "dtc" + "-",
					Namespace:    "default",
				},
				Spec: applicationapiv1alpha1.DeploymentTargetClaimSpec{
					DeploymentTargetClassName: applicationapiv1alpha1.DeploymentTargetClassName("dtcls-name"),
					TargetName:                deploymentTarget.Name,
				},
			}
			Expect(k8sClient.Create(ctx, deploymentTargetClaim)).Should(Succeed())

			snapshotEnvironmentBinding = &applicationapiv1alpha1.SnapshotEnvironmentBinding{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "binding" + "-",
					Namespace:    "default",
				},
				Spec: applicationapiv1alpha1.SnapshotEnvironmentBindingSpec{
					Application: hasApp.Name,
					Environment: hasEnv.Name,
					Snapshot:    hasSnapshot.Name,
					Components:  []applicationapiv1alpha1.BindingComponent{},
				},
			}
			Expect(k8sClient.Create(ctx, snapshotEnvironmentBinding)).Should(Succeed())

			hasEnv.Spec.Configuration.Target = applicationapiv1alpha1.EnvironmentTarget{
				DeploymentTargetClaim: applicationapiv1alpha1.DeploymentTargetClaimConfig{
					ClaimName: deploymentTargetClaim.Name,
				},
			}
			Expect(k8sClient.Update(ctx, hasEnv)).Should(Succeed())
		})
		It("ensures ephemeral environment is deleted for the given pipelineRun ", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			adapter = NewAdapter(integrationPipelineRunComponent, hasApp, hasSnapshot, log, loader.NewMockLoader(), k8sClient, ctx)
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
					ContextKey: loader.PipelineRunsContextKey,
					Resource:   []tektonv1.PipelineRun{*integrationPipelineRunComponent},
				},
				{
					ContextKey: loader.SnapshotEnvironmentBindingContextKey,
					Resource:   snapshotEnvironmentBinding,
				},
				{
					ContextKey: loader.DeploymentTargetContextKey,
					Resource:   deploymentTarget,
				},
				{
					ContextKey: loader.DeploymentTargetClaimContextKey,
					Resource:   deploymentTargetClaim,
				},
			})

			dtc, _ := adapter.loader.GetDeploymentTargetClaimForEnvironment(k8sClient, adapter.context, hasEnv)
			Expect(dtc).NotTo(BeNil())

			dt, _ := adapter.loader.GetDeploymentTargetForDeploymentTargetClaim(k8sClient, adapter.context, dtc)
			Expect(dt).NotTo(BeNil())

			binding, _ := adapter.loader.FindExistingSnapshotEnvironmentBinding(k8sClient, adapter.context, hasApp, hasEnv)
			Expect(binding).NotTo(BeNil())

			result, err := adapter.EnsureEphemeralEnvironmentsCleanedUp()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			expectedLogEntry := "DeploymentTargetClaim deleted"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

			expectedLogEntry = "Ephemeral environment is deleted and its owning SnapshotEnvironmentBinding is in the process of being deleted"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
		})
	})

	When("EnsureStatusReportedInSnapshot is called with a PLR related to PR event", func() {
		BeforeEach(func() {
			snapshotPREvent = &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "snapshot-pr-event-sample",
					Namespace: "default",
					Labels: map[string]string{
						gitops.SnapshotTypeLabel:                     "component",
						gitops.SnapshotComponentLabel:                hasComp.Name,
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
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
							ContainerImage: SampleImage,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, snapshotPREvent)).Should(Succeed())

			//Create one failed scenario and its failed pipelineRun
			integrationTestScenarioFailed = &v1beta1.IntegrationTestScenario{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "example-fail",
					Namespace: "default",

					Labels: map[string]string{
						"test.appstudio.openshift.io/optional": "false",
					},
				},
				Spec: v1beta1.IntegrationTestScenarioSpec{
					Application: hasApp.Name,
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
			Expect(k8sClient.Create(ctx, integrationTestScenarioFailed)).Should(Succeed())

			integrationPipelineRunComponentFailed = &tektonv1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pipelinerun-component-sample-failed",
					Namespace: "default",
					Labels: map[string]string{
						"pipelines.appstudio.openshift.io/type":           "test",
						"pac.test.appstudio.openshift.io/url-org":         "redhat-appstudio",
						"pac.test.appstudio.openshift.io/original-prname": "build-service-on-push",
						"pac.test.appstudio.openshift.io/url-repository":  "build-service",
						"pac.test.appstudio.openshift.io/repository":      "build-service-pac",
						"appstudio.openshift.io/snapshot":                 snapshotPREvent.Name,
						"test.appstudio.openshift.io/scenario":            integrationTestScenarioFailed.Name,
						"appstudio.openshift.io/environment":              hasEnv.Name,
						"appstudio.openshift.io/application":              hasApp.Name,
						"appstudio.openshift.io/component":                hasComp.Name,
					},
					Finalizers: []string{
						"test.appstudio.openshift.io/pipelinerun",
					},
					Annotations: map[string]string{
						"pac.test.appstudio.openshift.io/on-target-branch": "[main]",
					},
				},
				Spec: tektonv1.PipelineRunSpec{
					PipelineRef: &tektonv1.PipelineRef{
						Name: "component-pipeline-fail",
						ResolverRef: tektonv1.ResolverRef{
							Resolver: "bundle",
							Params: tektonv1.Params{
								{
									Name:  "bundle",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:component-pipeline-fail"},
								},
								{
									Name:  "name",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
								},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, integrationPipelineRunComponentFailed)).Should(Succeed())

			integrationPipelineRunComponentFailed.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					CompletionTime: &metav1.Time{Time: time.Now().Add(5 * time.Minute)},
					ChildReferences: []tektonv1.ChildStatusReference{
						{
							Name:             failedTaskRun.Name,
							PipelineTaskName: "task1",
						},
					},
				},
				Status: v1.Status{
					Conditions: v1.Conditions{
						apis.Condition{
							Reason: "Failed",
							Status: "False",
							Type:   apis.ConditionSucceeded,
						},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, integrationPipelineRunComponentFailed)).Should(Succeed())

			adapter = NewAdapter(integrationPipelineRunComponentFailed, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
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
					ContextKey: loader.SnapshotContextKey,
					Resource:   snapshotPREvent,
				},
				{
					ContextKey: loader.PipelineRunsContextKey,
					Resource:   []tektonv1.PipelineRun{*integrationPipelineRunComponent, *integrationPipelineRunComponentFailed},
				},
				{
					ContextKey: loader.RequiredIntegrationTestScenariosContextKey,
					Resource:   []v1beta1.IntegrationTestScenario{*integrationTestScenario, *integrationTestScenarioFailed},
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
				{
					ContextKey: loader.AllTaskRunsWithMatchingPipelineRunLabelContextKey,
					Resource:   []tektonv1.TaskRun{*failedTaskRun},
				},
			})
		})

		AfterEach(func() {
			err := k8sClient.Delete(ctx, integrationPipelineRunComponentFailed)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
			err = k8sClient.Delete(ctx, integrationTestScenarioFailed)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		})

		It("ensures test status in snapshot is updated to failed", func() {
			result, err := adapter.EnsureStatusReportedInSnapshot()
			Expect(!result.CancelRequest && err == nil).To(BeTrue())

			statuses, err := gitops.NewSnapshotIntegrationTestStatusesFromSnapshot(snapshotPREvent)
			Expect(err).ToNot(HaveOccurred())

			detail, ok := statuses.GetScenarioStatus(integrationTestScenarioFailed.Name)
			Expect(ok).To(BeTrue())
			Expect(detail.Status).To(Equal(intgteststat.IntegrationTestStatusTestFail))
			Expect(detail.TestPipelineRunName).To(Equal(integrationPipelineRunComponentFailed.Name))

			Expect(integrationPipelineRunComponentFailed.Finalizers).To(ContainElement(ContainSubstring("test.appstudio.openshift.io/pipelinerun")))
		})
	})

	When("GetIntegrationPipelineRunStatus is called with an Integration PLR with non-nil Deletion timestamp", func() {
		BeforeEach(func() {
			intgPipelineRunWithDeletionTimestamp = &tektonv1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pipelinerun-component-sample-deletion-timestamp",
					Namespace: "default",
					Annotations: map[string]string{
						"pac.test.appstudio.openshift.io/on-target-branch": "[main]",
					},
				},
				Spec: tektonv1.PipelineRunSpec{
					PipelineRef: &tektonv1.PipelineRef{
						Name: "component-pipeline-fail",
						ResolverRef: tektonv1.ResolverRef{
							Resolver: "bundle",
							Params: tektonv1.Params{
								{
									Name:  "bundle",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:component-pipeline-fail"},
								},
								{
									Name:  "name",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
								},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, intgPipelineRunWithDeletionTimestamp)).Should(Succeed())

			now := metav1.NewTime(metav1.Now().Add(time.Second * 1))
			intgPipelineRunWithDeletionTimestamp.SetDeletionTimestamp(&now)

			adapter = NewAdapter(intgPipelineRunWithDeletionTimestamp, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
		})

		AfterEach(func() {
			err := k8sClient.Delete(ctx, intgPipelineRunWithDeletionTimestamp)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		})

		It("ensures test status in snapshot is updated to failed", func() {
			status, detail, err := adapter.GetIntegrationPipelineRunStatus(adapter.client, adapter.context, intgPipelineRunWithDeletionTimestamp)

			Expect(err).ToNot(HaveOccurred())
			Expect(status).To(Equal(intgteststat.IntegrationTestStatusDeleted))
			Expect(detail).To(ContainSubstring(fmt.Sprintf("Integration test which is running as pipeline run '%s', has been deleted", intgPipelineRunWithDeletionTimestamp.Name)))
			Expect(intgPipelineRunWithDeletionTimestamp.DeletionTimestamp).ToNot(BeNil())
		})
	})

	When("GetIntegrationPipelineRunStatus is called with an Integration PLR with invalid TEST_OUTPUT result", func() {
		var (
			taskRunInvalidResult      *tektonv1.TaskRun
			intgPipelineInvalidResult *tektonv1.PipelineRun
		)

		BeforeEach(func() {

			taskRunInvalidResult = &tektonv1.TaskRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-taskrun-invalid",
					Namespace: "default",
				},
				Spec: tektonv1.TaskRunSpec{
					TaskRef: &tektonv1.TaskRef{
						Name: "test-taskrun-invalid",
						ResolverRef: tektonv1.ResolverRef{
							Resolver: "bundle",
							Params: tektonv1.Params{
								{
									Name:  "bundle",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
								},
								{
									Name:  "name",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, taskRunInvalidResult)).Should(Succeed())

			now := time.Now()
			taskRunInvalidResult.Status = tektonv1.TaskRunStatus{
				TaskRunStatusFields: tektonv1.TaskRunStatusFields{
					StartTime:      &metav1.Time{Time: now},
					CompletionTime: &metav1.Time{Time: now.Add(5 * time.Minute)},
					Results: []tektonv1.TaskRunResult{
						{
							Name: "TEST_OUTPUT",
							Value: *tektonv1.NewStructuredValues(`{
												"result": "INVALID",
											}`),
						},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, taskRunInvalidResult)).Should(Succeed())

			intgPipelineInvalidResult = &tektonv1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pipelinerun-component-sample-invalid-result",
					Namespace: "default",
					Annotations: map[string]string{
						"pac.test.appstudio.openshift.io/on-target-branch": "[main]",
					},
				},
				Spec: tektonv1.PipelineRunSpec{
					PipelineRef: &tektonv1.PipelineRef{
						Name: "component-pipeline-invalid",
						ResolverRef: tektonv1.ResolverRef{
							Resolver: "bundle",
							Params: tektonv1.Params{
								{
									Name:  "bundle",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:component-pipeline-fail"},
								},
								{
									Name:  "name",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "test-task"},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, intgPipelineInvalidResult)).Should(Succeed())

			intgPipelineInvalidResult.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					CompletionTime: &metav1.Time{Time: time.Now()},
					ChildReferences: []tektonv1.ChildStatusReference{
						{
							Name:             taskRunInvalidResult.Name,
							PipelineTaskName: "task1",
						},
					},
				},
				Status: v1.Status{
					Conditions: v1.Conditions{
						apis.Condition{
							Reason: "Completed",
							Status: "True",
							Type:   apis.ConditionSucceeded,
						},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, intgPipelineInvalidResult)).Should(Succeed())

			adapter = NewAdapter(intgPipelineInvalidResult, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.AllTaskRunsWithMatchingPipelineRunLabelContextKey,
					Resource:   []tektonv1.TaskRun{*taskRunInvalidResult},
				},
			})
		})

		AfterEach(func() {
			err := k8sClient.Delete(ctx, intgPipelineInvalidResult)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
			err = k8sClient.Delete(ctx, taskRunInvalidResult)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		})

		It("ensures test status in snapshot is updated to failed", func() {
			status, detail, err := adapter.GetIntegrationPipelineRunStatus(adapter.client, adapter.context, intgPipelineInvalidResult)

			Expect(err).ToNot(HaveOccurred())
			Expect(status).To(Equal(intgteststat.IntegrationTestStatusTestFail))
			Expect(detail).To(ContainSubstring("Invalid result:"))
		})
	})

	When("GetIntegrationPipelineRunStatus is called with a PLR with TaskRun, mentioned in its ChildReferences field, missing from the cluster", func() {
		BeforeEach(func() {
			adapter = NewAdapter(integrationPipelineRunComponent, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
		})

		It("ensures test status in snapshot is updated to failed", func() {
			status, detail, err := adapter.GetIntegrationPipelineRunStatus(adapter.client, adapter.context, integrationPipelineRunComponent)
			Expect(err).ToNot(HaveOccurred())
			Expect(status).To(Equal(intgteststat.IntegrationTestStatusTestInvalid))
			Expect(detail).To(ContainSubstring(fmt.Sprintf("Failed to determine status of pipelinerun '%s', due to mismatch"+
				" in TaskRuns present in cluster (0) and those referenced within childReferences (1)", integrationPipelineRunComponent.Name)))
		})
	})

	When("GetIntegrationPipelineRunStatus is called with a PLR with TaskRun, mentioned in its ChildReferences field, present within the cluster", func() {
		BeforeEach(func() {
			adapter = NewAdapter(integrationPipelineRunComponent, hasApp, hasSnapshot, logger, loader.NewMockLoader(), k8sClient, ctx)
			adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
				{
					ContextKey: loader.AllTaskRunsWithMatchingPipelineRunLabelContextKey,
					Resource:   []tektonv1.TaskRun{*successfulTaskRun},
				},
			})
		})

		It("ensures test status in snapshot is updated to failed", func() {
			status, detail, err := adapter.GetIntegrationPipelineRunStatus(adapter.client, adapter.context, integrationPipelineRunComponent)
			Expect(err).ToNot(HaveOccurred())
			Expect(status).To(Equal(intgteststat.IntegrationTestStatusTestPassed))
			Expect(detail).To(ContainSubstring("Integration test passed"))
		})
	})
})
