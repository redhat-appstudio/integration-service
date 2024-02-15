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

package loader

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/redhat-appstudio/integration-service/api/v1beta1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"
	releasev1alpha1 "github.com/redhat-appstudio/release-service/api/v1alpha1"
)

var _ = Describe("Loader", Ordered, func() {
	var (
		loader                  ObjectLoader
		hasSnapshot             *applicationapiv1alpha1.Snapshot
		hasApp                  *applicationapiv1alpha1.Application
		hasComp                 *applicationapiv1alpha1.Component
		hasEnv                  *applicationapiv1alpha1.Environment
		deploymentTargetClass   *applicationapiv1alpha1.DeploymentTargetClass
		deploymentTarget        *applicationapiv1alpha1.DeploymentTarget
		deploymentTargetClaim   *applicationapiv1alpha1.DeploymentTargetClaim
		integrationTestScenario *v1beta1.IntegrationTestScenario
		successfulTaskRun       *tektonv1.TaskRun
		taskRunSample           *tektonv1.TaskRun
		buildPipelineRun        *tektonv1.PipelineRun
		integrationPipelineRun  *tektonv1.PipelineRun
		hasBinding              *applicationapiv1alpha1.SnapshotEnvironmentBinding
	)

	const (
		SampleRepoLink  = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		applicationName = "application-sample"
		snapshotName    = "snapshot-sample"
		sample_image    = "quay.io/redhat-appstudio/sample-image"
		sample_revision = "random-value"
		namespace       = "default"
	)

	BeforeAll(func() {
		loader = NewLoader()

		hasApp = &applicationapiv1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{
				Name:      applicationName,
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ApplicationSpec{
				DisplayName: "application-sample",
				Description: "This is an example application",
			},
		}
		Expect(k8sClient.Create(ctx, hasApp)).Should(Succeed())

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample",
				Application:    applicationName,
				ContainerImage: "",
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL: SampleRepoLink,
						},
					},
				},
			},
			Status: applicationapiv1alpha1.ComponentStatus{
				LastBuiltCommit: "",
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapshotName,
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:         "component",
					gitops.SnapshotComponentLabel:    "component-sample",
					gitops.BuildPipelineRunNameLabel: "pipelinerun-sample",
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
						ContainerImage: sample_image,
						Source: applicationapiv1alpha1.ComponentSource{
							ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
								GitSource: &applicationapiv1alpha1.GitSource{
									Revision: sample_revision,
								},
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

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

		Expect(k8sClient.Create(ctx, successfulTaskRun)).Should(Succeed())

		now := time.Now()
		successfulTaskRun.Status = tektonv1.TaskRunStatus{
			TaskRunStatusFields: tektonv1.TaskRunStatusFields{
				StartTime:      &metav1.Time{Time: now},
				CompletionTime: &metav1.Time{Time: now.Add(5 * time.Minute)},
				Results: []tektonv1.TaskRunResult{
					{
						Name: "HACBS_TEST_OUTPUT",
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

		deploymentTargetClass = &applicationapiv1alpha1.DeploymentTargetClass{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "dtcls" + "-",
			},
			Spec: applicationapiv1alpha1.DeploymentTargetClassSpec{
				Provisioner: "appstudio.redhat.com/devsandbox",
			},
		}
		Expect(k8sClient.Create(ctx, deploymentTargetClass)).Should(Succeed())

		deploymentTarget = &applicationapiv1alpha1.DeploymentTarget{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "dt" + "-",
				Namespace:    "default",
			},
			Spec: applicationapiv1alpha1.DeploymentTargetSpec{
				DeploymentTargetClassName: applicationapiv1alpha1.DeploymentTargetClassName(deploymentTargetClass.Name),
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
				DeploymentTargetClassName: applicationapiv1alpha1.DeploymentTargetClassName(deploymentTargetClass.Name),
				TargetName:                deploymentTarget.Name,
			},
		}
		Expect(k8sClient.Create(ctx, deploymentTargetClaim)).Should(Succeed())

		hasEnv = &applicationapiv1alpha1.Environment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-env",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTestScenarioLabel: integrationTestScenario.Name,
					gitops.SnapshotLabel:             hasSnapshot.Name,
				},
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
					Target: applicationapiv1alpha1.EnvironmentTarget{
						DeploymentTargetClaim: applicationapiv1alpha1.DeploymentTargetClaimConfig{
							ClaimName: deploymentTargetClaim.Name,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasEnv)).Should(Succeed())

		buildPipelineRun = &tektonv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pipelinerun-sample",
				Namespace: "default",
				Labels: map[string]string{
					"pipelines.appstudio.openshift.io/type": "build",
					"pipelines.openshift.io/used-by":        "build-cloud",
					"pipelines.openshift.io/runtime":        "nodejs",
					"pipelines.openshift.io/strategy":       "s2i",
					"appstudio.openshift.io/component":      "component-sample",
					"appstudio.openshift.io/application":    applicationName,
					"appstudio.openshift.io/snapshot":       snapshotName,
					"appstudio.openshift.io/environment":    hasEnv.Name,
					"test.appstudio.openshift.io/scenario":  integrationTestScenario.Name,
				},
				Annotations: map[string]string{
					"appstudio.redhat.com/updateComponentOnSuccess": "false",
					"appstudio.openshift.io/snapshot":               hasSnapshot.Name,
				},
			},
			Spec: tektonv1.PipelineRunSpec{
				PipelineRef: &tektonv1.PipelineRef{
					Name: "build-pipeline-pass",
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
				Params: []tektonv1.Param{
					{
						Name: "output-image",
						Value: tektonv1.ParamValue{
							Type:      tektonv1.ParamTypeString,
							StringVal: "quay.io/redhat-appstudio/sample-image",
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, buildPipelineRun)).Should(Succeed())

		buildPipelineRun.Status = tektonv1.PipelineRunStatus{
			PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
				ChildReferences: []tektonv1.ChildStatusReference{
					{
						Name:             successfulTaskRun.Name,
						PipelineTaskName: "task1",
					},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, buildPipelineRun)).Should(Succeed())

		integrationPipelineRun = &tektonv1.PipelineRun{
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
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/test-bundle:component-pipeline-pass"},
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

		Expect(k8sClient.Create(ctx, integrationPipelineRun)).Should(Succeed())

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

		taskRunSample = &tektonv1.TaskRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "taskrun-sample",
				Namespace: "default",
				Labels: map[string]string{
					"tekton.dev/pipelineRun": buildPipelineRun.Name,
				},
			},
		}
		Expect(k8sClient.Create(ctx, taskRunSample)).Should(Succeed())
	})

	AfterAll(func() {
		_ = k8sClient.Delete(ctx, hasSnapshot)
		_ = k8sClient.Delete(ctx, buildPipelineRun)
		_ = k8sClient.Delete(ctx, integrationPipelineRun)
		_ = k8sClient.Delete(ctx, successfulTaskRun)
		_ = k8sClient.Delete(ctx, hasEnv)
		_ = k8sClient.Delete(ctx, integrationTestScenario)
		_ = k8sClient.Delete(ctx, hasApp)
		_ = k8sClient.Delete(ctx, hasComp)
		_ = k8sClient.Delete(ctx, deploymentTargetClass)
		_ = k8sClient.Delete(ctx, deploymentTargetClaim)
		_ = k8sClient.Delete(ctx, deploymentTarget)
		_ = k8sClient.Delete(ctx, hasBinding)
		_ = k8sClient.Delete(ctx, taskRunSample)
	})

	createReleasePlan := func(releasePlan *releasev1alpha1.ReleasePlan) {
		Expect(k8sClient.Create(ctx, releasePlan)).Should(Succeed())

		// Wait for the release plan to be created
		Eventually(func() bool {
			tmpPlan := &releasev1alpha1.ReleasePlan{}
			err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: releasePlan.Namespace,
				Name:      releasePlan.Name,
			}, tmpPlan)
			return err == nil
		}, time.Second*10).Should(BeTrue())
	}

	deleteReleasePlan := func(releasePlan *releasev1alpha1.ReleasePlan) {
		err := k8sClient.Delete(ctx, releasePlan)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())

		// Wait for the release plan to be removed
		Eventually(func() bool {
			tmpPlan := &releasev1alpha1.ReleasePlan{}
			err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: releasePlan.Namespace,
				Name:      releasePlan.Name,
			}, tmpPlan)
			return k8serrors.IsNotFound(err)
		}, time.Second*10).Should(BeTrue())
	}

	It("ensures environments can be found", func() {
		environments, err := loader.GetAllEnvironments(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(environments).NotTo(BeNil())
	})

	It("ensures all Releases exists when HACBSTests succeeded", func() {
		Expect(k8sClient).NotTo(BeNil())
		Expect(ctx).NotTo(BeNil())
		Expect(hasSnapshot).NotTo(BeNil())
		err := gitops.MarkSnapshotAsPassed(k8sClient, ctx, hasSnapshot, "test passed")
		Expect(err).To(Succeed())
		Expect(gitops.HaveAppStudioTestsSucceeded(hasSnapshot)).To(BeTrue())

		// Normally we would Ensure that releases exist here, but that requires
		// importing the snapshot package which causes an import cycle

		releases, err := loader.GetReleasesWithSnapshot(k8sClient, ctx, hasSnapshot)
		Expect(err).To(BeNil())
		Expect(releases).NotTo(BeNil())
		for _, release := range *releases {
			Expect(k8sClient.Delete(ctx, &release)).Should(Succeed())
		}
	})

	It("ensures the Application Components can be found ", func() {
		applicationComponents, err := loader.GetAllApplicationComponents(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(applicationComponents).NotTo(BeNil())
	})

	It("ensures we can get an Application from a Snapshot ", func() {
		app, err := loader.GetApplicationFromSnapshot(k8sClient, ctx, hasSnapshot)
		Expect(err).To(BeNil())
		Expect(app).NotTo(BeNil())
		Expect(app.ObjectMeta).To(Equal(hasApp.ObjectMeta))
	})

	It("ensures we can get a Component from a Snapshot ", func() {
		comp, err := loader.GetComponentFromSnapshot(k8sClient, ctx, hasSnapshot)
		Expect(err).To(BeNil())
		Expect(comp).NotTo(BeNil())
		Expect(comp.ObjectMeta).To(Equal(hasComp.ObjectMeta))
	})

	It("ensures we can get a Component from a Pipeline Run ", func() {
		comp, err := loader.GetComponentFromPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).To(BeNil())
		Expect(comp).NotTo(BeNil())
		Expect(comp.ObjectMeta).To(Equal(hasComp.ObjectMeta))
	})

	It("ensures we can get the application from the Pipeline Run", func() {
		app, err := loader.GetApplicationFromPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).To(BeNil())
		Expect(app).NotTo(BeNil())
		Expect(app.ObjectMeta).To(Equal(hasApp.ObjectMeta))
	})

	It("ensures we can get the environment from the Pipeline Run", func() {
		env, err := loader.GetApplicationFromPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).To(BeNil())
		Expect(env).NotTo(BeNil())
	})

	It("ensures we can get the Application from a Component", func() {
		app, err := loader.GetApplicationFromComponent(k8sClient, ctx, hasComp)
		Expect(err).To(BeNil())
		Expect(app).NotTo(BeNil())
		Expect(app.ObjectMeta).To(Equal(hasApp.ObjectMeta))
	})

	It("ensures we can get the Snapshot from a Pipeline Run", func() {
		snapshot, err := loader.GetSnapshotFromPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).To(BeNil())
		Expect(snapshot).NotTo(BeNil())
		Expect(snapshot.ObjectMeta).To(Equal(hasSnapshot.ObjectMeta))
	})

	It("ensures we can get the Snapshot from a Pipeline Run", func() {
		snapshots, err := loader.GetAllSnapshotsForBuildPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).ToNot(HaveOccurred())
		Expect(snapshots).NotTo(BeNil())
		Expect(*snapshots).To(HaveLen(1))
		Expect((*snapshots)[0].Name).To(Equal(hasSnapshot.Name))
	})

	It("ensures we can get the Environment from a Pipeline Run", func() {
		env, err := loader.GetEnvironmentFromIntegrationPipelineRun(k8sClient, ctx, buildPipelineRun)
		Expect(err).To(BeNil())
		Expect(env).NotTo(BeNil())
		Expect(env.ObjectMeta).To(Equal(hasEnv.ObjectMeta))
	})

	It("can fetch all build pipelineRuns", func() {
		pipelineRuns, err := loader.GetAllBuildPipelineRunsForComponent(k8sClient, ctx, hasComp)
		Expect(err).To(BeNil())
		Expect(pipelineRuns).NotTo(BeNil())
		Expect(*pipelineRuns).To(HaveLen(1))
		Expect((*pipelineRuns)[0].Name).To(Equal(buildPipelineRun.Name))
	})

	It("can fetch all pipelineRuns for snapshot and scenario", func() {
		pipelineRuns, err := loader.GetAllPipelineRunsForSnapshotAndScenario(k8sClient, ctx, hasSnapshot, integrationTestScenario)
		Expect(err).To(BeNil())
		Expect(pipelineRuns).NotTo(BeNil())
		Expect(*pipelineRuns).To(HaveLen(1))
		Expect((*pipelineRuns)[0].Name).To(Equal(integrationPipelineRun.Name))
	})

	It("can fetch all integrationTestScenario for application", func() {
		integrationTestScenarios, err := loader.GetAllIntegrationTestScenariosForApplication(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(integrationTestScenarios).NotTo(BeNil())
		Expect(*integrationTestScenarios).To(HaveLen(1))
		Expect((*integrationTestScenarios)[0].Name).To(Equal(integrationTestScenario.Name))
	})

	It("can fetch required integrationTestScenario for application", func() {
		integrationTestScenarios, err := loader.GetRequiredIntegrationTestScenariosForApplication(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(integrationTestScenarios).NotTo(BeNil())
		Expect(*integrationTestScenarios).To(HaveLen(1))
		Expect((*integrationTestScenarios)[0].Name).To(Equal(integrationTestScenario.Name))
	})

	It("can fetch DeploymentTargetClaim for environment", func() {
		dtcls, err := loader.GetDeploymentTargetClaimForEnvironment(k8sClient, ctx, hasEnv)
		Expect(err).To(BeNil())
		Expect(dtcls.Name).To(Equal(deploymentTargetClaim.Name))
	})

	It("can fetch DeploymentTarget for DeploymentTargetClaim", func() {
		dt, err := loader.GetDeploymentTargetForDeploymentTargetClaim(k8sClient, ctx, deploymentTargetClaim)
		Expect(err).To(BeNil())
		Expect(dt.Name).To(Equal(deploymentTarget.Name))
	})

	It("can snapshotEnvironmentBinding for application and environment", func() {
		binding, err := loader.FindExistingSnapshotEnvironmentBinding(k8sClient, ctx, hasApp, hasEnv)
		Expect(err).To(BeNil())
		Expect(binding.Name).To(Equal(hasBinding.Name))
	})

	It("ensures that all Snapshots for a given application can be found", func() {
		snapshots, err := loader.GetAllSnapshots(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(*snapshots).To(HaveLen(1))
	})

	It("ensures the ReleasePlan can be gotten for Application", func() {
		gottenReleasePlanItems, err := loader.GetAutoReleasePlansForApplication(k8sClient, ctx, hasApp)
		Expect(err).To(BeNil())
		Expect(gottenReleasePlanItems).NotTo(BeNil())

	})

	It("can get all environments for integrationTestScenario", func() {
		environments, err := loader.GetAllEnvironmentsForScenario(k8sClient, ctx, integrationTestScenario)
		Expect(err).ToNot(HaveOccurred())
		Expect(*environments).To(HaveLen(1))
		Expect((*environments)[0].Name).To(Equal(hasEnv.Name))
	})

	It("can get all environments for integrationTestScenario", func() {
		environments, err := loader.GetAllEnvironmentsForScenario(k8sClient, ctx, integrationTestScenario)
		Expect(err).ToNot(HaveOccurred())
		Expect(*environments).To(HaveLen(1))
		Expect((*environments)[0].Name).To(Equal(hasEnv.Name))
	})

	It("can get all TaskRuns present in the cluster that are associated with the given pipelineRun", func() {
		taskRuns, err := loader.GetAllTaskRunsWithMatchingPipelineRunLabel(k8sClient, ctx, buildPipelineRun)
		Expect(err).ToNot(HaveOccurred())
		Expect(*taskRuns).To(HaveLen(1))
		Expect((*taskRuns)[0].Name).To(Equal(taskRunSample.Name))
	})

	When("release plan with auto-release label is created", func() {

		const (
			releasePlanName = "test-release-plan-with-label"
		)

		var (
			releasePlanWithLabel *releasev1alpha1.ReleasePlan
		)

		BeforeEach(func() {
			// Create ReleasePlan with "auto-release" label set to true
			releasePlanWithLabel = &releasev1alpha1.ReleasePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      releasePlanName,
					Namespace: hasApp.Namespace,
					Labels: map[string]string{
						"release.appstudio.openshift.io/auto-release": "true",
					},
				},
				Spec: releasev1alpha1.ReleasePlanSpec{
					Application: hasApp.Name,
					Target:      "default",
				},
			}
			createReleasePlan(releasePlanWithLabel)
		})

		AfterEach(func() {
			deleteReleasePlan(releasePlanWithLabel)
		})

		It("ensures the auto-release plans for application are returned correctly when the auto-release label is set to true", func() {
			// Get auto-release plans for application
			autoReleasePlans, err := loader.GetAutoReleasePlansForApplication(k8sClient, ctx, hasApp)
			Expect(err).To(BeNil())
			Expect(autoReleasePlans).ToNot(BeNil())
			Expect(*autoReleasePlans).To(HaveLen(1))
			Expect((*autoReleasePlans)[0].Name).To(Equal(releasePlanWithLabel.Name))

		})
	})

	When("release plan without auto-release label is created", func() {

		const (
			releasePlanName = "test-release-plan-no-label"
		)

		var (
			releasePlanNoLabel *releasev1alpha1.ReleasePlan
		)

		BeforeEach(func() {
			// Create ReleasePlan without "auto-release" label
			releasePlanNoLabel = &releasev1alpha1.ReleasePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      releasePlanName,
					Namespace: hasApp.Namespace,
				},
				Spec: releasev1alpha1.ReleasePlanSpec{
					Application: hasApp.Name,
					Target:      "default",
				},
			}
			createReleasePlan(releasePlanNoLabel)
		})

		AfterEach(func() {
			deleteReleasePlan(releasePlanNoLabel)
		})

		It("ensures the auto-release plans for application are returned correctly when the auto-release label is missing", func() {
			// Get auto-release plans for application
			autoReleasePlans, err := loader.GetAutoReleasePlansForApplication(k8sClient, ctx, hasApp)
			Expect(err).To(BeNil())
			Expect(autoReleasePlans).ToNot(BeNil())
			Expect(*autoReleasePlans).To(HaveLen(1))
			Expect((*autoReleasePlans)[0].Name).To(Equal(releasePlanNoLabel.Name))

		})
	})

	When("release plan with auto-release label is set to false", func() {

		const (
			releasePlanName = "test-release-plan-false-label"
		)

		var (
			releasePlanFalseLabel *releasev1alpha1.ReleasePlan
		)

		BeforeEach(func() {
			// Create ReleasePlan with the "auto-release" label set to false
			releasePlanFalseLabel = &releasev1alpha1.ReleasePlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      releasePlanName,
					Namespace: hasApp.Namespace,
					Labels: map[string]string{
						"release.appstudio.openshift.io/auto-release": "false",
					},
				},
				Spec: releasev1alpha1.ReleasePlanSpec{
					Application: hasApp.Name,
					Target:      "default",
				},
			}
			createReleasePlan(releasePlanFalseLabel)
		})

		AfterEach(func() {
			deleteReleasePlan(releasePlanFalseLabel)
		})

		It("ensures the auto-release plans for application are returned correctly when the auto-release label is set to false", func() {
			// Get auto-release plans for application
			autoReleasePlans, err := loader.GetAutoReleasePlansForApplication(k8sClient, ctx, hasApp)
			Expect(err).To(BeNil())
			Expect(autoReleasePlans).ToNot(BeNil())
			Expect(*autoReleasePlans).To(BeEmpty())
		})

		It("Can fetch integration test scenario", func() {
			fetchedScenario, err := loader.GetScenario(k8sClient, ctx, integrationTestScenario.Name, integrationTestScenario.Namespace)
			Expect(err).To(Succeed())
			Expect(fetchedScenario.Name).To(Equal(integrationTestScenario.Name))
			Expect(fetchedScenario.Namespace).To(Equal(integrationTestScenario.Namespace))
			Expect(fetchedScenario.Spec).To(Equal(integrationTestScenario.Spec))
		})
	})
})
