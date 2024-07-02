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

package buildpipeline

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/konflux-ci/integration-service/gitops"
	"github.com/konflux-ci/integration-service/helpers"
	"github.com/konflux-ci/integration-service/loader"
	"github.com/konflux-ci/integration-service/tekton"
	"knative.dev/pkg/apis"
	v1 "knative.dev/pkg/apis/duck/v1"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/strings/slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ctrl "sigs.k8s.io/controller-runtime"

	toolkit "github.com/konflux-ci/operator-toolkit/loader"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	"github.com/tonglil/buflogr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Pipeline Adapter", Ordered, func() {
	var (
		adapter       *Adapter
		createAdapter func() *Adapter
		logger        helpers.IntegrationLogger

		successfulTaskRun *tektonv1.TaskRun
		failedTaskRun     *tektonv1.TaskRun
		buildPipelineRun  *tektonv1.PipelineRun
		buildPipelineRun2 *tektonv1.PipelineRun
		hasComp           *applicationapiv1alpha1.Component
		hasComp2          *applicationapiv1alpha1.Component
		hasApp            *applicationapiv1alpha1.Application
		hasSnapshot       *applicationapiv1alpha1.Snapshot
	)
	const (
		SampleRepoLink           = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		SampleCommit             = "a2ba645d50e471d5f084b"
		SampleDigest             = "sha256:841328df1b9f8c4087adbdcfec6cc99ac8308805dea83f6d415d6fb8d40227c1"
		SampleImageWithoutDigest = "quay.io/redhat-appstudio/sample-image"
		SampleImage              = SampleImageWithoutDigest + "@" + SampleDigest
		invalidDigest            = "invalidDigest"
	)

	BeforeAll(func() {
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

		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:            "component",
					gitops.SnapshotComponentLabel:       hasComp.Name,
					gitops.PipelineAsCodeEventTypeLabel: gitops.PipelineAsCodePullRequestType,
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
							{Name: "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
							},
							{Name: "name",
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
											"timestamp": "2024-05-22T06:42:21+00:00",
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
							{Name: "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
							},
							{Name: "name",
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
											"timestamp": "2024-05-22T06:42:21+00:00",
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
		buildPipelineRun = &tektonv1.PipelineRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pipelinerun-build-sample",
				Namespace: "default",
				Labels: map[string]string{
					"pipelines.appstudio.openshift.io/type":    "build",
					"pipelines.openshift.io/used-by":           "build-cloud",
					"pipelines.openshift.io/runtime":           "nodejs",
					"pipelines.openshift.io/strategy":          "s2i",
					"appstudio.openshift.io/component":         "component-sample",
					"pipelinesascode.tekton.dev/event-type":    "pull_request",
					"build.appstudio.redhat.com/target_branch": "main",
				},
				Annotations: map[string]string{
					"appstudio.redhat.com/updateComponentOnSuccess": "false",
					"pipelinesascode.tekton.dev/on-target-branch":   "[main,master]",
					"build.appstudio.openshift.io/repo":             "https://github.com/devfile-samples/devfile-sample-go-basic?rev=c713067b0e65fb3de50d1f7c457eb51c2ab0dbb0",
					"foo":                                           "bar",
					"chains.tekton.dev/signed":                      "true",
				},
			},
			Spec: tektonv1.PipelineRunSpec{
				PipelineRef: &tektonv1.PipelineRef{
					Name: "build-pipeline-pass",
					ResolverRef: tektonv1.ResolverRef{
						Resolver: "bundle",
						Params: tektonv1.Params{
							{Name: "bundle",
								Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
							},
							{Name: "name",
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
							StringVal: SampleImageWithoutDigest,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, buildPipelineRun)).Should(Succeed())

		buildPipelineRun.Status = tektonv1.PipelineRunStatus{
			PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
				Results: []tektonv1.PipelineRunResult{
					{
						Name:  "IMAGE_DIGEST",
						Value: *tektonv1.NewStructuredValues(SampleDigest),
					},
					{
						Name:  "IMAGE_URL",
						Value: *tektonv1.NewStructuredValues(SampleImageWithoutDigest),
					},
					{
						Name:  "CHAINS-GIT_URL",
						Value: *tektonv1.NewStructuredValues(SampleRepoLink),
					},
					{
						Name:  "CHAINS-GIT_COMMIT",
						Value: *tektonv1.NewStructuredValues(SampleCommit),
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
		Expect(k8sClient.Status().Update(ctx, buildPipelineRun)).Should(Succeed())
	})

	AfterEach(func() {
		err := k8sClient.Delete(ctx, buildPipelineRun)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, successfulTaskRun)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, failedTaskRun)
		Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
	})

	When("NewAdapter is called", func() {
		It("creates and return a new adapter", func() {
			Expect(reflect.TypeOf(NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient))).To(Equal(reflect.TypeOf(&Adapter{})))
		})
	})

	When("NewAdapter is created", func() {
		BeforeEach(func() {
			adapter = createAdapter()
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
					ContextKey: loader.GetPipelineRunContextKey,
					Resource:   buildPipelineRun,
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp, *hasComp2},
				},
			})
		})

		It("ensures the Imagepullspec and ComponentSource from pipelinerun and prepare snapshot can be created", func() {
			imagePullSpec, err := adapter.getImagePullSpecFromPipelineRun(buildPipelineRun)
			Expect(err).To(BeNil())
			Expect(imagePullSpec).NotTo(BeEmpty())

			componentSource, err := adapter.getComponentSourceFromPipelineRun(buildPipelineRun)
			Expect(err).To(BeNil())

			applicationComponents, err := adapter.loader.GetAllApplicationComponents(adapter.context, adapter.client, adapter.application)
			Expect(err).To(BeNil())
			Expect(applicationComponents).NotTo(BeNil())

			snapshot, err := gitops.PrepareSnapshot(adapter.context, adapter.client, hasApp, applicationComponents, hasComp, imagePullSpec, componentSource)
			Expect(snapshot).NotTo(BeNil())
			Expect(err).To(BeNil())
			Expect(snapshot).NotTo(BeNil())
			Expect(snapshot.Spec.Components).To(HaveLen(1), "One component should have been added to snapshot.  Other component should have been omited due to empty ContainerImage field or missing valid digest")
			Expect(snapshot.Spec.Components[0].Name).To(Equal(hasComp.Name), "The built component should have been added to the snapshot")
		})

		It("ensures that snapshot has label pointing to build pipelinerun", func() {
			expectedSnapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).To(BeNil())
			Expect(expectedSnapshot).NotTo(BeNil())

			Expect(expectedSnapshot.Labels).NotTo(BeNil())
			Expect(expectedSnapshot.Labels).Should(HaveKeyWithValue(Equal(gitops.BuildPipelineRunNameLabel), Equal(buildPipelineRun.Name)))
			Expect(expectedSnapshot.Labels).Should(HaveKeyWithValue(Equal(gitops.ApplicationNameLabel), Equal(hasApp.Name)))
		})

		It("ensures that Labels and Annotations were copied to snapshot from pipelinerun", func() {
			copyToSnapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).ToNot(HaveOccurred())
			Expect(copyToSnapshot).NotTo(BeNil())

			gitops.CopySnapshotLabelsAndAnnotation(hasApp, copyToSnapshot, hasComp.Name, &buildPipelineRun.ObjectMeta, gitops.BuildPipelineRunPrefix)
			Expect(copyToSnapshot.Labels[gitops.SnapshotTypeLabel]).To(Equal(gitops.SnapshotComponentType))
			Expect(copyToSnapshot.Labels[gitops.SnapshotComponentLabel]).To(Equal(hasComp.Name))
			Expect(copyToSnapshot.Labels[gitops.ApplicationNameLabel]).To(Equal(hasApp.Name))
			Expect(copyToSnapshot.Labels["build.appstudio.redhat.com/target_branch"]).To(Equal("main"))
			Expect(copyToSnapshot.Annotations["build.appstudio.openshift.io/repo"]).To(Equal("https://github.com/devfile-samples/devfile-sample-go-basic?rev=c713067b0e65fb3de50d1f7c457eb51c2ab0dbb0"))

		})

		It("ensure err is returned when pipelinerun doesn't have Result for ", func() {
			// We don't need to update the underlying resource on the control plane,
			// so we create a copy and modify its status. This prevents update conflicts in other tests.
			buildPipelineRunNoSource := buildPipelineRun.DeepCopy()
			buildPipelineRunNoSource.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					ChildReferences: []tektonv1.ChildStatusReference{
						{
							Name:             successfulTaskRun.Name,
							PipelineTaskName: "task1",
						},
					},
					Results: []tektonv1.PipelineRunResult{
						{
							Name:  "CHAINS-GIT_URL",
							Value: *tektonv1.NewStructuredValues(SampleRepoLink),
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

			componentSource, err := adapter.getComponentSourceFromPipelineRun(buildPipelineRunNoSource)
			Expect(componentSource).To(BeNil())
			Expect(err).ToNot(BeNil())
		})

		It("ensure err is returned when pipelinerun doesn't have Result for customized error and build pipelineRun annotated ", func() {
			// We don't need to update the underlying resource on the control plane,
			// so we create a copy and modify its status. This prevents update conflicts in other tests.
			buildPipelineRunNoSource := buildPipelineRun.DeepCopy()
			buildPipelineRunNoSource.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					ChildReferences: []tektonv1.ChildStatusReference{
						{
							Name:             successfulTaskRun.Name,
							PipelineTaskName: "task1",
						},
					},
					Results: []tektonv1.PipelineRunResult{
						{
							Name:  "CHAINS-GIT_URL",
							Value: *tektonv1.NewStructuredValues(SampleRepoLink),
						},
						{
							Name:  "IMAGE_URL",
							Value: *tektonv1.NewStructuredValues(SampleImageWithoutDigest),
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

			messageError := "Missing info IMAGE_DIGEST from pipelinerun pipelinerun-build-sample"
			var info map[string]string
			expectedSnap, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRunNoSource, hasComp, hasApp)
			Expect(expectedSnap).To(BeNil())
			Expect(err).To(HaveOccurred())
			err = tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(adapter.context, buildPipelineRun, adapter.client, err)
			Expect(err).NotTo(HaveOccurred())
			Expect(adapter.pipelineRun.GetAnnotations()[helpers.CreateSnapshotAnnotationName]).ToNot(BeNil())
			err = json.Unmarshal([]byte(adapter.pipelineRun.GetAnnotations()[helpers.CreateSnapshotAnnotationName]), &info)
			Expect(err).NotTo(HaveOccurred())
			Expect(info["status"]).To(Equal("failed"))
			Expect(info["message"]).To(Equal("Failed to create snapshot. Error: " + messageError))
		})

		It("ensures pipelines as code labels and annotations are propagated to the snapshot", func() {
			snapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).To(BeNil())
			Expect(snapshot).ToNot(BeNil())
			annotation, found := snapshot.GetAnnotations()["pac.test.appstudio.openshift.io/on-target-branch"]
			Expect(found).To(BeTrue())
			Expect(annotation).To(Equal("[main,master]"))
			label, found := snapshot.GetLabels()["pac.test.appstudio.openshift.io/event-type"]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal("pull_request"))
		})

		It("ensures non-pipelines as code labels and annotations are NOT propagated to the snapshot", func() {
			snapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).To(BeNil())
			Expect(snapshot).ToNot(BeNil())

			// non-PaC labels are not copied
			_, found := buildPipelineRun.GetLabels()["pipelines.appstudio.openshift.io/type"]
			Expect(found).To(BeTrue())
			_, found = snapshot.GetLabels()["pipelines.appstudio.openshift.io/type"]
			Expect(found).To(BeFalse())

			// non-PaC annotations are not copied
			_, found = buildPipelineRun.GetAnnotations()["foo"]
			Expect(found).To(BeTrue())
			_, found = snapshot.GetAnnotations()["foo"]
			Expect(found).To(BeFalse())
		})

		It("ensures build labels and annotations prefixed with 'build.appstudio' are propagated to the snapshot", func() {
			snapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).To(BeNil())
			Expect(snapshot).ToNot(BeNil())

			annotation, found := snapshot.GetAnnotations()["build.appstudio.openshift.io/repo"]
			Expect(found).To(BeTrue())
			Expect(annotation).To(Equal("https://github.com/devfile-samples/devfile-sample-go-basic?rev=c713067b0e65fb3de50d1f7c457eb51c2ab0dbb0"))

			label, found := snapshot.GetLabels()["build.appstudio.redhat.com/target_branch"]
			Expect(found).To(BeTrue())
			Expect(label).To(Equal("main"))
		})

		It("ensures build labels and annotations non-prefixed with 'build.appstudio' are NOT propagated to the snapshot", func() {
			snapshot, err := adapter.prepareSnapshotForPipelineRun(buildPipelineRun, hasComp, hasApp)
			Expect(err).To(BeNil())
			Expect(snapshot).ToNot(BeNil())

			// build annotations non-prefixed with 'build.appstudio' are not copied
			_, found := buildPipelineRun.GetAnnotations()["appstudio.redhat.com/updateComponentOnSuccess"]
			Expect(found).To(BeTrue())
			_, found = snapshot.GetAnnotations()["appstudio.redhat.com/updateComponentOnSuccess"]
			Expect(found).To(BeFalse())

			// build labels non-prefixed with 'build.appstudio' are not copied
			_, found = buildPipelineRun.GetLabels()["pipelines.appstudio.openshift.io/type"]
			Expect(found).To(BeTrue())
			_, found = snapshot.GetLabels()["pipelines.appstudio.openshift.io/type"]
			Expect(found).To(BeFalse())
		})

		It("ensure snapshot will not be created in instance when chains is incomplete", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
			buildPipelineRun.ObjectMeta.Annotations = map[string]string{
				"appstudio.redhat.com/updateComponentOnSuccess": "false",
				"pipelinesascode.tekton.dev/on-target-branch":   "[main,master]",
				"build.appstudio.openshift.io/repo":             "https://github.com/devfile-samples/devfile-sample-go-basic?rev=c713067b0e65fb3de50d1f7c457eb51c2ab0dbb0",
				"foo":                                           "bar",
			}
			adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, log, loader.NewMockLoader(), k8sClient)

			Eventually(func() bool {
				result, err := adapter.EnsureSnapshotExists()
				return !result.CancelRequest && err != nil
			}, time.Second*10).Should(BeTrue())

			expectedLogEntry := "Not processing the pipelineRun because it's not yet signed with Chains"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			unexpectedLogEntry := "Created new Snapshot"
			Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
		})

		It("ensure error info is added to build pipelineRun annotation", func() {
			buildPipelineRun.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					Results: []tektonv1.PipelineRunResult{
						{
							Name:  "IMAGE_DIGEST",
							Value: *tektonv1.NewStructuredValues(invalidDigest),
						},
						{
							Name:  "IMAGE_URL",
							Value: *tektonv1.NewStructuredValues(SampleImageWithoutDigest),
						},
						{
							Name:  "CHAINS-GIT_URL",
							Value: *tektonv1.NewStructuredValues(SampleRepoLink),
						},
						{
							Name:  "CHAINS-GIT_COMMIT",
							Value: *tektonv1.NewStructuredValues(SampleCommit),
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
			Expect(k8sClient.Status().Update(ctx, buildPipelineRun)).Should(Succeed())
			adapter = createAdapter()
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
					ContextKey: loader.GetPipelineRunContextKey,
					Resource:   buildPipelineRun,
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
			})
			_, err := adapter.prepareSnapshotForPipelineRun(adapter.pipelineRun, adapter.component, adapter.application)
			Expect(helpers.IsInvalidImageDigestError(err)).To(BeTrue())
			Eventually(func() bool {
				result, err := adapter.EnsureSnapshotExists()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())
			Expect(adapter.pipelineRun.GetAnnotations()[helpers.CreateSnapshotAnnotationName]).ToNot(BeNil())
			var info map[string]string
			err = json.Unmarshal([]byte(adapter.pipelineRun.GetAnnotations()[helpers.CreateSnapshotAnnotationName]), &info)
			Expect(err).NotTo(HaveOccurred())
			invalidDigestError := helpers.NewInvalidImageDigestError(hasComp.Name, SampleImageWithoutDigest+"@"+invalidDigest)
			Expect(info["status"]).To(Equal("failed"))
			Expect(info["message"]).Should(ContainSubstring(invalidDigestError.Error()))
		})
	})

	When("Snapshot already exists", func() {
		It("ensures snapshot creation is skipped when snapshot already exists", func() {
			var buf bytes.Buffer
			log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}

			// check the behavior when there are multiple Snapshots associated with the build pipelineRun
			adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, log, loader.NewMockLoader(), k8sClient)
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
					ContextKey: loader.GetPipelineRunContextKey,
					Resource:   buildPipelineRun,
				},
				{
					ContextKey: loader.PipelineRunsContextKey,
					Resource:   []tektonv1.PipelineRun{*buildPipelineRun},
				},
				{
					ContextKey: loader.AllSnapshotsForBuildPipelineRunContextKey,
					Resource:   []applicationapiv1alpha1.Snapshot{*hasSnapshot, *hasSnapshot},
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
			})

			Eventually(func() bool {
				result, err := adapter.EnsureSnapshotExists()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			expectedLogEntry := "The build pipelineRun is already associated with more than one existing Snapshot"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			unexpectedLogEntry := "Created new Snapshot"
			Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))

			// check the behavior when there is only one Snapshot associated with the build pipelineRun
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
					ContextKey: loader.GetPipelineRunContextKey,
					Resource:   buildPipelineRun,
				},
				{
					ContextKey: loader.PipelineRunsContextKey,
					Resource:   []tektonv1.PipelineRun{*buildPipelineRun},
				},
				{
					ContextKey: loader.AllSnapshotsForBuildPipelineRunContextKey,
					Resource:   []applicationapiv1alpha1.Snapshot{*hasSnapshot},
				},
				{
					ContextKey: loader.ApplicationComponentsContextKey,
					Resource:   []applicationapiv1alpha1.Component{*hasComp},
				},
			})

			Eventually(func() bool {
				result, err := adapter.EnsureSnapshotExists()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			expectedLogEntry = "There is an existing Snapshot associated with this build pipelineRun, but the pipelineRun is not yet annotated"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			expectedLogEntry = "Updated build pipelineRun"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			unexpectedLogEntry = "Created new Snapshot"
			Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))

			// The previous call should have added the Snapshot annotation to the buildPipelineRun
			// now we test if that is detected correctly
			Eventually(func() bool {
				result, err := adapter.EnsureSnapshotExists()
				return !result.CancelRequest && err == nil
			}, time.Second*10).Should(BeTrue())

			expectedLogEntry = "The build pipelineRun is already associated with existing Snapshot via annotation"
			Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
			unexpectedLogEntry = "Created new Snapshot"
			Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
		})
	})

	When("multiple succesfull build pipeline runs exists for the same component", func() {
		BeforeAll(func() {
			buildPipelineRun2 = &tektonv1.PipelineRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pipelinerun-build-sample-2",
					Namespace: "default",
					Labels: map[string]string{
						"pipelines.appstudio.openshift.io/type": "build",
						"pipelines.openshift.io/used-by":        "build-cloud",
						"pipelines.openshift.io/runtime":        "nodejs",
						"pipelines.openshift.io/strategy":       "s2i",
						"appstudio.openshift.io/component":      "component-sample",
						"pipelinesascode.tekton.dev/event-type": "pull_request",
					},
					Annotations: map[string]string{
						"appstudio.redhat.com/updateComponentOnSuccess": "false",
						"pipelinesascode.tekton.dev/on-target-branch":   "[main,master]",
						"foo": "bar",
					},
				},
				Spec: tektonv1.PipelineRunSpec{
					PipelineRef: &tektonv1.PipelineRef{
						Name: "build-pipeline-pass",
						ResolverRef: tektonv1.ResolverRef{
							Resolver: "bundle",
							Params: tektonv1.Params{
								{Name: "bundle",
									Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
								},
								{Name: "name",
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
								StringVal: SampleImageWithoutDigest,
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, buildPipelineRun2)).Should(Succeed())

			buildPipelineRun2.Status = tektonv1.PipelineRunStatus{
				PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
					Results: []tektonv1.PipelineRunResult{
						{
							Name:  "IMAGE_DIGEST",
							Value: *tektonv1.NewStructuredValues(SampleDigest),
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
			Expect(k8sClient.Status().Update(ctx, buildPipelineRun2)).Should(Succeed())
			Expect(helpers.HasPipelineRunSucceeded(buildPipelineRun2)).To(BeTrue())
		})

		AfterAll(func() {
			err := k8sClient.Delete(ctx, buildPipelineRun2)
			Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
		})

		It("can detect if a PipelineRun has succeeded", func() {
			buildPipelineRun.Status.SetCondition(&apis.Condition{
				Type:   apis.ConditionSucceeded,
				Status: "False",
			})
			Expect(helpers.HasPipelineRunSucceeded(buildPipelineRun)).To(BeFalse())
			buildPipelineRun.Status.SetCondition(&apis.Condition{
				Type:   apis.ConditionSucceeded,
				Status: "True",
			})
			Expect(helpers.HasPipelineRunSucceeded(buildPipelineRun)).To(BeTrue())
			Expect(helpers.HasPipelineRunSucceeded(&tektonv1.TaskRun{})).To(BeFalse())
		})

		It("Can add an annotation to the build pipelinerun", func() {
			err := tekton.AnnotateBuildPipelineRun(adapter.context, buildPipelineRun, "test", "value", adapter.client)
			Expect(err).NotTo(HaveOccurred())
			Expect(buildPipelineRun.ObjectMeta.Annotations["test"]).To(Equal("value"))
		})

		It("can annotate the build pipelineRun with the Snapshot name", func() {
			adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
			err := adapter.annotateBuildPipelineRunWithSnapshot(hasSnapshot)
			Expect(err).To(BeNil())
			Expect(adapter.pipelineRun.ObjectMeta.Annotations[tekton.SnapshotNameLabel]).To(Equal(hasSnapshot.Name))
		})

		It("Can annotate the build pipelineRun with the CreateSnapshot annotate", func() {
			sampleErr := errors.New("this is a sample error")
			adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
			err := tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(adapter.context, buildPipelineRun, adapter.client, sampleErr)
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(3 * time.Second)
			// Get pipeline run from cluster
			newPipelineRun := new(tektonv1.PipelineRun)
			err = k8sClient.Get(ctx, types.NamespacedName{
				Namespace: buildPipelineRun.ObjectMeta.Namespace,
				Name:      buildPipelineRun.ObjectMeta.Name,
			}, newPipelineRun)
			Expect(err).NotTo(HaveOccurred())

			// Check that annotation from pipelineRun contains the JSON string we expect
			Expect(newPipelineRun.ObjectMeta.Annotations[helpers.CreateSnapshotAnnotationName]).NotTo(BeNil())
			var info map[string]string
			err = json.Unmarshal([]byte(newPipelineRun.ObjectMeta.Annotations[helpers.CreateSnapshotAnnotationName]), &info)
			Expect(err).NotTo(HaveOccurred())
			Expect(info["status"]).To(Equal("failed"))
			Expect(info["message"]).To(Equal("Failed to create snapshot. Error: " + sampleErr.Error()))

			// Check that an attempt to modify a pipelineRun that's being deleted doesn't do anything
			newPipelineRun.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			newSampleErr := errors.New("this is a different sample error")
			err = tekton.AnnotateBuildPipelineRunWithCreateSnapshotAnnotation(adapter.context, newPipelineRun, adapter.client, newSampleErr)
			Expect(err).NotTo(HaveOccurred())
			Expect(info["status"]).To(Equal("failed"))
			Expect(info["message"]).To(Equal("Failed to create snapshot. Error: " + sampleErr.Error()))
		})

		It("can find matching snapshot", func() {
			// make sure the first pipeline started as first
			adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
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
					ContextKey: loader.AllSnapshotsContextKey,
					Resource:   []applicationapiv1alpha1.Snapshot{*hasSnapshot},
				},
			})
			allSnapshots, err := adapter.loader.GetAllSnapshots(adapter.context, adapter.client, adapter.application)
			Expect(err).To(BeNil())
			Expect(allSnapshots).NotTo(BeNil())
			existingSnapshot := gitops.FindMatchingSnapshot(hasApp, allSnapshots, hasSnapshot)
			Expect(existingSnapshot).NotTo(BeNil())
			Expect(existingSnapshot.Name).To(Equal(hasSnapshot.Name))
		})
	})

	When("A new Build pipelineRun is created", func() {

		When("can add and remove finalizers from the pipelineRun", func() {
			BeforeEach(func() {
				adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
			})
			It("can add and remove finalizers from build pipelineRun", func() {
				// Mark build PLR as incomplete
				buildPipelineRun.Status.Conditions = nil
				Expect(k8sClient.Status().Update(ctx, buildPipelineRun)).Should(Succeed())

				// Ensure PLR does not have finalizer
				existingBuildPLR := new(tektonv1.PipelineRun)
				err := k8sClient.Get(ctx, types.NamespacedName{
					Namespace: buildPipelineRun.ObjectMeta.Namespace,
					Name:      buildPipelineRun.ObjectMeta.Name,
				}, existingBuildPLR)
				Expect(err).ToNot(HaveOccurred())
				Expect(existingBuildPLR.ObjectMeta.Finalizers).To(BeNil())

				// Add Finalizer to PLR
				result, err := adapter.EnsurePipelineIsFinalized()
				Expect(result.CancelRequest).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())

				// Ensure PLR has finalizer
				Eventually(func() bool {
					return slices.Contains(buildPipelineRun.ObjectMeta.Finalizers, helpers.IntegrationPipelineRunFinalizer)
				}, time.Second*10).Should(BeTrue())

				// Update build PLR as completed
				buildPipelineRun.Status = tektonv1.PipelineRunStatus{
					PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
						ChildReferences: []tektonv1.ChildStatusReference{
							{
								Name:             successfulTaskRun.Name,
								PipelineTaskName: "task1",
							},
						},
						Results: []tektonv1.PipelineRunResult{
							{
								Name:  "CHAINS-GIT_URL",
								Value: *tektonv1.NewStructuredValues(SampleRepoLink),
							},
							{
								Name:  "IMAGE_URL",
								Value: *tektonv1.NewStructuredValues(SampleImageWithoutDigest),
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
				Expect(k8sClient.Status().Update(ctx, buildPipelineRun)).Should(Succeed())

				Eventually(func() bool {
					result, err := adapter.EnsureSnapshotExists()
					return !result.CancelRequest && err == nil
				}, time.Second*10).Should(BeTrue())
				// Ensure the PLR on the control plane does not have finalizer
				Eventually(func() bool {
					updatedBuildPLR := new(tektonv1.PipelineRun)
					err := k8sClient.Get(ctx, types.NamespacedName{
						Namespace: buildPipelineRun.Namespace,
						Name:      buildPipelineRun.Name,
					}, updatedBuildPLR)
					return err == nil && !controllerutil.ContainsFinalizer(updatedBuildPLR, helpers.IntegrationPipelineRunFinalizer)
				}, time.Second*20).Should(BeTrue())
			})

		})

		When("running pipeline with deletion timestamp is processed", func() {

			var runningDeletingBuildPipeline *tektonv1.PipelineRun

			BeforeEach(func() {
				runningDeletingBuildPipeline = &tektonv1.PipelineRun{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pipelinerun-build-running-deleting",
						Namespace: "default",
						Labels: map[string]string{
							"pipelines.appstudio.openshift.io/type":    "build",
							"pipelines.openshift.io/used-by":           "build-cloud",
							"pipelines.openshift.io/runtime":           "nodejs",
							"pipelines.openshift.io/strategy":          "s2i",
							"appstudio.openshift.io/component":         "component-sample",
							"pipelinesascode.tekton.dev/event-type":    "pull_request",
							"build.appstudio.redhat.com/target_branch": "main",
						},
						Annotations: map[string]string{
							"appstudio.redhat.com/updateComponentOnSuccess": "false",
							"pipelinesascode.tekton.dev/on-target-branch":   "[main,master]",
							"build.appstudio.openshift.io/repo":             "https://github.com/devfile-samples/devfile-sample-go-basic?rev=c713067b0e65fb3de50d1f7c457eb51c2ab0dbb0",
							"foo":                                           "bar",
						},
					},
					Spec: tektonv1.PipelineRunSpec{
						PipelineRef: &tektonv1.PipelineRef{
							Name: "build-pipeline-pass",
							ResolverRef: tektonv1.ResolverRef{
								Resolver: "bundle",
								Params: tektonv1.Params{
									{Name: "bundle",
										Value: tektonv1.ParamValue{Type: "string", StringVal: "quay.io/redhat-appstudio/example-tekton-bundle:test"},
									},
									{Name: "name",
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
									StringVal: SampleImageWithoutDigest,
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, runningDeletingBuildPipeline)).Should(Succeed())

				runningDeletingBuildPipeline.Status = tektonv1.PipelineRunStatus{
					PipelineRunStatusFields: tektonv1.PipelineRunStatusFields{
						Results: []tektonv1.PipelineRunResult{},
					},
					Status: v1.Status{
						Conditions: v1.Conditions{
							apis.Condition{
								Reason: "Running",
								Status: "Unknown",
								Type:   apis.ConditionSucceeded,
							},
						},
					},
				}
				Expect(k8sClient.Status().Update(ctx, runningDeletingBuildPipeline)).Should(Succeed())

				adapter = NewAdapter(ctx, runningDeletingBuildPipeline, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.GetPipelineRunContextKey,
						Resource:   runningDeletingBuildPipeline,
					},
				})
			})

			AfterEach(func() {
				err := k8sClient.Delete(ctx, runningDeletingBuildPipeline)
				Expect(err == nil || k8serrors.IsNotFound(err)).To(BeTrue())
			})
			// tekton is keeping deleted pipelines in running state, we have to remove finalizer in this case
			It("removes finalizer", func() {
				// Add Finalizer to PLR
				result, err := adapter.EnsurePipelineIsFinalized()
				Expect(result.CancelRequest).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
				// make sure the finelizer is there
				Expect(controllerutil.ContainsFinalizer(runningDeletingBuildPipeline, helpers.IntegrationPipelineRunFinalizer)).To(BeTrue())

				// deletionTimestamp must be set here, create client call in BeforeEach() removes it
				runningDeletingBuildPipeline.DeletionTimestamp = &metav1.Time{Time: time.Now()}
				Eventually(func() bool {
					result, err := adapter.EnsureSnapshotExists()
					return !result.CancelRequest && err == nil
				}, time.Second*10).Should(BeTrue())
				Expect(controllerutil.ContainsFinalizer(runningDeletingBuildPipeline, helpers.IntegrationPipelineRunFinalizer)).To(BeFalse())
			})

		})

		When("attempting to update a problematic build pipelineRun", func() {
			It("handles an already deleted build pipelineRun", func() {
				notFoundErr := new(k8serrors.StatusError)
				notFoundErr.ErrStatus = metav1.Status{
					Message: "Resource Not Found",
					Code:    404,
					Status:  "Failure",
					Reason:  metav1.StatusReasonNotFound,
				}

				var buf bytes.Buffer
				log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
				buildPipelineRun.ObjectMeta.Annotations[gitops.SnapshotLabel] = hasSnapshot.Name
				adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, log, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.GetPipelineRunContextKey,
						Resource:   nil,
						Err:        notFoundErr,
					},
				})

				Eventually(func() bool {
					result, err := adapter.EnsureSnapshotExists()
					return !result.RequeueRequest && err == nil
				}, time.Second*10).Should(BeTrue())

				expectedLogEntry := "The build pipelineRun is already associated with existing Snapshot via annotation"
				Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
				unexpectedLogEntry := "Created new Snapshot"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Updated build pipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Removed Finalizer from the PipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
			})
			It("handles a build pipelineRun that has Snapshot annotation, but runs into conflicts updating status", func() {
				conflictErr := new(k8serrors.StatusError)
				conflictErr.ErrStatus = metav1.Status{
					Message: "Operation cannot be fulfilled",
					Code:    409,
					Status:  "Failure",
					Reason:  metav1.StatusReasonConflict,
				}

				var buf bytes.Buffer
				log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
				buildPipelineRun.ObjectMeta.Annotations[gitops.SnapshotLabel] = hasSnapshot.Name
				adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, log, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.GetPipelineRunContextKey,
						Resource:   buildPipelineRun,
						Err:        conflictErr,
					},
				})

				Eventually(func() bool {
					result, err := adapter.EnsureSnapshotExists()
					return result.RequeueRequest && err != nil
				}, time.Second*10).Should(BeTrue())

				expectedLogEntry := "The build pipelineRun is already associated with existing Snapshot via annotation"
				Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))
				expectedLogEntry = "Operation cannot be fulfilled"
				Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

				unexpectedLogEntry := "Created new Snapshot"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Updated build pipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Removed Finalizer from the PipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
			})
			It("handles a build pipelineRun without Snapshot annotation that runs into conflicts when trying to add it", func() {
				conflictErr := new(k8serrors.StatusError)
				conflictErr.ErrStatus = metav1.Status{
					Message: "Operation cannot be fulfilled",
					Code:    409,
					Status:  "Failure",
					Reason:  metav1.StatusReasonConflict,
				}

				var buf bytes.Buffer
				log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
				adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, log, loader.NewMockLoader(), k8sClient)
				adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
					{
						ContextKey: loader.GetPipelineRunContextKey,
						Resource:   buildPipelineRun,
						Err:        conflictErr,
					},
					{
						ContextKey: loader.AllSnapshotsContextKey,
						Resource:   []applicationapiv1alpha1.Snapshot{*hasSnapshot},
					},
				})

				Eventually(func() bool {
					result, err := adapter.EnsureSnapshotExists()
					return result.RequeueRequest && err != nil
				}, time.Second*10).Should(BeTrue())

				expectedLogEntry := "Operation cannot be fulfilled"
				Expect(buf.String()).Should(ContainSubstring(expectedLogEntry))

				unexpectedLogEntry := "The build pipelineRun is already associated with existing Snapshot via annotation"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Created new Snapshot"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Updated build pipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
				unexpectedLogEntry = "Removed Finalizer from the PipelineRun"
				Expect(buf.String()).ShouldNot(ContainSubstring(unexpectedLogEntry))
			})
		})
	})

	createAdapter = func() *Adapter {
		adapter = NewAdapter(ctx, buildPipelineRun, hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient)
		return adapter
	}
})
