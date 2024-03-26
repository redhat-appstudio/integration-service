/*
Copyright 2022 Red Hat Inc.

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

package snapshot

import (
	"reflect"
	"time"

	"github.com/konflux-ci/operator-toolkit/metadata"

	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	crwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	"k8s.io/apimachinery/pkg/api/errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/gitops"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	klog "k8s.io/klog/v2"
)

var _ = Describe("SnapshotController", func() {
	var (
		manager            ctrl.Manager
		snapshotReconciler *Reconciler
		scheme             runtime.Scheme
		req                ctrl.Request
		hasApp             *applicationapiv1alpha1.Application
		hasComp            *applicationapiv1alpha1.Component
		hasSnapshot        *applicationapiv1alpha1.Snapshot
	)
	const (
		SampleRepoLink = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
	)

	BeforeEach(func() {

		applicationName := "application-sample"

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
				ComponentName: "component-sample",
				Application:   applicationName,
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
		hasSnapshot = &applicationapiv1alpha1.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "snapshot-sample",
				Namespace: "default",
				Labels: map[string]string{
					gitops.SnapshotTypeLabel:      "component",
					gitops.SnapshotComponentLabel: "component-sample",
				},
			},
			Spec: applicationapiv1alpha1.SnapshotSpec{
				Application: hasApp.Name,
				Components: []applicationapiv1alpha1.SnapshotComponent{
					{
						Name:           "component-sample",
						ContainerImage: "testimage",
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasSnapshot)).Should(Succeed())

		req = ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: "default",
				Name:      hasSnapshot.Name,
			},
		}

		webhookInstallOptions := &testEnv.WebhookInstallOptions

		klog.Info(webhookInstallOptions.LocalServingHost)
		klog.Info(webhookInstallOptions.LocalServingPort)
		klog.Info(webhookInstallOptions.LocalServingCertDir)

		var err error
		manager, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme: clientsetscheme.Scheme,
			WebhookServer: crwebhook.NewServer(crwebhook.Options{
				CertDir: webhookInstallOptions.LocalServingCertDir,
				Host:    webhookInstallOptions.LocalServingHost,
				Port:    webhookInstallOptions.LocalServingPort,
			}),
			Metrics: server.Options{
				BindAddress: "0", // disables metrics
			},
			LeaderElection: false,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(err).To(BeNil())

		snapshotReconciler = NewSnapshotReconciler(k8sClient, &logf.Log, &scheme)
	})
	AfterEach(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasSnapshot)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	It("can create and return a new Reconciler object", func() {
		Expect(reflect.TypeOf(snapshotReconciler)).To(Equal(reflect.TypeOf(&Reconciler{})))
	})

	It("can fail when Reconcile fails to prepare the adapter when snapshot is not found", func() {
		Expect(k8sClient.Delete(ctx, hasSnapshot)).Should(Succeed())
		Eventually(func() error {
			_, err := snapshotReconciler.Reconcile(ctx, req)
			return err
		}).Should(BeNil())
	})

	It("can Reconcile function prepare the adapter and return the result of the reconcile handling operation", func() {
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "non-existent",
				Namespace: "default",
			},
		}
		result, err := snapshotReconciler.Reconcile(ctx, req)
		Expect(reflect.TypeOf(result)).To(Equal(reflect.TypeOf(reconcile.Result{})))
		Expect(err).To(BeNil())
	})

	It("Does not return an error if the component cannot be found", func() {
		err := k8sClient.Delete(ctx, hasComp)
		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: hasComp.ObjectMeta.Namespace,
				Name:      hasComp.ObjectMeta.Name,
			}, hasComp)
			return err != nil
		}).Should(BeTrue())
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())

		result, err := snapshotReconciler.Reconcile(ctx, req)
		Expect(result).To(Equal(ctrl.Result{}))
		Expect(err).To(BeNil())
	})

	It("Does not return an error if the application cannot be found", func() {
		hasSnapshot.Spec.Application = string("im-a-ghost")
		Expect(k8sClient.Update(ctx, hasSnapshot)).Should(Succeed())

		result, err := snapshotReconciler.Reconcile(ctx, req)
		Expect(result).To(Equal(ctrl.Result{}))
		Expect(err).To(BeNil())
		Eventually(func() bool {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: hasSnapshot.Namespace,
				Name:      hasSnapshot.Name,
			}, hasSnapshot)
			return err == nil && gitops.IsSnapshotMarkedAsInvalid(hasSnapshot)
		}, time.Second*20).Should(BeTrue())
	})

	It("can setup the cache by adding a new index field to search for ReleasePlanAdmissions", func() {
		err := setupCache(manager)
		Expect(err).ToNot(HaveOccurred())
	})

	It("can setup a new controller manager with the given snapshotReconciler", func() {
		err := setupControllerWithManager(manager, snapshotReconciler)
		Expect(err).NotTo(HaveOccurred())
	})

	It("can setup a new Controller manager and start it", func() {
		err := SetupController(manager, &ctrl.Log)
		Expect(err).To(BeNil())
	})

	When("snapshot is restored from backup", func() {

		BeforeEach(func() {
			hasSnapshot.Labels["velero.io/restore-name"] = "something"
			Expect(k8sClient.Update(ctx, hasSnapshot)).To(Succeed())

			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{
					Namespace: hasSnapshot.Namespace,
					Name:      hasSnapshot.Name,
				}, hasSnapshot)
				return err == nil && metadata.HasLabel(hasSnapshot, "velero.io/restore-name")
			}, time.Second*20).Should(BeTrue())
		})

		It("stops reconciliation without error", func() {
			result, err := snapshotReconciler.Reconcile(ctx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).To(BeNil())
		})
	})

})
