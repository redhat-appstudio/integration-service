package component

import (
	"bytes"
	"reflect"
	"time"

	"github.com/tonglil/buflogr"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	toolkit "github.com/konflux-ci/operator-toolkit/loader"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/redhat-appstudio/integration-service/loader"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/redhat-appstudio/integration-service/helpers"
	"k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("Component Adapter", Ordered, func() {
	var (
		adapter *Adapter
		logger  helpers.IntegrationLogger

		hasApp   *applicationapiv1alpha1.Application
		hasComp  *applicationapiv1alpha1.Component
		hasComp2 *applicationapiv1alpha1.Component
	)
	const (
		SampleCommit             = "a2ba645d50e471d5f084b"
		SampleRepoLink           = "https://github.com/devfile-samples/devfile-sample-java-springboot-basic"
		sample_revision          = "random-value"
		SampleDigest             = "sha256:841328df1b9f8c4087adbdcfec6cc99ac8308805dea83f6d415d6fb8d40227c1"
		SampleImageWithoutDigest = "quay.io/redhat-appstudio/sample-image"
		SampleImage              = SampleImageWithoutDigest + "@" + SampleDigest
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

		hasComp = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-sample-2",
				Application:    hasApp.Name,
				ContainerImage: SampleImage,
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL:      SampleRepoLink,
							Revision: SampleCommit,
						},
					},
				},
			},
			Status: applicationapiv1alpha1.ComponentStatus{
				LastBuiltCommit: "",
			},
		}
		Expect(k8sClient.Create(ctx, hasComp)).Should(Succeed())

		hasComp2 = &applicationapiv1alpha1.Component{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "component-second-sample",
				Namespace: "default",
			},
			Spec: applicationapiv1alpha1.ComponentSpec{
				ComponentName:  "component-second-sample",
				Application:    "application-sample",
				ContainerImage: SampleImage,
				Source: applicationapiv1alpha1.ComponentSource{
					ComponentSourceUnion: applicationapiv1alpha1.ComponentSourceUnion{
						GitSource: &applicationapiv1alpha1.GitSource{
							URL: SampleRepoLink,
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, hasComp2)).Should(Succeed())
	})

	AfterAll(func() {
		err := k8sClient.Delete(ctx, hasApp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Delete(ctx, hasComp2)
		Expect(err == nil || errors.IsNotFound(err)).To(BeTrue())
	})

	It("can create a new Adapter instance", func() {
		Expect(reflect.TypeOf(NewAdapter(hasComp, hasApp, logger, loader.NewMockLoader(), k8sClient, ctx))).To(Equal(reflect.TypeOf(&Adapter{})))
	})
	It("ensures removing a component will result in a new snapshot being created", func() {
		buf := bytes.Buffer{}

		log := helpers.IntegrationLogger{Logger: buflogr.NewWithBuffer(&buf)}
		adapter = NewAdapter(hasComp, hasApp, log, loader.NewMockLoader(), k8sClient, ctx)
		adapter.context = toolkit.GetMockedContext(ctx, []toolkit.MockData{
			{
				ContextKey: loader.ApplicationContextKey,
				Resource:   hasApp,
			},
			{
				ContextKey: loader.ApplicationComponentsContextKey,
				Resource:   []applicationapiv1alpha1.Component{*hasComp, *hasComp2},
			},
		})
		snapshots := &applicationapiv1alpha1.SnapshotList{}
		Eventually(func() bool {
			Expect(k8sClient.List(ctx, snapshots, &client.ListOptions{Namespace: hasApp.Namespace})).To(Succeed())
			return len(snapshots.Items) == 0
		}, time.Second*20).Should(BeTrue())

		now := metav1.NewTime(metav1.Now().Add(time.Second * 1))
		hasComp.SetDeletionTimestamp(&now)

		result, err := adapter.EnsureComponentIsCleanedUp()

		Eventually(func() bool {
			Expect(k8sClient.List(ctx, snapshots, &client.ListOptions{Namespace: hasApp.Namespace})).To(Succeed())
			return !result.CancelRequest && len(snapshots.Items) == 1 && err == nil
		}, time.Second*20).Should(BeTrue())
	})

})
