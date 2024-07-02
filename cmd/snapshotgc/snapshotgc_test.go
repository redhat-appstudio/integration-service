package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	gomonkey "github.com/agiledragon/gomonkey/v2"
	"github.com/go-logr/logr"
	releasev1alpha1 "github.com/konflux-ci/release-service/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	"github.com/tonglil/buflogr"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("Test garbage collection for snapshots", func() {
	var buf bytes.Buffer
	var logger logr.Logger

	BeforeEach(func() {
		buf.Reset()
		logger = buflogr.NewWithBuffer(&buf)
	})

	Describe("Test getSnapshotsForNSReleases", func() {
		It("Finds no snapshot when there are no releases", func() {

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&releasev1alpha1.ReleaseList{
						Items: []releasev1alpha1.Release{},
					}).Build()
			snapToData := make(map[string]snapshotData)
			output, err := getSnapshotsForNSReleases(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("Finds a snapshot for a single release", func() {
			rel1 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "my-release",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "my-snapshot",
				},
			}
			rel2 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "another-ns-release",
					Namespace: "another-ns",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "another-snapshot",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&releasev1alpha1.ReleaseList{
						Items: []releasev1alpha1.Release{*rel1, *rel2},
					}).Build()
			snapToData := make(map[string]snapshotData)
			output, err := getSnapshotsForNSReleases(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(HaveLen(1))
			Expect(output["my-snapshot"].release.Name).To(Equal("my-release"))
		})

		It("Finds multiple snapshots for multiple releases", func() {
			rel1 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel1",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "snap1",
				},
			}
			rel2 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel2",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "snap2",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&releasev1alpha1.ReleaseList{
						Items: []releasev1alpha1.Release{*rel1, *rel2},
					}).Build()
			snapToData := make(map[string]snapshotData)
			output, err := getSnapshotsForNSReleases(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(HaveLen(2))
			Expect(output["snap1"].release.Name).To(Equal("rel1"))
			Expect(output["snap2"].release.Name).To(Equal("rel2"))
		})

		It("Returns error when fails API call", func() {
			cl := fake.NewClientBuilder().Build()
			snapToData := make(map[string]snapshotData)
			_, err := getSnapshotsForNSReleases(cl, snapToData, "ns1", logger)

			Expect(err).Should(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(
				"no kind is registered for the type v1alpha1.ReleaseList in scheme",
			)))
		})
	})

	Describe("Test getUnassociatedNSSnapshots", func() {
		It("Finds no unassociated snapshots when there are no snapshots", func() {

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{},
					}).Build()
			snapToData := make(map[string]snapshotData)
			output, err := getUnassociatedNSSnapshots(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("Finds no unassociated snapshots when all are associated", func() {
			snap1 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "associated-snapshot",
					Namespace: "ns1",
				},
			}
			snap2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "another-associated-snapshot",
					Namespace: "ns1",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{*snap1, *snap2},
					}).Build()
			snapToData := make(map[string]snapshotData)
			snapToData["associated-snapshot"] = snapshotData{}
			snapToData["another-associated-snapshot"] = snapshotData{}
			output, err := getUnassociatedNSSnapshots(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(BeEmpty())
		})

		It("Finds unassociated snapshots when some snapshots are associated", func() {
			snap1 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "associated-snapshot",
					Namespace: "ns1",
				},
			}
			snap2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "another-associated-snapshot",
					Namespace: "ns1",
				},
			}
			snap3 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unassociated-snapshot",
					Namespace: "ns1",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*snap1, *snap2, *snap3,
						},
					}).Build()
			snapToData := make(map[string]snapshotData)
			snapToData["associated-snapshot"] = snapshotData{}
			snapToData["another-associated-snapshot"] = snapshotData{}
			output, err := getUnassociatedNSSnapshots(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(HaveLen(1))
			Expect(output[0].Name).To(Equal("unassociated-snapshot"))
		})

		It("Finds unassociated snapshots when no snapshots are associated", func() {
			snap1 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "unassociated-snapshot",
					Namespace: "ns1",
				},
			}
			snap2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "another-unassociated-snapshot",
					Namespace: "ns1",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{*snap1, *snap2},
					}).Build()
			snapToData := make(map[string]snapshotData)
			output, err := getUnassociatedNSSnapshots(cl, snapToData, "ns1", logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(output).To(HaveLen(2))
		})

		It("Returns error when fails API call", func() {

			cl := fake.NewClientBuilder().Build()
			snapToData := make(map[string]snapshotData)
			_, err := getUnassociatedNSSnapshots(cl, snapToData, "ns1", logger)

			Expect(err).Should(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(
				"no kind is registered for the type v1alpha1.SnapshotList in scheme",
			)))
		})
	})
	Describe("Test getSnapshotsForRemoval", func() {
		It("Handles no snapshots", func() {

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{}
			output := getSnapshotsForRemoval(cl, candidates, 2, 1, logger)

			Expect(output).To(BeEmpty())
		})

		It("No PR snapshots, some non-PR snapshots. Not enough to GCed", func() {
			currentTime := time.Now()
			newerSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "push",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerSnap, *olderSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{*newerSnap, *olderSnap}
			output := getSnapshotsForRemoval(cl, candidates, 0, 2, logger)

			Expect(output).To(BeEmpty())
		})

		It("Some PR snapshots, no non-PR snapshots. some to be GCed", func() {
			currentTime := time.Now()
			newerSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "older-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "Merge Request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerSnap, *olderSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{*newerSnap, *olderSnap}
			output := getSnapshotsForRemoval(cl, candidates, 1, 0, logger)

			Expect(output).To(HaveLen(1))
			Expect(output[0].Name).To(Equal("older-snapshot"))
		})

		It("Snapshots of both types to be GCed", func() {
			currentTime := time.Now()
			newerPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "older-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "Merge_Request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			anotherOldPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "another-old-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "Note",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Minute * 1)),
				},
			}
			newerNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "newer-non-pr-snapshot",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "Push",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}
			overrideSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "override-snapshot",
					CreationTimestamp: metav1.NewTime(currentTime),
					Labels: map[string]string{
						"test.appstudio.openshift.io/type": "override",
					},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerPRSnap, *olderPRSnap, *anotherOldPRSnap,
							*newerNonPRSnap, *olderNonPRSnap, *overrideSnap,
						},
					}).Build()
			candidates := []applicationapiv1alpha1.Snapshot{
				*newerPRSnap, *olderPRSnap, *newerNonPRSnap, *olderNonPRSnap,
				*anotherOldPRSnap, *overrideSnap,
			}
			output := getSnapshotsForRemoval(cl, candidates, 1, 1, logger)

			Expect(output).To(HaveLen(4))
			Expect(output[0].Name).To(Equal("older-non-pr-snapshot"))
			Expect(output[1].Name).To(Equal("another-old-pr-snapshot"))
			Expect(output[2].Name).To(Equal("older-pr-snapshot"))
			Expect(output[3].Name).To(Equal("override-snapshot"))
		})
	})

	Describe("Test deleteSnapshots", func() {
		It("Handles no snapshots to be removed", func() {
			snap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "snap",
				},
			}
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{*snap},
					}).Build()
			snapsToDelete := []applicationapiv1alpha1.Snapshot{}
			deleteSnapshots(cl, snapsToDelete, logger)

			snapsRemaining := &applicationapiv1alpha1.SnapshotList{}
			err := cl.List(context.Background(), snapsRemaining)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsRemaining.Items).To(HaveLen(1))
		})

		It("Handles some snapshots to be removed", func() {
			noDel := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "no-del",
				},
			}
			del1 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "del1",
				},
			}
			del2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "del2",
				},
			}
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{*noDel, *del1, *del2},
					}).Build()
			snapsToDelete := []applicationapiv1alpha1.Snapshot{*del1, *del2}
			deleteSnapshots(cl, snapsToDelete, logger)

			snapsRemaining := &applicationapiv1alpha1.SnapshotList{}
			err := cl.List(context.Background(), snapsRemaining)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsRemaining.Items).To(HaveLen(1))
			Expect(snapsRemaining.Items[0].Name).To(Equal("no-del"))
		})

		It("Handles all snapshots to be removed and continues on failure", func() {
			nonExist := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "non-existing",
				},
			}
			del1 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "del1",
				},
			}
			del2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name: "del2",
				},
			}
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{*del1, *del2},
					}).Build()
			snapsToDelete := []applicationapiv1alpha1.Snapshot{*nonExist, *del1, *del2}
			deleteSnapshots(cl, snapsToDelete, logger)

			snapsRemaining := &applicationapiv1alpha1.SnapshotList{}
			err := cl.List(context.Background(), snapsRemaining)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsRemaining.Items).To(BeEmpty())

			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"ERROR snapshots.appstudio.redhat.com \"non-existing\" not found " +
					"Failed to delete snapshot. snapshot.name non-existing",
			))
		})
	})

	Describe("Test getTenantNamespaces", func() {
		It("Handles no namespaces", func() {
			cl := fake.NewClientBuilder().WithScheme(scheme).Build()

			tenantsNs, err := getTenantNamespaces(cl, logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(tenantsNs).To(BeEmpty())
		})

		It("Handles no tenant namespaces", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "not-a-tenant",
					},
				},
			}
			ns2 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns2",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(&core.NamespaceList{Items: []core.Namespace{*ns1, *ns2}}).
				Build()

			tenantsNs, err := getTenantNamespaces(cl, logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(tenantsNs).To(BeEmpty())
		})

		It("Handles one tenant namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "not-a-tenant",
					},
				},
			}
			ns2 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns2",
				},
			}
			ns3 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns3",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(&core.NamespaceList{Items: []core.Namespace{
					*ns1, *ns2, *ns3,
				}}).
				Build()

			tenantsNs, err := getTenantNamespaces(cl, logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(tenantsNs).To(HaveLen(1))
			Expect(tenantsNs[0].Name).To(Equal("ns3"))
		})

		It("Handles all tenant namespaces", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			ns2 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns2",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			ns3 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns3",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(&core.NamespaceList{Items: []core.Namespace{
					*ns1, *ns2, *ns3,
				}}).
				Build()

			tenantsNs, err := getTenantNamespaces(cl, logger)

			Expect(err).ShouldNot(HaveOccurred())
			Expect(tenantsNs).To(HaveLen(3))
		})

		It("Returns error when fails API call", func() {
			cl := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
			_, err := getTenantNamespaces(cl, logger)
			Expect(err).Should(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(
				"no kind is registered for the type v1.NamespaceList in scheme",
			)))
		})
	})

	Describe("Test garbageCollectSnapshots", func() {
		It("Garbage collect snapshots from multiple namespaces", func() {

			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			ns2 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns2",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "not-a-tenant",
					},
				},
			}
			ns3 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns3",
				},
			}
			ns4 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns4",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			rel1 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel1",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "keep11",
				},
			}
			rel2 := &releasev1alpha1.Release{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "rel2",
					Namespace: "ns1",
				},
				Spec: releasev1alpha1.ReleaseSpec{
					Snapshot: "keep21",
				},
			}

			currentTime := time.Now()
			keep11 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep11",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep21 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep21",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep31 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep31",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			keep41 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "keep41",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns1",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns1",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "newer-non-pr-snapshot",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnap := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					Namespace:         "ns1",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}

			newerPRSnapNs2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns2",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnapNs2 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns2",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}
			newerNonPRSnapNs3 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "newer-non-pr-snapshot",
					Namespace:         "ns3",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 11)),
				},
			}
			olderNonPRSnapNs3 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "older-non-pr-snapshot",
					Namespace:         "ns3",
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 10)),
				},
			}
			newerPRSnapNs4 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "newer-pr-snapshot",
					Namespace: "ns4",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime.Add(time.Hour * 1)),
				},
			}
			olderPRSnapNs4 := &applicationapiv1alpha1.Snapshot{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "older-pr-snapshot",
					Namespace: "ns4",
					Labels: map[string]string{
						"pac.test.appstudio.openshift.io/event-type": "pull_request",
					},
					CreationTimestamp: metav1.NewTime(currentTime),
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithLists(
					&core.NamespaceList{
						Items: []core.Namespace{*ns1, *ns2, *ns3, *ns4},
					},
					&releasev1alpha1.ReleaseList{
						Items: []releasev1alpha1.Release{*rel1, *rel2},
					},
					&applicationapiv1alpha1.SnapshotList{
						Items: []applicationapiv1alpha1.Snapshot{
							*newerPRSnap, *olderPRSnap,
							*newerNonPRSnap, *olderNonPRSnap,
							*keep11, *keep21, *keep31, *keep41,
							*newerPRSnapNs2, *olderPRSnapNs2,
							*newerNonPRSnapNs3, *olderNonPRSnapNs3,
							*newerPRSnapNs4, *olderPRSnapNs4,
						},
					},
				).Build()

			var err error

			snapsBefore := &applicationapiv1alpha1.SnapshotList{}
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns1",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(8))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns2",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns3",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsBefore, &client.ListOptions{
				Namespace: "ns4",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsBefore.Items).To(HaveLen(2))

			err = garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())

			snapsAfter := &applicationapiv1alpha1.SnapshotList{}
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns1",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(4))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns2",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns3",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(2))
			err = cl.List(context.Background(), snapsAfter, &client.ListOptions{
				Namespace: "ns4",
			})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(snapsAfter.Items).To(HaveLen(1))
		})

		It("Fails if cannot list namespaces", func() {
			cl := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).Should(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring(
				"no kind is registered for the type v1.NamespaceList in scheme",
			)))
		})

		It("Does not fail if getSnapshotsForNSReleases fails for namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}
			// using default scheme having namespace but not releases
			cl := fake.NewClientBuilder().WithLists(
				&core.NamespaceList{Items: []core.Namespace{*ns1}},
			).Build()

			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())
			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"Failed getting releases associated with snapshots.",
			))
		})

		It("Does not fail if getUnassociatedNSSnapshots fails for namespace", func() {
			ns1 := &core.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ns1",
					Labels: map[string]string{
						"toolchain.dev.openshift.com/type": "tenant",
					},
				},
			}

			// Monkey-patching getUnassociatedNSSnapshots so it returns with error
			patches := gomonkey.ApplyFunc(
				getUnassociatedNSSnapshots, func(cl client.Client,
					snapToData map[string]snapshotData,
					namespace string,
					logger logr.Logger) ([]applicationapiv1alpha1.Snapshot, error) {
					return nil, errors.New("")
				})
			defer patches.Reset()
			cl := fake.NewClientBuilder().WithScheme(scheme).WithLists(
				&core.NamespaceList{Items: []core.Namespace{*ns1}},
			).Build()

			err := garbageCollectSnapshots(cl, logger, 1, 1)
			Expect(err).ShouldNot(HaveOccurred())
			logLines := strings.Split(buf.String(), "\n")
			Expect(logLines[len(logLines)-2]).Should(ContainSubstring(
				"Failed getting unassociated snapshots.",
			))
		})
	})
})
