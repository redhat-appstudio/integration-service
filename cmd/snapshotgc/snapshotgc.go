package main

import (
	"context"
	"flag"
	"os"
	"sort"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/konflux-ci/operator-toolkit/metadata"
	releasev1alpha1 "github.com/konflux-ci/release-service/api/v1alpha1"
	applicationapiv1alpha1 "github.com/redhat-appstudio/application-api/api/v1alpha1"
	zap2 "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(applicationapiv1alpha1.AddToScheme(scheme))
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(releasev1alpha1.AddToScheme(scheme))
}

// Stores pointers to resources to which the snapshot is associated
type snapshotData struct {
	release releasev1alpha1.Release
}

// Iterates tenant namespaces and garbage-collect their snapshots
func garbageCollectSnapshots(
	cl client.Client,
	logger logr.Logger,
	prSnapshotsToKeep, nonPrSnapshotsToKeep int,
) error {
	namespaces, err := getTenantNamespaces(cl, logger)
	if err != nil {
		return err
	}

	logger.V(1).Info("Snapshot garbage collection started...")
	for _, ns := range namespaces {
		logger.V(1).Info("Processing namespace", "namespace", ns.Name)

		snapToData := make(map[string]snapshotData)
		snapToData, err = getSnapshotsForNSReleases(cl, snapToData, ns.Name, logger)
		if err != nil {
			logger.Error(
				err,
				"Failed getting releases associated with snapshots. Skipping namespace",
				"namespace",
				ns.Name,
			)
			continue
		}

		var candidates []applicationapiv1alpha1.Snapshot
		candidates, err = getUnassociatedNSSnapshots(cl, snapToData, ns.Name, logger)
		if err != nil {
			logger.Error(
				err,
				"Failed getting unassociated snapshots. Skipping namespace",
				"namespace", ns.Name,
			)
			continue
		}

		candidates = getSnapshotsForRemoval(
			cl, candidates, prSnapshotsToKeep, nonPrSnapshotsToKeep, logger,
		)

		deleteSnapshots(cl, candidates, logger)
	}
	return nil
}

// Gets all tenant namespaces
func getTenantNamespaces(
	cl client.Client, logger logr.Logger) ([]core.Namespace, error) {
	req, _ := labels.NewRequirement(
		"toolchain.dev.openshift.com/type", selection.In, []string{"tenant"},
	)
	selector := labels.NewSelector().Add(*req)
	namespaceList := &core.NamespaceList{}
	err := cl.List(
		context.Background(),
		namespaceList,
		&client.ListOptions{LabelSelector: selector},
	)
	if err != nil {
		logger.Error(err, "Failed listing namespaces")
		return nil, err
	}
	return namespaceList.Items, nil
}

// Gets a map to allow to tell with direct lookup if a snapshot is associated with
// a release resource
func getSnapshotsForNSReleases(
	cl client.Client,
	snapToData map[string]snapshotData,
	namespace string,
	logger logr.Logger,
) (map[string]snapshotData, error) {
	releases := &releasev1alpha1.ReleaseList{}
	err := cl.List(
		context.Background(),
		releases,
		&client.ListOptions{Namespace: namespace},
	)
	if err != nil {
		logger.Error(err, "Failed to list releases")
		return nil, err
	}

	for _, release := range releases.Items {
		data, ok := snapToData[release.Spec.Snapshot]
		if !ok {
			data = snapshotData{}
		}
		data.release = release
		snapToData[release.Spec.Snapshot] = data
	}
	return snapToData, nil
}

// Gets all namespace snapshots that aren't associated with a release
func getUnassociatedNSSnapshots(
	cl client.Client,
	snapToData map[string]snapshotData,
	namespace string,
	logger logr.Logger,
) ([]applicationapiv1alpha1.Snapshot, error) {
	snaps := &applicationapiv1alpha1.SnapshotList{}
	err := cl.List(
		context.Background(),
		snaps,
		&client.ListOptions{Namespace: namespace},
	)
	if err != nil {
		logger.Error(err, "Failed to list snapshots")
		return nil, err
	}

	var unAssociatedSnaps []applicationapiv1alpha1.Snapshot

	for _, snap := range snaps.Items {
		if _, found := snapToData[snap.Name]; found {
			logger.V(1).Info(
				"Skipping snapshot as it's associated with release",
				"namespace", snap.Namespace,
				"snapshot.name", snap.Name,
			)
			continue
		}
		unAssociatedSnaps = append(unAssociatedSnaps, snap)
	}

	return unAssociatedSnaps, nil
}

// Keep a certain amount of pr/non-pr snapshots
func getSnapshotsForRemoval(
	cl client.Client,
	snapshots []applicationapiv1alpha1.Snapshot,
	prSnapshotsToKeep int,
	nonPrSnapshotsToKeep int,
	logger logr.Logger,
) []applicationapiv1alpha1.Snapshot {
	sort.Slice(snapshots, func(i, j int) bool {
		// sorting in reverse order, so we keep the latest snapshots
		return snapshots[j].CreationTimestamp.Before(&snapshots[i].CreationTimestamp)
	})

	shortList := []applicationapiv1alpha1.Snapshot{}
	keptPrSnaps := 0
	keptNonPrSnaps := 0

	for _, snap := range snapshots {
		snap := snap
		label, found := snap.GetLabels()["pac.test.appstudio.openshift.io/event-type"]
		isOverrideSnapshot := metadata.HasLabelWithValue(&snap, "test.appstudio.openshift.io/type", "override")
		// override snapshot does not have the event-type label, but we still want to add it to the cleanup list
		if !found || label == "push" || label == "Push" || isOverrideSnapshot {
			if keptNonPrSnaps < nonPrSnapshotsToKeep {
				logger.V(1).Info(
					"Skipping non-PR candidate snapshot",
					"namespace", snap.Namespace,
					"snapshot.name", snap.Name,
					"non-pr-snapshot-kept", keptNonPrSnaps+1,
					"non-pr-snapshots-to-keep", nonPrSnapshotsToKeep,
				)
				keptNonPrSnaps++
			} else {
				logger.V(1).Info(
					"Adding non-PR candidate snapshot",
					"namespace", snap.Namespace,
					"snapshot.name", snap.Name,
					"non-pr-snapshot-kept", keptNonPrSnaps,
					"non-pr-snapshots-to-keep", nonPrSnapshotsToKeep,
				)
				shortList = append(shortList, snap)
			}
		} else {
			if keptPrSnaps < prSnapshotsToKeep {
				logger.V(1).Info(
					"Skipping PR candidate snapshot",
					"namespace", snap.Namespace,
					"snapshot.name", snap.Name,
					"pr-snapshot-kept", keptPrSnaps+1,
					"pr-snapshots-to-keep", prSnapshotsToKeep,
				)
				keptPrSnaps++
			} else {
				logger.V(1).Info(
					"Adding PR candidate snapshot",
					"namespace", snap.Namespace,
					"snapshot.name", snap.Name,
					"pr-snapshot-kept", keptPrSnaps,
					"pr-snapshots-to-keep", prSnapshotsToKeep,
				)
				shortList = append(shortList, snap)
			}
		}
	}
	return shortList
}

// Delete snapshots determined to be garbage-collected
func deleteSnapshots(
	cl client.Client,
	snapshots []applicationapiv1alpha1.Snapshot,
	logger logr.Logger,
) {

	for _, snap := range snapshots {
		snap := snap
		err := cl.Delete(context.Background(), &snap)
		if err != nil {
			logger.Error(err, "Failed to delete snapshot.", "snapshot.name", snap.Name)
		}
	}
}

func main() {
	var prSnapshotsToKeep, nonPrSnapshotsToKeep int
	flag.IntVar(
		&prSnapshotsToKeep,
		"pr-snapshots-to-keep",
		512,
		"Number of PR snapshots to keep after garbage collection",
	)
	flag.IntVar(
		&nonPrSnapshotsToKeep,
		"non-pr-snapshots-to-keep",
		512,
		"Number of non-PR snapshots to keep after garbage collection",
	)
	opts := zap.Options{
		Development: false,
		TimeEncoder: zapcore.RFC3339TimeEncoder,
		ZapOpts:     []zap2.Option{zap2.WithCaller(true)},
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&opts))

	var err error

	// We want env vars args to take precedence over cli args
	if value, ok := os.LookupEnv("PR_SNAPSHOTS_TO_KEEP"); ok {
		prSnapshotsToKeep, err = strconv.Atoi(value)
		if err != nil {
			logger.Error(err, "Failed parsing env var PR_SNAPSHOTS_TO_KEEP")
			panic(err.Error())
		}
	}
	if value, ok := os.LookupEnv("NON_PR_SNAPSHOTS_TO_KEEP"); ok {
		nonPrSnapshotsToKeep, err = strconv.Atoi(value)
		if err != nil {
			logger.Error(err, "Failed parsing env var NON_PR_SNAPSHOTS_TO_KEEP")
			panic(err.Error())
		}
	}

	cl, err := client.New(config.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		logger.Error(err, "Snapshots garbage collection failed creating client")
		panic(err.Error())
	}

	err = garbageCollectSnapshots(cl, logger, prSnapshotsToKeep, nonPrSnapshotsToKeep)
	if err != nil {
		logger.Error(err, "Snapshots garbage collection failed")
		panic(err.Error())
	}
}
