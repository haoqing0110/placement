package hub

import (
	"context"
	"net/http"
	"time"

	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	clusterclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	clusterscheme "open-cluster-management.io/api/client/cluster/clientset/versioned/scheme"
	clusterinformers "open-cluster-management.io/api/client/cluster/informers/externalversions"
	scheduling "open-cluster-management.io/placement/pkg/controllers/scheduling"
)

// RunControllerManager starts the controllers on hub to make placement decisions.
func RunControllerManager(ctx context.Context, controllerContext *controllercmd.ControllerContext) error {
	clusterClient, err := clusterclient.NewForConfig(controllerContext.KubeConfig)
	if err != nil {
		return err
	}

	kubeClient, err := kubernetes.NewForConfig(controllerContext.KubeConfig)
	if err != nil {
		return err
	}

	installDebugger(controllerContext.Server.Handler.NonGoRestfulMux)

	clusterInformers := clusterinformers.NewSharedInformerFactory(clusterClient, 10*time.Minute)

	broadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: kubeClient.EventsV1()})

	broadcaster.StartRecordingToSink(ctx.Done())

	recorder := broadcaster.NewRecorder(clusterscheme.Scheme, "placementController")

	scheduler := scheduling.NewPluginScheduler(
		scheduling.NewSchedulerHandler(
			clusterClient,
			clusterInformers.Cluster().V1alpha1().PlacementDecisions().Lister(),
			recorder),
	)

	schedulingController := scheduling.NewSchedulingController(
		clusterClient,
		clusterInformers.Cluster().V1().ManagedClusters(),
		clusterInformers.Cluster().V1alpha1().ManagedClusterSets(),
		clusterInformers.Cluster().V1alpha1().ManagedClusterSetBindings(),
		clusterInformers.Cluster().V1alpha1().Placements(),
		clusterInformers.Cluster().V1alpha1().PlacementDecisions(),
		scheduler,
		controllerContext.EventRecorder, recorder,
	)

	go clusterInformers.Start(ctx.Done())

	go schedulingController.Run(ctx, 1)

	<-ctx.Done()
	return nil
}

func installDebugger(mux *mux.PathRecorderMux) {
	mux.Handle("/debug", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		return
	}))
}
