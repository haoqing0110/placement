package scheduling

import (
	"context"
	"sort"

	kevents "k8s.io/client-go/tools/events"
	clusterclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	clusterlisterv1alpha1 "open-cluster-management.io/api/client/cluster/listers/cluster/v1alpha1"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	"open-cluster-management.io/placement/pkg/plugins"
	"open-cluster-management.io/placement/pkg/plugins/balance"
	"open-cluster-management.io/placement/pkg/plugins/demo"
	"open-cluster-management.io/placement/pkg/plugins/predicate"
	"open-cluster-management.io/placement/pkg/plugins/steady"
)

// Scheduler is an interface for scheduler, it returs the scheduler results
type Scheduler interface {
	Schedule(
		ctx context.Context,
		placement *clusterapiv1alpha1.Placement,
		clusters []*clusterapiv1.ManagedCluster,
	) (*ScheduleResult, error)
}

// ScheduleResult is the result for a certain schedule.
type ScheduleResult struct {
	feasibleClusters     []*clusterapiv1.ManagedCluster
	scheduledDecisions   []clusterapiv1alpha1.ClusterDecision
	unscheduledDecisions int
}

type schedulerHandler struct {
	recorder                kevents.EventRecorder
	placementDecisionLister clusterlisterv1alpha1.PlacementDecisionLister
	clusterClient           clusterclient.Interface
}

func NewSchedulerHandler(
	clusterClient clusterclient.Interface, placementDecisionLister clusterlisterv1alpha1.PlacementDecisionLister, recorder kevents.EventRecorder) plugins.Handle {

	return &schedulerHandler{
		recorder:                recorder,
		placementDecisionLister: placementDecisionLister,
		clusterClient:           clusterClient,
	}
}

func (s *schedulerHandler) EventRecorder() kevents.EventRecorder {
	return s.recorder
}

func (s *schedulerHandler) DecisionLister() clusterlisterv1alpha1.PlacementDecisionLister {
	return s.placementDecisionLister
}

func (s *schedulerHandler) ClusterClient() clusterclient.Interface {
	return s.clusterClient
}

type pluginScheduler struct {
	filters      []plugins.Filter
	prioritizers []plugins.Prioritizer
}

func NewPluginScheduler(handle plugins.Handle) *pluginScheduler {
	return &pluginScheduler{
		filters: []plugins.Filter{
			predicate.New(handle),
		},
		prioritizers: []plugins.Prioritizer{
			steady.New(handle),
			balance.New(handle),
			demo.New(handle),
		},
	}
}

func (s *pluginScheduler) Schedule(
	ctx context.Context,
	placement *clusterapiv1alpha1.Placement,
	clusters []*clusterapiv1.ManagedCluster,
) (*ScheduleResult, error) {
	var err error
	filtered := clusters

	// filter clusters
	for _, f := range s.filters {
		filtered, err = f.Filter(ctx, placement, filtered)

		if err != nil {
			return nil, err
		}
	}

	// score clusters
	// Score the cluster
	scoreSum := map[string]int64{}
	for _, cluster := range filtered {
		scoreSum[cluster.Name] = 0
	}
	for _, p := range s.prioritizers {
		score, err := p.Score(ctx, placement, filtered)
		if err != nil {
			return nil, err
		}

		// TODO we currently weigh each prioritizer as equal. We should consider
		// importance factor for each priotizer when caculating the final score.
		// Since currently balance plugin has a score range of +/- 100 while the score range of
		// balacne is 0/100, the balance plugin will trigger the reschedule for rebalancing when
		// a cluster's decision count is larger than average.
		for name, val := range score {
			scoreSum[name] = scoreSum[name] + val
		}
	}

	// Sort cluster by score
	sort.SliceStable(filtered, func(i, j int) bool {
		return scoreSum[clusters[i].Name] > scoreSum[clusters[j].Name]
	})

	// select clusters and generate cluster decisions
	// TODO: sort the feasible clusters and make sure the selection stable
	decisions := selectClusters(placement, filtered)
	scheduled, unscheduled := len(decisions), 0
	if placement.Spec.NumberOfClusters != nil {
		unscheduled = int(*placement.Spec.NumberOfClusters) - scheduled
	}

	return &ScheduleResult{
		feasibleClusters:     filtered,
		scheduledDecisions:   decisions,
		unscheduledDecisions: unscheduled,
	}, nil
}

// makeClusterDecisions selects clusters based on given cluster slice and then creates
// cluster decisions.
func selectClusters(placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) []clusterapiv1alpha1.ClusterDecision {
	numOfDecisions := len(clusters)
	if placement.Spec.NumberOfClusters != nil {
		numOfDecisions = int(*placement.Spec.NumberOfClusters)
	}

	// truncate the cluster slice if the desired number of decisions is less than
	// the number of the candidate clusters
	if numOfDecisions < len(clusters) {
		clusters = clusters[:numOfDecisions]
	}

	decisions := []clusterapiv1alpha1.ClusterDecision{}
	for _, cluster := range clusters {
		decisions = append(decisions, clusterapiv1alpha1.ClusterDecision{
			ClusterName: cluster.Name,
		})
	}
	return decisions
}
