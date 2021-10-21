package scheduling

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"k8s.io/client-go/kubernetes"
	kevents "k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	clusterclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	clusterlisterv1alpha1 "open-cluster-management.io/api/client/cluster/listers/cluster/v1alpha1"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	"open-cluster-management.io/placement/pkg/plugins"
	"open-cluster-management.io/placement/pkg/plugins/balance"
	"open-cluster-management.io/placement/pkg/plugins/predicate"
	"open-cluster-management.io/placement/pkg/plugins/resource"
	"open-cluster-management.io/placement/pkg/plugins/steady"
)

// PrioritizerScore defines the score for each cluster
type PrioritizerScore map[string]int64

// Scheduler is an interface for scheduler, it returs the scheduler results
type Scheduler interface {
	Schedule(
		ctx context.Context,
		placement *clusterapiv1alpha1.Placement,
		clusters []*clusterapiv1.ManagedCluster,
		//		scorer ScoreManager,
	) (ScheduleResult, error)
}

type ScheduleResult interface {
	// FilterResults returns results for each filter
	FilterResults() []FilterResult

	// PrioritizerResults returns results for each prioritizer
	PrioritizerResults() []PrioritizerResult

	// PrioritizerScores returns total score for each cluster
	PrioritizerScores() PrioritizerScore

	// Decisions returns the decisions of the schedule
	Decisions() []clusterapiv1alpha1.ClusterDecision

	// NumOfUnscheduled returns the number of unscheduled.
	NumOfUnscheduled() int
}

type FilterResult struct {
	Name             string   `json:"name"`
	FilteredClusters []string `json:"filteredClusters"`
}

// PrioritizerResult defines the result of one prioritizer,
// include name, weight, and score of each cluster.
type PrioritizerResult struct {
	Name   string           `json:"name"`
	Weight int32            `json:"weight"`
	Scores PrioritizerScore `json:"scores"`
}

// ScheduleResult is the result for a certain schedule.
type scheduleResult struct {
	feasibleClusters     []*clusterapiv1.ManagedCluster
	scheduledDecisions   []clusterapiv1alpha1.ClusterDecision
	unscheduledDecisions int

	filteredRecords map[string][]*clusterapiv1.ManagedCluster
	scoreRecords    []PrioritizerResult
	scoreSum        PrioritizerScore
}

type schedulerHandler struct {
	recorder                kevents.EventRecorder
	placementDecisionLister clusterlisterv1alpha1.PlacementDecisionLister
	clusterClient           clusterclient.Interface
	kubeClient              *kubernetes.Clientset
}

func NewSchedulerHandler(
	clusterClient clusterclient.Interface, kubeClient *kubernetes.Clientset, placementDecisionLister clusterlisterv1alpha1.PlacementDecisionLister, recorder kevents.EventRecorder) plugins.Handle {

	return &schedulerHandler{
		recorder:                recorder,
		placementDecisionLister: placementDecisionLister,
		clusterClient:           clusterClient,
		kubeClient:              kubeClient,
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

func (s *schedulerHandler) KubeClient() *kubernetes.Clientset {
	return s.kubeClient
}

// Initialize the default prioritizer weight.
// Balane and Steady weight 1, others weight 0.
// The default weight can be replaced by each placement's PrioritizerConfigs.
var defaultPrioritizerConfig = map[string]int32{
	"Balance": 1,
	"Steady":  1,
}

type pluginScheduler struct {
	filters            []plugins.Filter
	prioritizers       []plugins.Prioritizer
	prioritizerWeights map[string]int32
}

func NewPluginScheduler(handle plugins.Handle) *pluginScheduler {
	return &pluginScheduler{
		filters: []plugins.Filter{
			predicate.New(handle),
		},
		prioritizers: []plugins.Prioritizer{
			balance.New(handle),
			steady.New(handle),
			resource.NewResourcePrioritizerBuilder(handle).WithPrioritizerName("ResourceAllocatableCPU").Build(),
			resource.NewResourcePrioritizerBuilder(handle).WithPrioritizerName("ResourceAllocatableMemory").Build(),
		},
		prioritizerWeights: defaultPrioritizerConfig,
	}
}

func (s *pluginScheduler) Schedule(
	ctx context.Context,
	placement *clusterapiv1alpha1.Placement,
	clusters []*clusterapiv1.ManagedCluster,
) (ScheduleResult, error) {
	var err error
	filtered := clusters

	results := &scheduleResult{
		filteredRecords: map[string][]*clusterapiv1.ManagedCluster{},
		scoreRecords:    []PrioritizerResult{},
	}

	// filter clusters
	filterPipline := []string{}

	for _, f := range s.filters {
		filtered, err = f.Filter(ctx, placement, filtered)

		if err != nil {
			return nil, err
		}

		filterPipline = append(filterPipline, f.Name())

		results.filteredRecords[strings.Join(filterPipline, ",")] = filtered
	}

	// get weight for each prioritizers
	weights, err := getWeights(s.prioritizerWeights, placement)
	if err != nil {
		return nil, err
	}

	// score clusters
	scoreSum := PrioritizerScore{}
	for _, cluster := range filtered {
		scoreSum[cluster.Name] = 0
	}
	for _, p := range s.prioritizers {
		// If weight is 0 (set to 0 or not defined in map), skip Score().
		weight := weights[p.Name()]
		if weight == 0 {
			results.scoreRecords = append(results.scoreRecords, PrioritizerResult{Name: p.Name(), Weight: weight, Scores: nil})
			continue
		}

		err := p.PreScore(ctx, placement, filtered)
		if err != nil {
			klog.Warning("Prioritizer %s PreScore() failed: %s", p.Name(), err)
		}

		score, err := p.Score(ctx, placement, filtered)
		if err != nil {
			return nil, err
		}

		results.scoreRecords = append(results.scoreRecords, PrioritizerResult{Name: p.Name(), Weight: weight, Scores: score})

		// The final score is a sum of each prioritizer score * weight.
		// A higher weight indicates that the prioritizer weights more in the cluster selection,
		// while 0 weight indicate thats the prioritizer is disabled.
		for name, val := range score {
			scoreSum[name] = scoreSum[name] + val*int64(weight)
		}

	}

	// Sort cluster by score, if score is equal, sort by name
	sort.SliceStable(filtered, func(i, j int) bool {
		if scoreSum[filtered[i].Name] == scoreSum[filtered[j].Name] {
			return filtered[i].Name < filtered[j].Name
		} else {
			return scoreSum[filtered[i].Name] > scoreSum[filtered[j].Name]
		}
	})

	results.feasibleClusters = filtered
	results.scoreSum = scoreSum

	// select clusters and generate cluster decisions
	// TODO: sort the feasible clusters and make sure the selection stable
	decisions := selectClusters(placement, filtered)
	scheduled, unscheduled := len(decisions), 0
	if placement.Spec.NumberOfClusters != nil {
		unscheduled = int(*placement.Spec.NumberOfClusters) - scheduled
	}
	results.scheduledDecisions = decisions
	results.unscheduledDecisions = unscheduled

	return results, nil
}

/*type ScoreManager struct {
	handle     plugins.Handle
	kubeclient *kubernetes.Clientset
}

func NewScoreManager(handle plugins.Handle, kubeclient *kubernetes.Clientset) *ScoreManager {
	return &ScoreManager{
		handle:     handle,
		kubeclient: kubeclient,
	}
}

func (s *ScoreManager) CreateManagedClusterScoreCRs(clusters []*clusterapiv1.ManagedCluster, pluginName string) error {
	clusterClient := s.handle.ClusterClient()
	kubeClient := s.kubeclient
	for _, cluster := range clusters {
		name := strings.ToLower(cluster.Name + "-" + pluginName)
		namespace := cluster.Name

		// create ManagedClusterScore namespace
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		_, err := kubeClient.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{})
		if err != nil {
			klog.Infof("Creating namespace failed :%s", err)
		}

		// create ManagedClusterScore CR
		_, err = clusterClient.ClusterV1alpha1().ManagedClusterScores(namespace).Get(context.Background(), name, metav1.GetOptions{})
		switch {
		case errors.IsNotFound(err):
			managedClusterScore := &clusterapiv1alpha1.ManagedClusterScore{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
			}
			_, err := clusterClient.ClusterV1alpha1().ManagedClusterScores(namespace).Create(context.Background(), managedClusterScore, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("Creating ManagedClusterScore failed :%s", err)
			}
		case err != nil:
			return err
		}
	}
	return nil
}
*/

/*func (*scoreManager) deleteManagedClusterScoreCRs() {

}

func (*scoreManager) cleanManagedClusterScoreCRs() {

}*/

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

// Get prioritizer weight for the placement.
// In Additive and "" mode, will override defaultWeight with what placement has defined and return.
// In Exact mode, will return the name and weight defined in placement.
func getWeights(defaultWeight map[string]int32, placement *clusterapiv1alpha1.Placement) (map[string]int32, error) {
	mode := placement.Spec.PrioritizerPolicy.Mode
	switch {
	case mode == clusterapiv1alpha1.PrioritizerPolicyModeExact:
		return mergeWeights(nil, placement.Spec.PrioritizerPolicy.Configurations), nil
	case mode == clusterapiv1alpha1.PrioritizerPolicyModeAdditive || mode == "":
		return mergeWeights(defaultWeight, placement.Spec.PrioritizerPolicy.Configurations), nil
	default:
		return nil, fmt.Errorf("incorrect prioritizer policy mode: %s", mode)
	}
}

func mergeWeights(defaultWeight map[string]int32, customizedWeight []clusterapiv1alpha1.PrioritizerConfig) map[string]int32 {
	weights := make(map[string]int32)
	// copy the default weight
	for k, v := range defaultWeight {
		weights[k] = v
	}
	// override the default weight
	for _, c := range customizedWeight {
		weights[c.Name] = c.Weight
	}
	return weights
}

func (r *scheduleResult) FilterResults() []FilterResult {
	results := []FilterResult{}
	for name, r := range r.filteredRecords {
		result := FilterResult{Name: name, FilteredClusters: []string{}}

		for _, c := range r {
			result.FilteredClusters = append(result.FilteredClusters, c.Name)
		}
		results = append(results, result)
	}
	return results
}

func (r *scheduleResult) PrioritizerResults() []PrioritizerResult {
	return r.scoreRecords
}

func (r *scheduleResult) PrioritizerScores() PrioritizerScore {
	return r.scoreSum
}

func (r *scheduleResult) Decisions() []clusterapiv1alpha1.ClusterDecision {
	return r.scheduledDecisions
}

func (r *scheduleResult) NumOfUnscheduled() int {
	return r.unscheduledDecisions
}
