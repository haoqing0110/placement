package scheduling

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	clusterfake "open-cluster-management.io/api/client/cluster/clientset/versioned/fake"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	testinghelpers "open-cluster-management.io/placement/pkg/helpers/testing"
)

func TestSchedule(t *testing.T) {
	clusterSetName := "clusterSets"
	placementNamespace := "ns1"
	placementName := "placement1"

	cases := []struct {
		name           string
		placement      *clusterapiv1alpha1.Placement
		initObjs       []runtime.Object
		clusters       []*clusterapiv1.ManagedCluster
		decisions      []runtime.Object
		scheduleResult scheduleResult
	}{
		{
			name:      "new placement satisfied",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
			},
			decisions: []runtime.Object{},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster1"},
				},
			},
		},
		{
			name:      "new placement unsatisfied",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(3).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
			},
			decisions: []runtime.Object{},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster1"},
				},
				unscheduledDecisions: 2,
			},
		},
		{
			name:      "placement with all decisions scheduled",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(2).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1", "cluster2").Build(),
			},
			decisions: []runtime.Object{
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1", "cluster2").Build(),
			},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster2").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster3").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").Build(),
					testinghelpers.NewManagedCluster("cluster2").Build(),
					testinghelpers.NewManagedCluster("cluster3").Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster1"},
					{ClusterName: "cluster2"},
				},
			},
		},
		{
			name:      "placement with part of decisions scheduled",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(4).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1").Build(),
			},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster2").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			decisions: []runtime.Object{
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1").Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").Build(),
					testinghelpers.NewManagedCluster("cluster2").Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster1"},
					{ClusterName: "cluster2"},
				},
				unscheduledDecisions: 2,
			},
		},
		{
			name:      "placement without more feasible cluster available",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(4).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1").Build(),
			},
			decisions: []runtime.Object{
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster1").Build(),
			},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster1"},
				},
				unscheduledDecisions: 3,
			},
		},
		{
			name:      "schedule to cluster with least decisions",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(1).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 1)).
					WithDecisions("cluster1", "cluster2").Build(),
			},
			decisions: []runtime.Object{
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 1)).
					WithDecisions("cluster1", "cluster2").Build(),
			},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster2").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster3").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").Build(),
					testinghelpers.NewManagedCluster("cluster2").Build(),
					testinghelpers.NewManagedCluster("cluster3").Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster3"},
				},
			},
		},
		{
			name:      "do not schedule to other cluster even with least decisions",
			placement: testinghelpers.NewPlacement(placementNamespace, placementName).WithNOC(1).Build(),
			initObjs: []runtime.Object{
				testinghelpers.NewClusterSet(clusterSetName),
				testinghelpers.NewClusterSetBinding(placementNamespace, clusterSetName),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 1)).
					WithDecisions("cluster3", "cluster2").Build(),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 2)).
					WithDecisions("cluster2", "cluster1").Build(),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster3").Build(),
			},
			decisions: []runtime.Object{
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 1)).
					WithDecisions("cluster3", "cluster2").Build(),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName("others", 2)).
					WithDecisions("cluster2", "cluster1").Build(),
				testinghelpers.NewPlacementDecision(placementNamespace, placementDecisionName(placementName, 1)).
					WithLabel(placementLabel, placementName).
					WithDecisions("cluster3").Build(),
			},
			clusters: []*clusterapiv1.ManagedCluster{
				testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster2").WithLabel(clusterSetLabel, clusterSetName).Build(),
				testinghelpers.NewManagedCluster("cluster3").WithLabel(clusterSetLabel, clusterSetName).Build(),
			},
			scheduleResult: scheduleResult{
				feasibleClusters: []*clusterapiv1.ManagedCluster{
					testinghelpers.NewManagedCluster("cluster1").WithLabel(clusterSetLabel, clusterSetName).Build(),
					testinghelpers.NewManagedCluster("cluster2").WithLabel(clusterSetLabel, clusterSetName).Build(),
					testinghelpers.NewManagedCluster("cluster3").WithLabel(clusterSetLabel, clusterSetName).Build(),
				},
				scheduledDecisions: []clusterapiv1alpha1.ClusterDecision{
					{ClusterName: "cluster3"},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.initObjs = append(c.initObjs, c.placement)
			clusterClient := clusterfake.NewSimpleClientset(c.initObjs...)
			s := NewPluginScheduler(testinghelpers.NewFakePluginHandle(t, clusterClient, c.initObjs...))
			result, err := s.Schedule(
				context.TODO(),
				c.placement,
				c.clusters,
			)
			if err != nil {
				t.Errorf("unexpected err: %v", err)
			}
			if !reflect.DeepEqual(result.Decisions(), c.scheduleResult.scheduledDecisions) {
				t.Errorf("expected %v scheduled, but got %v", c.scheduleResult.scheduledDecisions, result.Decisions())
			}
			if result.NumOfUnscheduled() != c.scheduleResult.unscheduledDecisions {
				t.Errorf("expected %d unscheduled, but got %d", c.scheduleResult.unscheduledDecisions, result.NumOfUnscheduled())
			}
		})
	}
}

func placementDecisionName(placementName string, index int) string {
	return fmt.Sprintf("%s-decision-%d", placementName, index)
}
