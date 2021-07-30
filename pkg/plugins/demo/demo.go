package demo

import (
	"context"

	"k8s.io/klog/v2"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	"open-cluster-management.io/placement/pkg/plugins"
)

const (
	placementLabel = "cluster.open-cluster-management.io/placement"
	description    = `
	xxx
	`
)

var _ plugins.Prioritizer = &Demo{}

type Demo struct {
	handle plugins.Handle
}

func New(handle plugins.Handle) *Demo {
	return &Demo{handle: handle}
}

func (b *Demo) Name() string {
	return "Demo"
}

func (b *Demo) Description() string {
	return description
}

func (b *Demo) Score(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	scores := map[string]int64{}

	klog.Infof("Demo scoring placement %q", placement.Name)
	for _, cluster := range clusters {
		acpu := cluster.Status.Allocatable[clusterapiv1.ResourceCPU]
		ccpu := cluster.Status.Capacity[clusterapiv1.ResourceCPU]
		amem := cluster.Status.Allocatable[clusterapiv1.ResourceMemory]
		cmem := cluster.Status.Capacity[clusterapiv1.ResourceMemory]
		scores[cluster.Name] = int64(acpu.AsApproximateFloat64()/ccpu.AsApproximateFloat64())*100*int64(placement.Spec.ResourceWeights[clusterapiv1alpha1.RESOURCE_CPU].ResourceWeight) +
			int64(amem.AsApproximateFloat64()/cmem.AsApproximateFloat64())*100*int64(placement.Spec.ResourceWeights[clusterapiv1alpha1.RESOURCE_MEMORY].ResourceWeight)

		scores[cluster.Name] = scores[cluster.Name] * int64(placement.Spec.Policies[clusterapiv1alpha1.POLICY_LEAST_UTILIZATION].PolicyWeight)
		klog.Infof("Demo scoring placement %q cluster %s score is %d", placement.Name, cluster.Name, scores[cluster.Name])
	}

	return scores, nil
}
