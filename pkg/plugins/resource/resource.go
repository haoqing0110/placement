package resource

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

var _ plugins.Prioritizer = &Resource{}

type Resource struct {
	handle plugins.Handle
}

func New(handle plugins.Handle) *Resource {
	return &Resource{handle: handle}
}

func (b *Resource) Name() string {
	return "resource"
}

func (b *Resource) Description() string {
	return description
}

func (b *Resource) Score(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	scores := map[string]int64{}
	var maxScore int64

	klog.Infof("Resource scoring placement %q", placement.Name)
	for _, cluster := range clusters {
		acpu := cluster.Status.Allocatable[clusterapiv1.ResourceCPU]
		ccpu := cluster.Status.Capacity[clusterapiv1.ResourceCPU]
		amem := cluster.Status.Allocatable[clusterapiv1.ResourceMemory]
		cmem := cluster.Status.Capacity[clusterapiv1.ResourceMemory]
		for _, v := range placement.Spec.ResourceUsagePreferences {
			if v.ResourceName == clusterapiv1alpha1.ResourceNameCPU {
				scores[cluster.Name] += int64(acpu.AsApproximateFloat64() * 100 / ccpu.AsApproximateFloat64())
			}
			if v.ResourceName == clusterapiv1alpha1.ResourceNameMemory {
				scores[cluster.Name] += int64(amem.AsApproximateFloat64() * 100 / cmem.AsApproximateFloat64())
			}
		}
		maxScore = max(maxScore, scores[cluster.Name])
	}

	// normalize the score and ensure the value falls in the range between 0 and 100.
	for _, cluster := range clusters {
		if maxScore != 0 {
			scores[cluster.Name] = scores[cluster.Name] * 100 / maxScore
			klog.Infof("Resource scoring placement %q cluster %s score is %d", placement.Name, cluster.Name, scores[cluster.Name])
		}
	}

	return scores, nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
