package resource

import (
	"context"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	clusterapiv1 "open-cluster-management.io/api/cluster/v1"
	clusterapiv1alpha1 "open-cluster-management.io/api/cluster/v1alpha1"
	"open-cluster-management.io/placement/pkg/plugins"
)

const (
	placementLabel = clusterapiv1alpha1.PlacementLabel
	description    = `
	ResourceAllocatableCPU and ResourceAllocatableMemory prioritizer makes the scheduling 
	decisions based on the resource allocatable of managed clusters. 
	The clusters that has the most allocatable are given the highest score, 
	while the least is given the lowest score.
	`
)

var _ plugins.Prioritizer = &ResourcePrioritizer{}

var resourceMap = map[string]clusterapiv1.ResourceName{
	"CPU":    clusterapiv1.ResourceCPU,
	"Memory": clusterapiv1.ResourceMemory,
}

type ResourcePrioritizer struct {
	handle          plugins.Handle
	prioritizerName string
	algorithm       string
	resource        clusterapiv1.ResourceName
}

type ResourcePrioritizerBuilder struct {
	resourcePrioritizer *ResourcePrioritizer
}

func NewResourcePrioritizerBuilder(handle plugins.Handle) *ResourcePrioritizerBuilder {
	return &ResourcePrioritizerBuilder{
		resourcePrioritizer: &ResourcePrioritizer{
			handle: handle,
		},
	}
}

func (r *ResourcePrioritizerBuilder) WithPrioritizerName(name string) *ResourcePrioritizerBuilder {
	r.resourcePrioritizer.prioritizerName = name
	return r
}

func (r *ResourcePrioritizerBuilder) Build() *ResourcePrioritizer {
	algorithm, resource := parsePrioritizerName(r.resourcePrioritizer.prioritizerName)
	r.resourcePrioritizer.algorithm = algorithm
	r.resourcePrioritizer.resource = resource
	return r.resourcePrioritizer
}

// parese prioritizerName to algorithm and resource.
// For example, prioritizerName ResourceAllocatableCPU will return Allocatable, CPU.
func parsePrioritizerName(prioritizerName string) (algorithm string, resource clusterapiv1.ResourceName) {
	s := regexp.MustCompile("[A-Z]+[a-z]*").FindAllString(prioritizerName, -1)
	if len(s) == 3 {
		return s[1], resourceMap[s[2]]
	}
	return "", ""
}

func (r *ResourcePrioritizer) Name() string {
	return r.prioritizerName
}

func (r *ResourcePrioritizer) Description() string {
	return description
}

func (r *ResourcePrioritizer) PreScore(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) error {
	clusterClient := r.handle.ClusterClient()
	kubeClient := r.handle.KubeClient()

	for _, cluster := range clusters {
		name := strings.ToLower(cluster.Name + "-" + r.Name())
		namespace := cluster.Name

		// TODO: delete below code before merge PR, only used for prototype.
		// create ManagedClusterScore namespace
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: namespace,
			},
		}
		_, err := kubeClient.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{})
		if err != nil {
			klog.Infof("%s", err)
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
				return err
			}
		case err != nil:
			return err
		}
	}

	return nil
}

func (r *ResourcePrioritizer) Score(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	/*	if r.algorithm == "Allocatable" {
			return mostResourceAllocatableScores(r.resource, clusters)
		}
	*/
	scores := map[string]int64{}
	for _, cluster := range clusters {
		name := strings.ToLower(cluster.Name + "-" + r.Name())
		namespace := cluster.Name
		// create ManagedClusterScore CR
		managedClusterScore, err := r.handle.ClusterClient().ClusterV1alpha1().ManagedClusterScores(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Getting ManagedClusterScore failed :%s", err)
		}
		scores[cluster.Name] = managedClusterScore.Status.Score
		klog.Errorf("HQ : Getting ManagedClusterScore :%s", managedClusterScore.Status.Score)
	}

	//TODO: haoqing
	//normarlizeScores(scores)
	return scores, nil
}

// Calculate clusters scores based on the resource allocatable.
// The clusters that has the most allocatable are given the highest score, while the least is given the lowest score.
// The score range is from -100 to 100.
/*func mostResourceAllocatableScores(resourceName clusterapiv1.ResourceName, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	scores := map[string]int64{}

	// get resourceName's min and max allocatable among all the clusters
	minAllocatable, maxAllocatable, err := getClustersMinMaxAllocatableResource(clusters, resourceName)
	if err != nil {
		return scores, nil
	}

	for _, cluster := range clusters {
		// get one cluster resourceName's allocatable
		allocatable, _, err := getClusterResource(cluster, resourceName)
		if err != nil {
			continue
		}

		// score = ((resource_x_allocatable - min(resource_x_allocatable)) / (max(resource_x_allocatable) - min(resource_x_allocatable)) - 0.5) * 2 * 100
		if (maxAllocatable - minAllocatable) != 0 {
			ratio := float64(allocatable-minAllocatable) / float64(maxAllocatable-minAllocatable)
			scores[cluster.Name] = int64((ratio - 0.5) * 2.0 * 100.0)
		} else {
			scores[cluster.Name] = 100.0
		}
	}

	return scores, nil
}

// Go through one cluster resources and return the allocatable and capacity of the resourceName.
func getClusterResource(cluster *clusterapiv1.ManagedCluster, resourceName clusterapiv1.ResourceName) (allocatable, capacity float64, err error) {
	if v, exist := cluster.Status.Allocatable[resourceName]; exist {
		allocatable = v.AsApproximateFloat64()
	} else {
		return allocatable, capacity, fmt.Errorf("no allocatable %s found in cluster %s", resourceName, cluster.ObjectMeta.Name)
	}

	if v, exist := cluster.Status.Capacity[resourceName]; exist {
		capacity = v.AsApproximateFloat64()
	} else {
		return allocatable, capacity, fmt.Errorf("no capacity %s found in cluster %s", resourceName, cluster.ObjectMeta.Name)
	}

	return allocatable, capacity, nil
}

// Go through all the cluster resources and return the min and max allocatable value of the resourceName.
func getClustersMinMaxAllocatableResource(clusters []*clusterapiv1.ManagedCluster, resourceName clusterapiv1.ResourceName) (minAllocatable, maxAllocatable float64, err error) {
	allocatable := sort.Float64Slice{}

	// get allocatable resources
	for _, cluster := range clusters {
		if alloc, _, err := getClusterResource(cluster, resourceName); err == nil {
			allocatable = append(allocatable, alloc)
		}
	}

	// return err if no allocatable resource
	if len(allocatable) == 0 {
		return 0, 0, fmt.Errorf("no allocatable %s found in clusters", resourceName)
	}

	// sort to get min and max
	sort.Float64s(allocatable)
	return allocatable[0], allocatable[len(allocatable)-1], nil
}
*/
