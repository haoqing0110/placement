package customize

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

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
	Customize prioritizer xxxxx.
	`
)

var _ plugins.Prioritizer = &CustomizePrioritizer{}

type CustomizePrioritizer struct {
	handle          plugins.Handle
	prioritizerName string
}

type CustomizePrioritizerBuilder struct {
	customizePrioritizer *CustomizePrioritizer
}

func NewCustomizePrioritizerBuilder(handle plugins.Handle) *CustomizePrioritizerBuilder {
	return &CustomizePrioritizerBuilder{
		customizePrioritizer: &CustomizePrioritizer{
			handle: handle,
		},
	}
}

func (c *CustomizePrioritizerBuilder) WithPrioritizerName(name string) *CustomizePrioritizerBuilder {
	c.customizePrioritizer.prioritizerName = name
	return c
}

func (c *CustomizePrioritizerBuilder) Build() *CustomizePrioritizer {
	return c.customizePrioritizer
}

func (c *CustomizePrioritizer) Name() string {
	return c.prioritizerName
}

func (c *CustomizePrioritizer) Description() string {
	return description
}

func (c *CustomizePrioritizer) PreScore(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) error {
	clusterClient := c.handle.ClusterClient()
	total := float64(len(clusters))
	valid := 0.0

	for _, cluster := range clusters {
		namespace := cluster.Name
		name := strings.ToLower(c.Name())

		// create ManagedClusterScalar CR
		cr, err := clusterClient.ClusterV1alpha1().ManagedClusterScalars(namespace).Get(context.Background(), name, metav1.GetOptions{})
		switch {
		case errors.IsNotFound(err):
			managedClusterScalar := &clusterapiv1alpha1.ManagedClusterScalar{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
				Spec: clusterapiv1alpha1.ManagedClusterScalarSpec{
					PrioritizerName: c.Name(),
				},
				Status: clusterapiv1alpha1.ManagedClusterScalarStatus{},
			}
			if _, err := clusterClient.ClusterV1alpha1().ManagedClusterScalars(namespace).Create(context.Background(), managedClusterScalar, metav1.CreateOptions{}); err != nil {
				return err
			}
		case err != nil:
			return err
		case err == nil && len(cr.Status.Conditions) > 0 && (cr.Status.ValidUntil == nil || time.Now().Before(cr.Status.ValidUntil.Time)):
			valid += 1.0
		}

	}

	// valid if 80% CRs are ready
	if valid/total < 0.8 {
		return fmt.Errorf("avaliable ManagedClusterScalar CRs of %s is less than 80%", c.Name())
	}

	return nil
}

func (c *CustomizePrioritizer) Score(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	scores := map[string]int64{}

	for _, cluster := range clusters {
		namespace := cluster.Name
		name := strings.ToLower(c.Name())

		// get ManagedClusterScalar CR
		managedClusterScalar, err := c.handle.ClusterClient().ClusterV1alpha1().ManagedClusterScalars(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Getting ManagedClusterScalar failed :%s", err)
		}

		// get ManagedClusterScalar score
		if managedClusterScalar.Spec.PrioritizerName == c.Name() {
			scores[cluster.Name] = managedClusterScalar.Status.Scalar
			klog.Infof("Getting ManagedClusterScalar name:%s, cluster(namespace): %s, scalar:%s", name, namespace, managedClusterScalar.Status.Scalar)
		} else {
			klog.Warningf("ManagedClusterScalar PrioritizerName %s and CustomizePrioritizer name %s mismatch", managedClusterScalar.Spec.PrioritizerName, c.Name())
		}
	}

	// normalize cluster scores of each prioritizer
	normalizeScores(scores)

	return scores, nil
}

// normalize score from -100 to 100
func normalizeScores(scores map[string]int64) {
	if len(scores) <= 0 {
		return
	}

	// get min and max number of scores
	ss := sort.IntSlice{}
	for _, v := range scores {
		ss = append(ss, int(v))
	}
	sort.Ints(ss)
	min, max := int64(ss[0]), int64(ss[len(ss)-1])

	// normarlize clusterscores
	// score = ((score - min) / (max - min) - 0.5) * 2 * 100
	for k, v := range scores {
		if (max - min) != 0 {
			ratio := float64(v-min) / float64(max-min)
			scores[k] = int64((ratio - 0.5) * 2.0 * 100.0)
		} else {
			scores[k] = 100
		}
	}
}
