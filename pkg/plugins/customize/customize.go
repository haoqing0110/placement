package customize

import (
	"context"
	"sort"
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

func (r *CustomizePrioritizerBuilder) WithPrioritizerName(name string) *CustomizePrioritizerBuilder {
	r.customizePrioritizer.prioritizerName = name
	return r
}

func (r *CustomizePrioritizerBuilder) Build() *CustomizePrioritizer {
	return r.customizePrioritizer
}

func (r *CustomizePrioritizer) Name() string {
	return r.prioritizerName
}

func (r *CustomizePrioritizer) Description() string {
	return description
}

func (r *CustomizePrioritizer) PreScore(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) error {
	clusterClient := r.handle.ClusterClient()
	kubeClient := r.handle.KubeClient()

	for _, cluster := range clusters {
		namespace := cluster.Name
		name := strings.ToLower(strings.TrimPrefix(r.Name(), "Customize"))

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
		// TODO: delete above code before merge PR, only used for prototype.

		// create ManagedClusterScore CR
		_, err = clusterClient.ClusterV1alpha1().ManagedClusterScalars(namespace).Get(context.Background(), name, metav1.GetOptions{})
		switch {
		case errors.IsNotFound(err):
			managedClusterScore := &clusterapiv1alpha1.ManagedClusterScalar{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
				Spec: clusterapiv1alpha1.ManagedClusterScalarSpec{
					PrioritizerName: r.Name(),
				},
			}
			_, err := clusterClient.ClusterV1alpha1().ManagedClusterScalars(namespace).Create(context.Background(), managedClusterScore, metav1.CreateOptions{})
			if err != nil {
				return err
			}
		case err != nil:
			return err
		}
	}

	return nil
}

func (r *CustomizePrioritizer) Score(ctx context.Context, placement *clusterapiv1alpha1.Placement, clusters []*clusterapiv1.ManagedCluster) (map[string]int64, error) {
	scores := map[string]int64{}

	for _, cluster := range clusters {
		namespace := cluster.Name
		name := strings.ToLower(strings.TrimPrefix(r.Name(), "Customize"))

		// get ManagedClusterScalar CR
		managedClusterScalar, err := r.handle.ClusterClient().ClusterV1alpha1().ManagedClusterScalars(namespace).Get(context.Background(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Getting ManagedClusterScalar failed :%s", err)
		}

		// get ManagedClusterScalar score
		scores[cluster.Name] = managedClusterScalar.Status.Scalar
		klog.Infof("Getting ManagedClusterScalar name:%s, cluster(namespace): %s, score:%s", name, namespace, managedClusterScalar.Status.Scalar)
	}

	// normalize cluster scores of each prioritizer
	normalizeScores(scores)

	return scores, nil
}

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
