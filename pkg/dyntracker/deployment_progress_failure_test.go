package dyntracker_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/werf/kubedog/pkg/dyntracker"
	"github.com/werf/kubedog/pkg/dyntracker/logstore"
	"github.com/werf/kubedog/pkg/dyntracker/statestore"
	"github.com/werf/kubedog/pkg/dyntracker/util"
	"github.com/werf/kubedog/pkg/informer"
)

func progressDeployment(cond *appsv1.DeploymentCondition) *appsv1.Deployment {
	one := int32(1)
	dep := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{Kind: "Deployment", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", UID: "dep-uid-1", Generation: 1},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "demo"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox:1"}}},
			},
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			UpdatedReplicas:    1,
			AvailableReplicas:  1,
		},
	}
	if cond != nil {
		dep.Status.Conditions = []appsv1.DeploymentCondition{*cond}
	}
	return dep
}

func progressingCondition(status corev1.ConditionStatus, reason string) *appsv1.DeploymentCondition {
	return &appsv1.DeploymentCondition{Type: appsv1.DeploymentProgressing, Status: status, Reason: reason}
}

func runProgressTracker(t *testing.T, seed runtime.Object, strict bool, allowance int) (*util.Concurrent[*statestore.ReadinessTaskState], context.CancelFunc) {
	t.Helper()

	dynClient := dynamicfake.NewSimpleDynamicClient(k8sscheme.Scheme, seed)
	kubeClient := k8sfake.NewSimpleClientset()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	watchErrCh := make(chan error, 100)
	factory := informer.NewConcurrentInformerFactory(ctx.Done(), watchErrCh, dynClient, informer.ConcurrentInformerFactoryOptions{})

	taskState := util.NewConcurrent(statestore.NewReadinessTaskState(
		"demo", "default", replicaFailureDeploymentGVK,
		statestore.ReadinessTaskStateOptions{TotalAllowFailuresCount: allowance},
	))
	logStore := util.NewConcurrent(logstore.NewLogStore())

	rt, err := dyntracker.NewDynamicReadinessTracker(
		ctx, taskState, logStore, factory, kubeClient, dynClient, nil, replicaFailureStubRESTMapper{},
		dyntracker.DynamicReadinessTrackerOptions{
			Timeout:                    60 * time.Second,
			CaseInsensitiveGVKMatching: true,
			IgnoreLogs:                 true,
			DeploymentStrictCompletion: strict,
		},
	)
	require.NoError(t, err)

	go func() { _ = rt.Track(ctx) }()
	return taskState, cancel
}

func taskStatus(taskState *util.Concurrent[*statestore.ReadinessTaskState]) statestore.ReadinessTaskStatus {
	var status statestore.ReadinessTaskStatus
	taskState.RTransaction(func(ts *statestore.ReadinessTaskState) {
		status = ts.Status()
	})
	return status
}

func TestDynamicReadinessTracker_StrictCompletionOptionDelaysReadiness(t *testing.T) {
	seed := progressDeployment(progressingCondition(corev1.ConditionTrue, "ReplicaSetUpdated"))

	defaultState, defaultCancel := runProgressTracker(t, seed, false, 0)
	defer defaultCancel()
	require.Eventually(t, func() bool {
		return taskStatus(defaultState) == statestore.ReadinessTaskStatusReady
	}, 10*time.Second, 100*time.Millisecond, "default mode must complete from replica counts")

	strictState, strictCancel := runProgressTracker(t, seed, true, 0)
	defer strictCancel()
	require.Never(t, func() bool {
		return taskStatus(strictState) == statestore.ReadinessTaskStatusReady
	}, 3*time.Second, 100*time.Millisecond, "strict mode must wait for NewReplicaSetAvailable")
}

func TestDynamicReadinessTracker_ProgressDeadlineIsCountedWithinAllowance(t *testing.T) {
	seed := progressDeployment(progressingCondition(corev1.ConditionFalse, "ProgressDeadlineExceeded"))

	taskState, cancel := runProgressTracker(t, seed, false, 1)
	defer cancel()

	require.Never(t, func() bool {
		return taskStatus(taskState) == statestore.ReadinessTaskStatusFailed
	}, 3*time.Second, 100*time.Millisecond, "a counted progress deadline within allowance must not fail the task")

	var resourceStatus statestore.ResourceStatus
	var hasError bool
	taskState.RTransaction(func(ts *statestore.ReadinessTaskState) {
		rs := ts.ResourceState(ts.Name(), ts.Namespace(), ts.GroupVersionKind())
		rs.RTransaction(func(state *statestore.ResourceState) {
			resourceStatus = state.Status()
			for _, errs := range state.Errors() {
				if len(errs) > 0 {
					hasError = true
				}
			}
		})
	})
	require.True(t, hasError, "the counted failure must be recorded on the resource state")
	require.NotEqual(t, statestore.ResourceStatusFailed, resourceStatus,
		"a counted failure must not mark the resource state failed")
}
