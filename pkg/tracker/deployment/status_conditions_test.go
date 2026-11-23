package deployment

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/werf/kubedog/pkg/tracker/indicators"
	"github.com/werf/kubedog/pkg/tracker/pod"
)

func statusTestDeployment() *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			UpdatedReplicas:    1,
			AvailableReplicas:  1,
		},
	}
}

func withProgressing(object *appsv1.Deployment, status corev1.ConditionStatus, reason string) *appsv1.Deployment {
	object.Status.Conditions = append(object.Status.Conditions, appsv1.DeploymentCondition{
		Type:    appsv1.DeploymentProgressing,
		Status:  status,
		Reason:  reason,
		Message: "condition message",
	})
	return object
}

func setReplicas(object *appsv1.Deployment, desired, updated, replicas, available int32) *appsv1.Deployment {
	object.Spec.Replicas = &desired
	object.Status.UpdatedReplicas = updated
	object.Status.Replicas = replicas
	object.Status.AvailableReplicas = available
	return object
}

func TestNewDeploymentStatus_readinessTimeline(t *testing.T) {
	tests := []struct {
		name            string
		object          *appsv1.Deployment
		opts            StatusOptions
		wantReady       bool
		wantPaused      bool
		wantMessages    []string
		wantIndicators  bool
		wantTrackerFail bool
		wantFailureMode FailureMode
	}{
		{
			name:           "counts satisfied with no condition is ready",
			object:         statusTestDeployment(),
			wantReady:      true,
			wantIndicators: true,
		},
		{
			name:           "ReplicaSetUpdated is ready without the strict gate",
			object:         withProgressing(statusTestDeployment(), corev1.ConditionTrue, "ReplicaSetUpdated"),
			wantReady:      true,
			wantIndicators: true,
		},
		{
			name:           "ReplicaSetUpdated waits under the strict gate",
			object:         withProgressing(statusTestDeployment(), corev1.ConditionTrue, "ReplicaSetUpdated"),
			opts:           StatusOptions{StrictCompletion: true},
			wantReady:      false,
			wantMessages:   []string{"condition Progressing=True(ReplicaSetUpdated)"},
			wantIndicators: true,
		},
		{
			name:           "NewReplicaSetAvailable is ready under the strict gate",
			object:         withProgressing(statusTestDeployment(), corev1.ConditionTrue, reasonNewReplicaSetAvailable),
			opts:           StatusOptions{StrictCompletion: true},
			wantReady:      true,
			wantIndicators: true,
		},
		{
			name:           "ProgressDeadlineExceeded blocks readiness despite satisfied counts",
			object:         withProgressing(statusTestDeployment(), corev1.ConditionFalse, reasonProgressDeadlineExceeded),
			wantReady:      false,
			wantMessages:   []string{"condition Progressing=False(ProgressDeadlineExceeded)"},
			wantIndicators: true,
		},
		{
			name:           "ProgressDeadlineExceeded blocks readiness in strict mode too",
			object:         withProgressing(statusTestDeployment(), corev1.ConditionFalse, reasonProgressDeadlineExceeded),
			opts:           StatusOptions{StrictCompletion: true},
			wantReady:      false,
			wantMessages:   []string{"condition Progressing=False(ProgressDeadlineExceeded)"},
			wantIndicators: true,
		},
		{
			name: "ProgressDeadlineExceeded with unfinished counts reports condition before replica diffs",
			object: withProgressing(
				setReplicas(statusTestDeployment(), 1, 0, 0, 0),
				corev1.ConditionFalse, reasonProgressDeadlineExceeded),
			wantReady: false,
			wantMessages: []string{
				"condition Progressing=False(ProgressDeadlineExceeded)",
				"up-to-date 0->1",
				"replicas 0->1",
				"available 0->1",
			},
			wantIndicators: true,
		},
		{
			name: "paused with unfinished counts waits with an explicit pause message",
			object: func() *appsv1.Deployment {
				dep := setReplicas(statusTestDeployment(), 1, 0, 0, 0)
				dep.Spec.Paused = true
				return dep
			}(),
			wantReady:  false,
			wantPaused: true,
			wantMessages: []string{
				"deployment is paused",
				"up-to-date 0->1",
				"replicas 0->1",
				"available 0->1",
			},
			wantIndicators: true,
		},
		{
			name: "paused with satisfied counts is ready because nothing is left to advance",
			object: func() *appsv1.Deployment {
				dep := statusTestDeployment()
				dep.Spec.Paused = true
				return dep
			}(),
			wantReady:      true,
			wantPaused:     true,
			wantIndicators: true,
		},
		{
			name: "paused with a retained deadline condition stays ready and reports no condition",
			object: func() *appsv1.Deployment {
				dep := withProgressing(statusTestDeployment(), corev1.ConditionFalse, reasonProgressDeadlineExceeded)
				dep.Spec.Paused = true
				return dep
			}(),
			wantReady:      true,
			wantPaused:     true,
			wantIndicators: true,
		},
		{
			name: "scale to zero is ready immediately",
			object: func() *appsv1.Deployment {
				return setReplicas(statusTestDeployment(), 0, 0, 0, 0)
			}(),
			wantReady:      true,
			wantIndicators: true,
		},
		{
			name: "scale to zero bypasses the strict condition gate",
			object: func() *appsv1.Deployment {
				dep := setReplicas(statusTestDeployment(), 0, 0, 0, 0)
				return withProgressing(dep, corev1.ConditionTrue, "ReplicaSetUpdated")
			}(),
			opts:           StatusOptions{StrictCompletion: true},
			wantReady:      true,
			wantIndicators: true,
		},
		{
			name:           "surge keeps the deployment waiting on the replica count",
			object:         setReplicas(statusTestDeployment(), 2, 2, 3, 2),
			wantReady:      false,
			wantMessages:   []string{"replicas 3->2"},
			wantIndicators: true,
		},
		{
			name: "stale observed generation only reports the generation message",
			object: func() *appsv1.Deployment {
				dep := statusTestDeployment()
				dep.Generation = 2
				dep.Status.ObservedGeneration = 1
				return dep
			}(),
			wantReady:      false,
			wantMessages:   []string{"observed generation 1 should be >= 2"},
			wantIndicators: false,
		},
		{
			name: "unset spec replicas default to one and become ready at count one",
			object: func() *appsv1.Deployment {
				dep := statusTestDeployment()
				dep.Spec.Replicas = nil
				return dep
			}(),
			wantReady:      true,
			wantIndicators: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := NewDeploymentStatus(tt.object, 1, false, "", nil, nil, tt.opts)

			if status.IsReady != tt.wantReady {
				t.Fatalf("IsReady = %v, want %v", status.IsReady, tt.wantReady)
			}
			if status.IsPaused != tt.wantPaused {
				t.Fatalf("IsPaused = %v, want %v", status.IsPaused, tt.wantPaused)
			}
			if status.IsFailed {
				t.Fatalf("status-level computation must not report a failure, got %q", status.FailedReason)
			}
			assertMessages(t, status.WaitingForMessages, tt.wantMessages)
			if tt.wantIndicators {
				if status.ReplicasIndicator == nil || status.UpToDateIndicator == nil || status.AvailableIndicator == nil {
					t.Fatalf("indicators must be populated: %+v", status)
				}
			} else if status.ReplicasIndicator != nil {
				t.Fatalf("indicators must stay nil before the generation is observed: %+v", status.ReplicasIndicator)
			}
			if tt.object.Spec.Replicas == nil && status.ReplicasIndicator.TargetValue != 1 {
				t.Fatalf("nil spec replicas must default target to 1, got %d", status.ReplicasIndicator.TargetValue)
			}
		})
	}
}

func TestNewDeploymentStatus_scaleToZeroReportsTerminatingOldPods(t *testing.T) {
	dep := setReplicas(statusTestDeployment(), 0, 0, 0, 0)
	podsStatuses := map[string]pod.PodStatus{
		"old-pod": {StatusIndicator: &indicators.StringEqualConditionIndicator{Value: "Terminating"}},
		"new-pod": {StatusIndicator: &indicators.StringEqualConditionIndicator{Value: "Running"}},
		"unknown": {},
	}

	status := NewDeploymentStatus(dep, 1, false, "", podsStatuses, []string{"new-pod"}, StatusOptions{})

	if !status.IsReady {
		t.Fatalf("scale to zero must be ready: messages=%v", status.WaitingForMessages)
	}
	assertMessages(t, status.WaitingForMessages, []string{"terminating pods: 1"})
}

func TestNewDeploymentStatus_trackerFailureFallback(t *testing.T) {
	dep := setReplicas(statusTestDeployment(), 1, 0, 0, 0)
	status := NewDeploymentStatus(dep, 1, true, "injected tracker failure", nil, nil, StatusOptions{})

	if status.IsReady || !status.IsFailed {
		t.Fatalf("injected tracker failure must win: %+v", status)
	}
	if status.FailedReason != "injected tracker failure" || status.FailureMode != FailureModeCounted {
		t.Fatalf("unexpected failure: reason=%q mode=%v", status.FailedReason, status.FailureMode)
	}
}

// deploymentStatusReady stays a pure count/generation predicate so ReplicaSet failure
// suppression ignores the condition gates.
func TestDeploymentStatusReady_ignoresConditionsAndPause(t *testing.T) {
	dep := withProgressing(statusTestDeployment(), corev1.ConditionFalse, reasonProgressDeadlineExceeded)
	dep.Spec.Paused = true

	if !deploymentStatusReady(dep) {
		t.Fatal("the count predicate must ignore the Progressing condition and pause")
	}

	dep.Status.AvailableReplicas = 0
	if deploymentStatusReady(dep) {
		t.Fatal("the count predicate must still react to mismatching counts")
	}
}

func TestDeploymentConditionSummaries_areSortedAndStable(t *testing.T) {
	dep := statusTestDeployment()
	dep.Status.Conditions = []appsv1.DeploymentCondition{
		{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: reasonNewReplicaSetAvailable},
		{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable"},
	}

	first := deploymentConditionSummaries(dep)
	second := deploymentConditionSummaries(dep)

	want := []string{
		"Available=True(MinimumReplicasAvailable)",
		"Progressing=True(NewReplicaSetAvailable)",
	}
	assertMessages(t, first, want)
	assertMessages(t, second, want)
}

func assertMessages(t *testing.T, got, want []string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if len(got) != len(want) {
		t.Fatalf("messages = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("messages = %v, want %v (mismatch at %d)", got, want, i)
		}
	}
}
