package deployment_test

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/werf/kubedog/pkg/tracker/deployment"
)

const progressDeadlineMarker = "ProgressDeadlineExceeded"

func withDeploymentProgressing(dep *appsv1.Deployment, status corev1.ConditionStatus, reason string) *appsv1.Deployment {
	dep.Status.Conditions = []appsv1.DeploymentCondition{{
		Type:    appsv1.DeploymentProgressing,
		Status:  status,
		Reason:  reason,
		Message: "progress condition marker-" + reason,
	}}
	return dep
}

func paused(dep *appsv1.Deployment, paused bool) *appsv1.Deployment {
	dep.Spec.Paused = paused
	return dep
}

func progressDeadlineDeployment() *appsv1.Deployment {
	dep := newNotReadyDeployment()
	return withDeploymentProgressing(dep, corev1.ConditionFalse, progressDeadlineMarker)
}

func statusWithWaitingMessage(want string) func(observation) bool {
	return func(ev observation) bool {
		status, ok := ev.Data.(deployment.DeploymentStatus)
		if !ok {
			return false
		}
		for _, msg := range status.WaitingForMessages {
			if msg == want {
				return true
			}
		}
		return false
	}
}

func TestTrack_ReportsProgressDeadlineAsCountedFailureOnce(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{progressDeadlineDeployment()},
	})

	if _, err := h.waitFor("Added", 10*time.Second, nil); err != nil {
		t.Fatalf("wait for Added: %v", err)
	}
	if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("progress deadline must be a counted failure: %v", err)
	}

	// The controller keeps the condition across retries: the same judgment must not be
	// charged to the error budget again.
	h.updateDeployment(t, progressDeadlineDeployment())
	if err := h.waitForNone("Failed", 2500*time.Millisecond, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("an unchanged progress deadline must be reported once: %v", err)
	}
}

func TestTrack_RecoversAfterProgressResumes(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{progressDeadlineDeployment()},
	})

	if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("wait for counted failure: %v", err)
	}

	// An external fix lets the rollout finish: the condition disappears and counts converge.
	h.updateDeployment(t, newReadyDeployment())
	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("the tracker must recover from a progress deadline failure: %v", err)
	}
}

func TestTrack_ProgressDeadlineOutranksSatisfiedReplicaCounts(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run("strict="+boolString(strict), func(t *testing.T) {
			dep := withDeploymentProgressing(newReadyDeployment(), corev1.ConditionFalse, progressDeadlineMarker)
			h := newHarness(t, harnessConfig{
				dynamicSeed:      []runtime.Object{dep},
				strictCompletion: strict,
			})

			if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
				t.Fatalf("the controller failure signal must outrank matching counts: %v", err)
			}
		})
	}
}

func TestTrack_RetainedProgressDeadlineDuringPauseIsNotReportedAgain(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{progressDeadlineDeployment()},
	})

	if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("wait for initial failure: %v", err)
	}

	// Pause keeps the stale condition alive; it must neither clear the one-shot flag nor
	// produce another failure.
	h.updateDeployment(t, paused(progressDeadlineDeployment(), true))
	if err := h.waitForNone("Failed", 2500*time.Millisecond, nil); err != nil {
		t.Fatalf("pausing must not re-report the retained deadline failure: %v", err)
	}

	h.updateDeployment(t, paused(progressDeadlineDeployment(), false))
	if err := h.waitForNone("Failed", 2500*time.Millisecond, nil); err != nil {
		t.Fatalf("unpausing must not re-report the retained deadline failure: %v", err)
	}

	h.updateDeployment(t, newReadyDeployment())
	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("recovery after pause must reach readiness: %v", err)
	}
}

func TestTrack_PausedDeploymentWaitsWithoutFailing(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{paused(newNotReadyDeployment(), true)},
	})

	added, err := h.waitFor("Added", 10*time.Second, nil)
	if err != nil {
		t.Fatalf("wait for Added: %v", err)
	}
	status := added.Data.(deployment.DeploymentStatus)
	if !status.IsPaused || status.IsFailed {
		t.Fatalf("paused deployment must wait, not fail: %+v", status)
	}

	h.updateDeployment(t, paused(newNotReadyDeployment(), true))
	if _, err := h.waitFor("Status", 10*time.Second, statusWithWaitingMessage("deployment is paused")); err != nil {
		t.Fatalf("waiting status must explain the pause: %v", err)
	}
	if err := h.waitForNone("Failed", 2500*time.Millisecond, nil); err != nil {
		t.Fatalf("a paused deployment must never fail on its own: %v", err)
	}

	h.updateDeployment(t, newReadyDeployment())
	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("unpausing and converging must reach readiness: %v", err)
	}
}

func TestTrack_StrictCompletionWaitsForNewReplicaSetAvailable(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{
			withDeploymentProgressing(newReadyDeployment(), corev1.ConditionTrue, "ReplicaSetUpdated"),
		},
		strictCompletion: true,
	})

	added, err := h.waitFor("Added", 10*time.Second, nil)
	if err != nil {
		t.Fatalf("wait for Added: %v", err)
	}
	if added.Data.(deployment.DeploymentStatus).IsReady {
		t.Fatal("strict mode must not report readiness while the reason is ReplicaSetUpdated")
	}

	h.updateDeployment(t, withDeploymentProgressing(newReadyDeployment(), corev1.ConditionTrue, "NewReplicaSetAvailable"))
	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("NewReplicaSetAvailable must complete the strict rollout: %v", err)
	}
}

func TestTrack_StrictCompletionScaleToZeroStaysReady(t *testing.T) {
	dep := newNotReadyDeployment()
	dep.Spec.Replicas = ptrTo(int32(0))
	dep.Status = appsv1.DeploymentStatus{ObservedGeneration: 1}
	withDeploymentProgressing(dep, corev1.ConditionTrue, "ReplicaSetUpdated")

	h := newHarness(t, harnessConfig{
		dynamicSeed:      []runtime.Object{dep},
		strictCompletion: true,
	})

	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("scale-to-zero must not wait for a condition the controller never emits: %v", err)
	}
}

func TestTrack_ProgressDeadlineAfterReadyDoesNotReopenTracking(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{newReadyDeployment()},
	})

	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("wait for Ready: %v", err)
	}

	// ResourceReady is terminal by design: a late condition change produces no new transition.
	h.updateDeployment(t, withDeploymentProgressing(newReadyDeployment(), corev1.ConditionFalse, progressDeadlineMarker))
	if err := h.waitForNone("Failed", 2500*time.Millisecond, nil); err != nil {
		t.Fatalf("readiness must stay terminal: %v", err)
	}
}

func TestTrack_RecreatedDeploymentRearmsProgressDeadlineReporting(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{progressDeadlineDeployment()},
	})

	if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("wait for initial failure: %v", err)
	}

	h.deleteObject(t, gvrDeployments, testDeploymentName)
	if _, err := h.waitFor("Status", 10*time.Second, deletionResetStatus); err != nil {
		t.Fatalf("wait for deletion reset: %v", err)
	}

	h.createObject(t, gvrDeployments, progressDeadlineDeployment())
	if _, err := h.waitFor("Failed", 10*time.Second, failedWithMode(progressDeadlineMarker, deployment.FailureModeCounted)); err != nil {
		t.Fatalf("a recreated deployment must report the deadline failure again: %v", err)
	}
}

func TestTrack_GenerationLagAfterReadyStaysTerminal(t *testing.T) {
	h := newHarness(t, harnessConfig{
		dynamicSeed: []runtime.Object{newReadyDeployment()},
	})

	if _, err := h.waitFor("Ready", 10*time.Second, nil); err != nil {
		t.Fatalf("wait for Ready: %v", err)
	}

	// A spec edit the controller has not observed yet would be waiting at the status level,
	// but a tracker that already reported ready has no post-Ready branch by design.
	stale := newReadyDeployment()
	stale.Generation = 2
	stale.Status.ObservedGeneration = 1
	h.updateDeployment(t, stale)
	if err := h.waitForNone("Status", 2500*time.Millisecond, nil); err != nil {
		t.Fatalf("ResourceReady must remain sticky after a generation bump: %v", err)
	}
}

func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
