package deployment

import (
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/werf/kubedog/pkg/tracker/indicators"
	"github.com/werf/kubedog/pkg/tracker/pod"
)

const (
	// reasonProgressDeadlineExceeded is the reason of a False Progressing condition when the
	// Deployment controller observed no progress within progressDeadlineSeconds.
	// reasonNewReplicaSetAvailable is the reason of a True Progressing condition once the new
	// ReplicaSet has the required availability. The API package does not export these strings.
	reasonProgressDeadlineExceeded = "ProgressDeadlineExceeded"
	reasonNewReplicaSetAvailable   = "NewReplicaSetAvailable"
)

// FailureMode qualifies a reported failure: a counted one may be transient and spends the
// consumer's error budget, while a fatal one is durable and bypasses it.
type FailureMode uint8

const (
	// FailureModeCounted is a recoverable failure charged against the consumer's error budget.
	FailureModeCounted FailureMode = iota
	// FailureModeFatal is a rollout-wide failure that cannot recover without an external change.
	FailureModeFatal
)

// StatusOptions tunes Deployment rollout completion decisions. The zero value preserves the
// historical behavior that only the observed generation and replica counts are considered.
type StatusOptions struct {
	// StrictCompletion additionally requires the Progressing condition to report True with the
	// NewReplicaSetAvailable reason once the controller has written one. When no Progressing
	// condition is present yet (controller write lag, old cluster), the decision falls back to
	// replica counts.
	StrictCompletion bool
}

type DeploymentStatus struct {
	appsv1.DeploymentStatus

	StatusGeneration uint64

	ReplicasIndicator  *indicators.Int32EqualConditionIndicator
	UpToDateIndicator  *indicators.Int32EqualConditionIndicator
	AvailableIndicator *indicators.Int32EqualConditionIndicator

	WaitingForMessages []string

	IsReady      bool
	IsFailed     bool
	FailedReason string
	// FailureMode qualifies the failure when IsFailed is set.
	FailureMode FailureMode
	// IsPaused mirrors Spec.Paused of the tracked Deployment.
	IsPaused bool

	Pods map[string]pod.PodStatus
	// New Pod belongs to the new ReplicaSet of the Deployment,
	// i.e. actual up-to-date Pod of the Deployment
	NewPodsNames []string
}

func NewDeploymentStatus(object *appsv1.Deployment, statusGeneration uint64, isTrackerFailed bool, trackerFailedReason string, podsStatuses map[string]pod.PodStatus, newPodsNames []string, opts StatusOptions) DeploymentStatus {
	res := DeploymentStatus{
		StatusGeneration: statusGeneration,
		DeploymentStatus: object.Status,
		IsPaused:         object.Spec.Paused,
		Pods:             make(map[string]pod.PodStatus),
		NewPodsNames:     newPodsNames,
	}

processingPodsStatuses:
	for k, v := range podsStatuses {
		res.Pods[k] = v

		for _, newPodName := range newPodsNames {
			if newPodName == k {
				if v.StatusIndicator != nil {
					// New Pod should be Running
					v.StatusIndicator.TargetValue = "Running"
				}
				continue processingPodsStatuses
			}
		}

		if v.StatusIndicator != nil {
			// Old Pod should gone
			v.StatusIndicator.TargetValue = ""
		}
	}

	res.IsReady = false

	if object.Status.ObservedGeneration < object.Generation {
		// A spec change the controller has not observed yet invalidates every status signal,
		// including conditions: the condition gates are only evaluated for the current generation.
		res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("observed generation %d should be >= %d", object.Status.ObservedGeneration, object.Generation))
	} else {
		desired := deploymentDesiredReplicas(object)

		res.ReplicasIndicator = &indicators.Int32EqualConditionIndicator{
			Value:       object.Status.Replicas,
			TargetValue: desired,
		}
		res.UpToDateIndicator = &indicators.Int32EqualConditionIndicator{
			Value:       object.Status.UpdatedReplicas,
			TargetValue: desired,
		}
		res.AvailableIndicator = &indicators.Int32EqualConditionIndicator{
			Value:       object.Status.AvailableReplicas,
			TargetValue: desired,
		}

		res.IsReady = deploymentConditionReady(object, opts)

		// Fixed message order: pause, condition, replica diffs, terminating pods.
		countsOK := object.Status.UpdatedReplicas == desired &&
			object.Status.Replicas == desired &&
			object.Status.AvailableReplicas == desired

		if object.Spec.Paused && !countsOK {
			res.WaitingForMessages = append(res.WaitingForMessages, "deployment is paused")
		}

		// A condition is reported only when it actually blocks readiness. The desired-0 and
		// paused bypasses may keep a stale ProgressDeadlineExceeded condition around.
		if cond, found := progressingCondition(object); found && desired != 0 && !object.Spec.Paused {
			switch {
			case isProgressDeadlineExceeded(cond):
				// The controller's explicit failure signal blocks readiness in every mode.
				res.WaitingForMessages = append(res.WaitingForMessages, deploymentConditionMessage(cond))
			case opts.StrictCompletion && countsOK && !isNewReplicaSetAvailable(cond):
				// Counts are satisfied but the controller has not confirmed completion yet.
				res.WaitingForMessages = append(res.WaitingForMessages, deploymentConditionMessage(cond))
			}
		}

		if object.Status.UpdatedReplicas != desired {
			res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("up-to-date %d->%d", object.Status.UpdatedReplicas, desired))
		}
		if object.Status.Replicas != desired {
			res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("replicas %d->%d", object.Status.Replicas, desired))
		}
		if object.Status.AvailableReplicas != desired {
			res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("available %d->%d", object.Status.AvailableReplicas, desired))
		}

		if res.IsReady && desired == 0 {
			// Scale-to-zero is reported at once; old Pods may still be terminating.
			if terminating := countTerminatingOldPods(podsStatuses, newPodsNames); terminating > 0 {
				res.WaitingForMessages = append(res.WaitingForMessages, fmt.Sprintf("terminating pods: %d", terminating))
			}
		}
	}

	if !res.IsReady && !res.IsFailed {
		res.IsFailed = isTrackerFailed
		res.FailedReason = trackerFailedReason
	}

	return res
}

// deploymentDesiredReplicas returns the configured replica count, treating an unset field as the
// Kubernetes default of 1. Such objects only appear when built by hand; treating them as
// never-ready provides no useful signal.
func deploymentDesiredReplicas(object *appsv1.Deployment) int32 {
	if object.Spec.Replicas == nil {
		return 1
	}
	return *object.Spec.Replicas
}

// deploymentStatusReady reports whether the Deployment controller has observed the current spec
// and reports every replica up-to-date, present and available. It deliberately ignores conditions
// and pause: ReplicaSet failure suppression and the base completion check rely on counts alone.
func deploymentStatusReady(object *appsv1.Deployment) bool {
	if object.Status.ObservedGeneration < object.Generation {
		return false
	}

	desired := deploymentDesiredReplicas(object)
	return object.Status.UpdatedReplicas == desired &&
		object.Status.Replicas == desired &&
		object.Status.AvailableReplicas == desired
}

// deploymentConditionReady applies the pause and condition gates on top of the count-based
// readiness. The caller must only invoke it for a spec generation the controller has observed.
func deploymentConditionReady(object *appsv1.Deployment, opts StatusOptions) bool {
	desired := deploymentDesiredReplicas(object)
	countsOK := object.Status.UpdatedReplicas == desired &&
		object.Status.Replicas == desired &&
		object.Status.AvailableReplicas == desired
	if !countsOK {
		return false
	}

	// Scaling to zero has no Pod that could become available, and a paused Deployment whose
	// counts are already satisfied has nothing left to advance. The controller emits no fresh
	// NewReplicaSetAvailable condition for either case, so the condition gates must not apply.
	if desired == 0 || object.Spec.Paused {
		return true
	}

	cond, found := progressingCondition(object)
	if !found {
		// The condition is written asynchronously: without one, counts are the only signal.
		return true
	}
	if isProgressDeadlineExceeded(cond) {
		return false
	}
	if opts.StrictCompletion && !isNewReplicaSetAvailable(cond) {
		return false
	}

	return true
}

func progressingCondition(object *appsv1.Deployment) (appsv1.DeploymentCondition, bool) {
	for _, cond := range object.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing {
			return cond, true
		}
	}
	return appsv1.DeploymentCondition{}, false
}

func isProgressDeadlineExceeded(cond appsv1.DeploymentCondition) bool {
	return cond.Type == appsv1.DeploymentProgressing &&
		cond.Status == corev1.ConditionFalse &&
		cond.Reason == reasonProgressDeadlineExceeded
}

func isNewReplicaSetAvailable(cond appsv1.DeploymentCondition) bool {
	return cond.Type == appsv1.DeploymentProgressing &&
		cond.Status == corev1.ConditionTrue &&
		cond.Reason == reasonNewReplicaSetAvailable
}

func deploymentConditionMessage(cond appsv1.DeploymentCondition) string {
	return fmt.Sprintf("condition %s=%s(%s)", cond.Type, cond.Status, cond.Reason)
}

// deploymentConditionSummaries renders the Deployment conditions as stable Type=Status(Reason)
// strings sorted by type, status and reason so identical objects produce identical summaries.
func deploymentConditionSummaries(object *appsv1.Deployment) []string {
	conditions := append([]appsv1.DeploymentCondition(nil), object.Status.Conditions...)
	sort.SliceStable(conditions, func(i, j int) bool {
		if conditions[i].Type != conditions[j].Type {
			return conditions[i].Type < conditions[j].Type
		}
		if conditions[i].Status != conditions[j].Status {
			return conditions[i].Status < conditions[j].Status
		}
		return conditions[i].Reason < conditions[j].Reason
	})

	res := make([]string, 0, len(conditions))
	for _, cond := range conditions {
		res = append(res, fmt.Sprintf("%s=%s(%s)", cond.Type, cond.Status, cond.Reason))
	}
	return res
}

// countTerminatingOldPods counts Pods that belong to old ReplicaSets and are still terminating.
// New Pods are excluded, and a missing StatusIndicator means the Pod has no phase signal yet.
func countTerminatingOldPods(podsStatuses map[string]pod.PodStatus, newPodsNames []string) int {
	isNewPod := make(map[string]struct{}, len(newPodsNames))
	for _, name := range newPodsNames {
		isNewPod[name] = struct{}{}
	}

	var terminating int
	for name, podStatus := range podsStatuses {
		if _, isNew := isNewPod[name]; isNew {
			continue
		}
		if podStatus.StatusIndicator != nil && podStatus.StatusIndicator.Value == "Terminating" {
			terminating++
		}
	}
	return terminating
}
