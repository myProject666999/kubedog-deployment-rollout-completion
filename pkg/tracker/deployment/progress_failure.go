package deployment

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/werf/kubedog/pkg/tracker"
)

// progressFailureUID marks a failure originating from the Deployment's own Progressing
// condition rather than from a ReplicaSet ReplicaFailure condition.
const progressFailureUID types.UID = "deployment-progress-deadline"

// rearmProgressFailure drops the remembered ProgressDeadlineExceeded when the controller's
// condition no longer reports it or the current generation is not observed yet. It must not clear
// merely because the Deployment is paused: the controller keeps the stale condition during a
// pause, so pause/unpause would otherwise report the same failure again.
func (d *Tracker) rearmProgressFailure(object *appsv1.Deployment) {
	cond, found := progressingCondition(object)
	if object.Status.ObservedGeneration >= object.Generation && found && isProgressDeadlineExceeded(cond) {
		return
	}

	d.progressDeadlineExceeded = false
	if d.failedFromReplicaSetUID != progressFailureUID {
		return
	}

	d.failedFromReplicaSetUID = ""
	if d.State == tracker.ResourceFailed {
		d.State = tracker.ResourceAdded
	}
}

// handleProgressDeadlineFailure reports the controller's progress-deadline judgment once. The
// failure is counted rather than fatal: an external change (for example adding an image-pull
// secret) can make the controller report progress again, after which rearmProgressFailure clears
// it and the rollout resumes.
func (d *Tracker) handleProgressDeadlineFailure(ctx context.Context) error {
	if d.lastObject == nil || d.State == tracker.ResourceReady || d.progressDeadlineExceeded {
		return nil
	}

	object := d.lastObject
	if object.Status.ObservedGeneration < object.Generation || object.Spec.Paused {
		return nil
	}

	cond, found := progressingCondition(object)
	if !found || !isProgressDeadlineExceeded(cond) {
		return nil
	}

	d.progressDeadlineExceeded = true

	return d.handleResourceFailure(ctx, resourceFailure{
		reason:            fmt.Sprintf("%s: %s", reasonProgressDeadlineExceeded, cond.Message),
		fromReplicaSetUID: progressFailureUID,
		mode:              FailureModeCounted,
	})
}
