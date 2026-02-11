package workerpool

import (
	"fmt"
	"sort"
	"time"
)

type JobStatus int

const (
	JobStatusUnknown JobStatus = iota
	JobStatusSuccess
	JobStatusInProgress
	JobStatusSkipped
	JobStatusFailed
)

const timeoutError = "timeout_error"

// DAGResult contains results from orchestrator.
// collection is not touched from particular jobs so as resource can't be thread safe
// (only orchestrator can it)
type DAGResult map[string]*JobResult

type JobResult struct {
	Name   string
	Status JobStatus
	Error  error
}

// FrontierItem is item in the workflow where work stay
type FrontierItem struct {
	Job       string
	Status    JobStatus
	BlockedBy []string
	Subtree   []string // optional: successors that depend on this job
	dagResult DAGResult
}

type Frontiers []*FrontierItem

func newDAGResult(dag DAG) DAGResult {
	result := make(map[string]*JobResult)
	for jobName := range dag {
		result[jobName] = &JobResult{
			Status: JobStatusUnknown,
			Name:   jobName,
		}
	}
	return result
}

func (j *JobResult) IsFailed() bool {
	return j.Status == JobStatusFailed
}

func (j *JobResult) IsSkipped() bool {
	return j.Status == JobStatusSkipped
}

func (j *JobResult) IsNotSkipped() bool {
	return !j.IsSkipped()
}

func (j *JobResult) IsInProgress() bool {
	return j.Status == JobStatusInProgress
}

func (j *JobResult) IsSuccessfull() bool {
	return j.Status == JobStatusSuccess
}

func (j *JobResult) Skip() {
	j.Status = JobStatusSkipped
}

func (j *JobResult) Success() {
	j.Status = JobStatusSuccess
}

func (j *JobResult) Fail(err error) {
	j.Status = JobStatusFailed
	j.Error = err
}

func (j *JobResult) NotReady() {
	j.Status = JobStatusInProgress
}

func (r DAGResult) TimeoutError(deadline time.Duration) {
	r[timeoutError] = &JobResult{
		Status: JobStatusFailed,
		Error:  fmt.Errorf("timeout after %v", deadline.Seconds()),
	}
}

func TimeoutErrorKey() string { return timeoutError }

func (r DAGResult) FirstError() error {
	for _, jobResult := range r {
		if jobResult.Error != nil {
			return jobResult.Error
		}
	}
	return nil
}

// IsFailed says result has Error(s)
func (r DAGResult) IsFailed() bool {
	for _, jobResult := range r {
		if jobResult.Status == JobStatusFailed {
			return true
		}
	}
	return false
}

func (r DAGResult) IsTimeouted() bool {
	_, found := r[timeoutError]
	return found
}

func (r DAGResult) IsReady() bool {
	for _, key := range r.orderedJobs() {
		if r[key].IsSkipped() || r[key].IsInProgress() || r[key].IsFailed() {
			return false
		}
	}
	return true
}

func (r DAGResult) IsNotReady() bool {
	return !r.IsReady()
}

func (r DAGResult) FirstInProgress() *JobResult {
	for _, key := range r.orderedJobs() {
		if r[key].IsInProgress() {
			return r[key]
		}
	}
	return nil
}

func (r DAGResult) IsSuccessful() bool {
	for _, result := range r {
		if result.Status != JobStatusSuccess {
			return false
		}
	}
	return true
}

func (r DAGResult) IsNotSuccessful() bool {
	return !r.IsSuccessful()
}

// Blockers returns direct deps that are not successful (so they block this job).
func (r DAGResult) Blockers(dag DAG, job string) []string {
	deps := dag[job]
	if len(deps) == 0 {
		return nil
	}
	out := make([]string, 0, len(deps))
	for _, d := range deps {
		if !r.isSuccess(d) {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// Subtree returns all successors reachable from "root" (including root).
func (r DAGResult) Subtree(dag DAG, root string) []string {
	seen := map[string]bool{}
	var out []string

	var dfs func(string)
	dfs = func(n string) {
		if seen[n] {
			return
		}
		seen[n] = true
		out = append(out, n)
		for _, s := range dag[n] {
			dfs(s)
		}
	}
	dfs(root)
	sort.Strings(out)
	return out
}

func (r DAGResult) Frontiers(dag DAG) Frontiers {
	rev := r.reverseDAG(dag)

	items := make([]*FrontierItem, 0)

	for job := range dag {
		jr := r.get(job)

		// We ignore successes - they are not part of the "stuck frontier".
		if jr.Status == JobStatusSuccess {
			continue
		}

		blockedBy := r.Blockers(dag, job)

		// - Failed is always interesting
		// - InProgress is always interesting
		// - Unknown/Skipped is interesting if it is not blocked (deps are all success)
		if jr.Status == JobStatusFailed || jr.Status == JobStatusInProgress || len(blockedBy) == 0 {
			items = append(items, &FrontierItem{
				Job:       job,
				Status:    jr.Status,
				BlockedBy: blockedBy,
				Subtree:   r.Subtree(rev, job),
				dagResult: r,
			})
		}
	}

	// Deterministic order for printing/debugging.
	sort.Slice(items, func(i, j int) bool { return items[i].Job < items[j].Job })
	return items
}

// BoundarySubtrees returns jobs that are successful but have a not-yet-successful job
// somewhere in their successor subtree.
func (r DAGResult) BoundarySubtrees(dag DAG) []string {
	succ := r.reverseDAG(dag) // job -> successors

	// Memoize: job -> subtreeHasNotSuccess
	memo := map[string]bool{}
	visiting := map[string]bool{}

	var hasNotSuccessInSubtree func(string) bool
	hasNotSuccessInSubtree = func(job string) bool {
		if v, ok := memo[job]; ok {
			return v
		}
		if visiting[job] {
			// Shouldn't happen in a DAG, but avoid infinite recursion.
			return false
		}
		visiting[job] = true
		defer delete(visiting, job)

		for _, s := range succ[job] {
			if !r.isSuccess(s) {
				memo[job] = true
				return true
			}
			if hasNotSuccessInSubtree(s) {
				memo[job] = true
				return true
			}
		}
		memo[job] = false
		return false
	}

	out := make([]string, 0)
	for job := range dag {
		if !r.isSuccess(job) {
			continue
		}
		if hasNotSuccessInSubtree(job) {
			out = append(out, job)
		}
	}
	sort.Strings(out)
	return out
}

func (r DAGResult) orderedJobs() []string {
	keys := make([]string, 0)
	for job := range r {
		keys = append(keys, job)
	}
	sort.Strings(keys)
	return keys
}

func (r DAGResult) get(job string) *JobResult {
	if jr, ok := r[job]; ok && jr != nil {
		return jr
	}
	return &JobResult{Name: job, Status: JobStatusUnknown}
}

func (r DAGResult) isSuccess(job string) bool {
	return r.get(job).Status == JobStatusSuccess
}

func (r DAGResult) reverseDAG(dag DAG) DAG {
	rev := make(DAG, len(dag))
	for n := range dag {
		rev[n] = nil
	}
	for n, deps := range dag {
		for _, dep := range deps {
			rev[dep] = append(rev[dep], n)
		}
	}
	return rev
}

func (f *FrontierItem) IsBlocked() bool {
	return len(f.BlockedBy) != 0
}

func (f Frontiers) AsMap() map[string]*FrontierItem {
	m := make(map[string]*FrontierItem)
	for _, frontier := range f {
		m[frontier.Job] = frontier
	}
	return m
}
