package watch

import (
	"time"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"k8s.io/client-go/util/workqueue"
)

// Queue is the ports.ObservationQueue over a client-go rate-limited work
// queue: repeated names merge, a name a worker holds is never handed to a
// second one, and Retry backs off per name.
type Queue struct {
	q workqueue.TypedRateLimitingInterface[string]
}

var _ ports.ObservationQueue = (*Queue)(nil)

// NewQueue returns an empty queue.
func NewQueue() *Queue {
	return &Queue{q: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]())}
}

func (q *Queue) Add(jobName string)                       { q.q.Add(jobName) }
func (q *Queue) AddAfter(jobName string, d time.Duration) { q.q.AddAfter(jobName, d) }
func (q *Queue) Get() (string, bool)                      { return q.q.Get() }
func (q *Queue) Done(jobName string)                      { q.q.Done(jobName) }
func (q *Queue) Retry(jobName string)                     { q.q.AddRateLimited(jobName) }
func (q *Queue) Forget(jobName string)                    { q.q.Forget(jobName) }
func (q *Queue) ShutDown()                                { q.q.ShutDown() }
