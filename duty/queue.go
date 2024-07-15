package duty

import "job/ssv-ha/duty/ws"

// queue implements container/heap to process duties in proper order
type queue []*ws.DutyMsg

func (q queue) Len() int {
	return len(q)
}

func (q queue) Less(i, j int) bool {
	if q[i].Height == q[j].Height {
		return q[i].ReceivedAt.Before(q[j].ReceivedAt)
	}
	return q[i].Height < q[j].Height
}

func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *queue) Push(d any) {
	*q = append(*q, d.(*ws.DutyMsg)) // TODO
}

func (q *queue) Pop() any {
	old := *q
	n := len(old)
	d := old[n-1]
	old[n-1] = nil // avoid memory leak
	*q = old[0 : n-1]
	return d
}
