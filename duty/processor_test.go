package duty

import (
	"context"
	"job/ssv-ha/duty/ws"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Mostly a sample test
func TestProcessor_Run(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.Ldate|log.Ltime)

	wsClientMock := new(MockWSClient)
	dutiesCount := 4
	c := make(chan any, dutiesCount)
	c <- &ws.DutyMsg{
		Validator:  1,
		Height:     0,
		Duty:       ws.Attester,
		ReceivedAt: time.Now(),
	}
	c <- &ws.DutyMsg{
		Validator:  1,
		Height:     0,
		Duty:       ws.Aggregator,
		ReceivedAt: time.Now().Add(1 * time.Millisecond),
	}
	c <- &ws.DutyMsg{
		Validator:  1,
		Height:     1,
		Duty:       ws.SyncCommittee,
		ReceivedAt: time.Now().Add(2 * time.Millisecond),
	}
	c <- &ws.DutyMsg{
		Validator:  1,
		Height:     0,
		Duty:       ws.Proposer, // Proposer duty here falls behind Sync Committee duty
		ReceivedAt: time.Now().Add(3 * time.Millisecond),
	}
	wsClientMock.EXPECT().MsgC().Return(c).Once()

	lateDutyWaitTime := 500 * time.Millisecond
	p := NewProcessor(logger, wsClientMock, lateDutyWaitTime)

	ctx, ctxCancel := context.WithTimeout(context.Background(), 1*time.Second)
	var receivedDuties []ws.DutyType
	var mu sync.Mutex // Need to protect slice operations with mutext since the method below runs asynchronously
	p.processFn = func(ctx context.Context, d *ws.DutyMsg) {
		t.Log(d.Duty)

		mu.Lock()
		defer mu.Unlock()
		receivedDuties = append(receivedDuties, d.Duty)
		if len(receivedDuties) == dutiesCount {
			ctxCancel()
		}
	}

	err := p.Run(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []ws.DutyType{ws.Proposer, ws.Attester, ws.Aggregator, ws.SyncCommittee}, receivedDuties)
}
