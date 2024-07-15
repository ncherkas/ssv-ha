package duty

import (
	"container/heap"
	"context"
	"fmt"
	"job/ssv-ha/duty/ws"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	maxProcessingTime  = 3 * time.Second
	pendingCheckPeriod = 250 * time.Millisecond
)

//go:generate mockery --name WSClient
type WSClient interface {
	MsgC() <-chan any
	Close() error
}

// validator info stored in sync.Map
type validator struct {
	Index   int32
	Height  int64
	DutiesQ queue
}

// processFunc is used for replacement in the unit tests
type processFunc func(ctx context.Context, d *ws.DutyMsg)

type Processor struct {
	logger           *log.Logger
	client           WSClient
	validators       sync.Map
	lateDutyWaitTime time.Duration // Time that we give for the late duties to come and still be processed
	processFn        processFunc
}

func NewProcessor(logger *log.Logger, c WSClient, lateDutyWaitTime time.Duration) (p *Processor) {
	p = &Processor{
		logger:           logger,
		client:           c,
		lateDutyWaitTime: lateDutyWaitTime,
	}
	p.processFn = p.process // Hack for mocking (replacing) private method
	return
}

func (p *Processor) Run(ctx context.Context) error {
	pendingDutiesTicker := time.NewTicker(pendingCheckPeriod)
	defer pendingDutiesTicker.Stop()
	msgC := p.client.MsgC()
	for {
		select {
		case msg := <-msgC:
			switch v := msg.(type) {
			case *ws.DutyMsg:
				val, ok := p.validators.Load(v.Validator)
				if !ok {
					val = &validator{
						Index:   v.Validator,
						Height:  v.Height,
						DutiesQ: queue{},
					}
					p.validators.Store(v.Validator, val)
				}
				validator := val.(*validator)
				// We always push duty into the queue and later perform a common handling logic
				heap.Push(&validator.DutiesQ, v)

				nextDuty := validator.DutiesQ[0] // Always holds min element in the queue
				if nextDuty.Height > validator.Height {
					// Pending duty is the one which came with the newer heigh
					// we mark it such and pospone to let other (possibly) late duties to come
					if p.isPending(nextDuty) {
						continue
					}
					validator.Height = nextDuty.Height
				}
				heap.Remove(&validator.DutiesQ, 0)
				if nextDuty.Height < validator.Height {
					// This is when late duty has come too late (i.e. after lateDutyWaitTime)
					p.logger.Printf("Discarded late duty %#v\n", v)
					continue
				}

				go p.processFn(ctx, nextDuty)

			case error:
				return fmt.Errorf("received error: %w", v)
			}

		case <-pendingDutiesTicker.C: // This is a periodical routine to handle the postponed duties
			p.validators.Range(func(k, v any) (proceed bool) {
				proceed = true

				// As an improvement here we could go over all duties and process them if they're ready
				// left it simplified for easier reviewing
				if validator := v.(*validator); len(validator.DutiesQ) > 0 {
					nextDuty := validator.DutiesQ[0]
					if nextDuty.Height > validator.Height {
						if p.isPending(nextDuty) {
							return
						}
						validator.Height = nextDuty.Height
					}

					heap.Remove(&validator.DutiesQ, 0)
					go p.processFn(ctx, nextDuty)
				}
				return
			})

		case <-ctx.Done():
			p.logger.Println("Duties processing has been terminated.")
			return nil
		}
	}
}

func (p *Processor) isPending(d *ws.DutyMsg) bool {
	receivedAt := d.ReceivedAt
	return time.Now().Before(receivedAt.Add(p.lateDutyWaitTime))
}

func (p *Processor) process(ctx context.Context, d *ws.DutyMsg) {
	p.logger.Printf("Processing duty %#v...\n", d)
	// Emulating approx processing time
	max := maxProcessingTime.Milliseconds()
	min := int64(float64(max) * 0.75)
	approxTime := rand.Int64N(int64(max)-min) + min
	select {
	case <-time.After(time.Duration(approxTime) * time.Millisecond):
		p.logger.Printf("Processed duty %#v\n", d)
	case <-ctx.Done():
		return
	}
}
