package ws

import (
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

type DutyType string

const (
	Proposer      DutyType = "PROPOSER"
	Attester      DutyType = "ATTESTER"
	Aggregator    DutyType = "AGGREGATOR"
	SyncCommittee DutyType = "SYNC_COMMITTEE"
)

type DutyMsg struct {
	Validator  int32     `json:"validator,omitempty"`
	Duty       DutyType  `json:"duty,omitempty"`
	Height     int64     `json:"height,omitempty"`
	ReceivedAt time.Time // TODO: move this into separate struct dedicated for processor
}

// Client implements Duties WS API
type Client struct {
	logger *log.Logger
	conn   *websocket.Conn
	msgC   chan any
	closeC chan any
}

func NewClient(logger *log.Logger, url string) (*Client, error) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect ws client: %w", err)
	}

	c := &Client{
		logger: logger,
		conn:   conn,
		msgC:   make(chan any),
		closeC: make(chan any, 1), // Buffered in order to avoid blocking Close() method
	}

	go c.runMessageProcessing()
	return c, nil
}

func (c *Client) runMessageProcessing() {
	for {
		if d, err := c.receiveDuty(); err != nil {
			c.msgC <- err
		} else {
			c.msgC <- d
		}

		select {
		case <-c.closeC:
			close(c.msgC)
			return
		default:
		}
	}

}

func (c *Client) receiveDuty() (*DutyMsg, error) {
	d := new(DutyMsg)
	if err := c.conn.ReadJSON(d); err != nil {
		return nil, fmt.Errorf("failed to read json: %w", err)
	}
	d.ReceivedAt = time.Now()
	return d, nil
}

func (c *Client) MsgC() <-chan any {
	return c.msgC
}

func (c *Client) Close() error {
	c.closeC <- struct{}{}
	return c.conn.Close()
}
