package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// NATSIngester consumes events from a NATS JetStream subject and forwards them
// onto the pipeline's event channel. It provides at-least-once delivery: a
// message is acknowledged only after it has been accepted into the pipeline, so
// a crash before Ack results in redelivery. See docs/implementation-guide.md
// Phase 5 and ADR-004-nats-phase5.
type NATSIngester struct {
	url     string
	subject string
	durable string
	eventCh chan<- model.Envelope

	nc  *nats.Conn
	sub *nats.Subscription
}

// NewNATS builds a NATS ingester. subject is both the JetStream subject and the
// stream name; durable identifies the consumer so redelivery survives restarts.
func NewNATS(url, subject string, eventCh chan<- model.Envelope) *NATSIngester {
	return &NATSIngester{
		url:     url,
		subject: subject,
		durable: "streamrail-" + subject,
		eventCh: eventCh,
	}
}

// Start connects to NATS, ensures the JetStream stream exists, and subscribes.
// The subscription runs asynchronously until Stop is called.
func (n *NATSIngester) Start(ctx context.Context) error {
	nc, err := nats.Connect(n.url)
	if err != nil {
		return fmt.Errorf("nats connect %s: %w", n.url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return fmt.Errorf("jetstream context: %w", err)
	}
	if err := ensureStream(js, n.subject); err != nil {
		nc.Close()
		return err
	}

	sub, err := js.Subscribe(n.subject, n.handle(ctx),
		nats.Durable(n.durable),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return fmt.Errorf("subscribe %s: %w", n.subject, err)
	}

	n.nc = nc
	n.sub = sub
	return nil
}

// Stop drains the subscription and closes the connection.
func (n *NATSIngester) Stop() {
	if n.sub != nil {
		_ = n.sub.Drain()
	}
	if n.nc != nil {
		_ = n.nc.Drain()
		n.nc.Close()
	}
}

// handle decodes each message and enqueues it wrapped in an Envelope whose Ack
// defers the NATS acknowledgement until the pipeline has durably processed the
// event (end-to-end at-least-once, #19). A malformed message is terminated
// (never redelivered). A crash after enqueue but before the window Manager acks
// leaves the message unacked, so JetStream redelivers it — no event is lost.
func (n *NATSIngester) handle(ctx context.Context) nats.MsgHandler {
	return func(msg *nats.Msg) {
		ev, err := decodeEvent(msg.Data)
		if err != nil {
			_ = msg.Term() // poison message: don't redeliver
			return
		}
		env := model.Envelope{Event: ev, Ack: func() { _ = msg.Ack() }}
		select {
		case n.eventCh <- env:
			// Ack is now the pipeline's responsibility (fires after processing).
		case <-ctx.Done():
			_ = msg.Nak() // shutting down before enqueue: redeliver later
		}
	}
}

// decodeEvent parses a JSON event payload.
func decodeEvent(data []byte) (model.Event, error) {
	var ev model.Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return model.Event{}, fmt.Errorf("decode event: %w", err)
	}
	return ev, nil
}

// ensureStream creates the JetStream stream for subject if it does not exist.
// The stream name is the subject (a valid stream name for our subjects).
func ensureStream(js nats.JetStreamContext, subject string) error {
	if _, err := js.StreamInfo(subject); err == nil {
		return nil
	} else if !errors.Is(err, nats.ErrStreamNotFound) {
		return fmt.Errorf("stream info %s: %w", subject, err)
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Subjects: []string{subject},
	})
	if err != nil {
		return fmt.Errorf("add stream %s: %w", subject, err)
	}
	return nil
}
