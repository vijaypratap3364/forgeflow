// Package broker defines ForgeFlow's durable task-transport boundary.
package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// MessageID is the stable identifier used to deduplicate one logical task
// dispatch. ForgeFlow uses its stable task-attempt ID at this boundary.
type MessageID string

// Topic isolates task deliveries, normally to one workflow run.
type Topic string

// ConsumerID identifies a durable competing-consumer group.
type ConsumerID string

// TaskMessage is an implementation-neutral durable task envelope. Headers are
// lowercase propagation metadata and are not part of stable-ID content checks;
// the first accepted publication supplies delivery metadata.
type TaskMessage struct {
	ID      MessageID
	Topic   Topic
	Body    []byte
	Headers map[string]string
}

// Validate checks the transport-independent message invariants.
func (message TaskMessage) Validate() error {
	if !validIdentifier(string(message.ID)) {
		return &ValidationError{Field: "message ID", Value: string(message.ID)}
	}
	if !validIdentifier(string(message.Topic)) {
		return &ValidationError{Field: "topic", Value: string(message.Topic)}
	}
	if len(message.Body) == 0 {
		return &ValidationError{Field: "body", Reason: "must not be empty"}
	}
	for name, value := range message.Headers {
		if !validHeaderName(name) {
			return &ValidationError{Field: "header name", Value: name}
		}
		if strings.ContainsAny(value, "\r\n") {
			return &ValidationError{Field: "header value", Reason: "must not contain a line break"}
		}
	}
	return nil
}

func validHeaderName(name string) bool {
	if name == "" || name != strings.ToLower(name) {
		return false
	}
	for index := 0; index < len(name); index++ {
		character := name[index]
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", rune(character)) {
			continue
		}
		return false
	}
	return true
}

// Subscription selects a topic through one durable competing-consumer group.
// Calls using the same ConsumerID and Topic share deliveries.
type Subscription struct {
	ConsumerID ConsumerID
	Topic      Topic
}

// Validate checks the transport-independent subscription invariants.
func (subscription Subscription) Validate() error {
	if !validIdentifier(string(subscription.ConsumerID)) {
		return &ValidationError{Field: "consumer ID", Value: string(subscription.ConsumerID)}
	}
	if !validIdentifier(string(subscription.Topic)) {
		return &ValidationError{Field: "topic", Value: string(subscription.Topic)}
	}
	return nil
}

// Delivery is one explicitly acknowledged broker delivery. Ack confirms that
// the corresponding state transition is durable. Nack requests redelivery.
// Progress extends the broker's acknowledgement deadline during long tasks.
type Delivery interface {
	Message() TaskMessage
	DeliveryCount() uint64
	Ack(context.Context) error
	Nack(context.Context) error
	Progress(context.Context) error
}

// Broker publishes durable task messages and receives them through competing
// consumers. Implementations must be safe for concurrent use.
type Broker interface {
	Publish(context.Context, TaskMessage) error
	Receive(context.Context, Subscription) (Delivery, error)
}

// ReadinessChecker is implemented by brokers that can verify their current
// ability to serve scheduler and worker operations.
type ReadinessChecker interface {
	Ping(context.Context) error
}

// ErrClosed reports use of a broker after shutdown.
var ErrClosed = errors.New("task broker is closed")

// ErrDeliverySettled reports an acknowledgement operation on an inactive or
// superseded delivery.
var ErrDeliverySettled = errors.New("task delivery is no longer active")

// ValidationError identifies an invalid broker message or subscription field.
type ValidationError struct {
	Field  string
	Value  string
	Reason string
}

// Error returns a contextual validation message.
func (e *ValidationError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("invalid broker %s: %s", e.Field, e.Reason)
	}
	return fmt.Sprintf("invalid broker %s %q", e.Field, e.Value)
}

// MessageConflictError reports reuse of a stable message ID with different
// topic or body content.
type MessageConflictError struct {
	MessageID MessageID
}

// Error returns a contextual stable-ID conflict message.
func (e *MessageConflictError) Error() string {
	return fmt.Sprintf("broker message %q already exists with different content", e.MessageID)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("task broker context is nil")
	}
	return ctx.Err()
}

func validIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, character := range identifier {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func cloneMessage(message TaskMessage) TaskMessage {
	clone := message
	clone.Body = append([]byte(nil), message.Body...)
	if message.Headers != nil {
		clone.Headers = make(map[string]string, len(message.Headers))
		for name, value := range message.Headers {
			clone.Headers[name] = value
		}
	}
	return clone
}
