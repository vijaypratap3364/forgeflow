package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultNATSStream          = "FORGEFLOW_TASKS"
	defaultNATSSubjectPrefix   = "forgeflow.tasks"
	defaultNATSAckWait         = time.Minute
	defaultNATSDuplicateWindow = 2 * time.Minute
	defaultNATSMaxAge          = 7 * 24 * time.Hour
	defaultNATSConnectTimeout  = 5 * time.Second
)

// NATSConfig configures a file-backed JetStream work queue. MaxDeliver controls
// broker redelivery, not ForgeFlow task attempts; -1 keeps broker redelivery
// unlimited so persisted task policy remains authoritative.
type NATSConfig struct {
	URL             string
	StreamName      string
	SubjectPrefix   string
	AckWait         time.Duration
	DuplicateWindow time.Duration
	MaxAge          time.Duration
	MaxDeliver      int
	ConnectTimeout  time.Duration
}

// DefaultNATSConfig returns production-oriented defaults for a single-node or
// clustered JetStream deployment.
func DefaultNATSConfig() NATSConfig {
	return NATSConfig{
		URL:             nats.DefaultURL,
		StreamName:      defaultNATSStream,
		SubjectPrefix:   defaultNATSSubjectPrefix,
		AckWait:         defaultNATSAckWait,
		DuplicateWindow: defaultNATSDuplicateWindow,
		MaxAge:          defaultNATSMaxAge,
		MaxDeliver:      -1,
		ConnectTimeout:  defaultNATSConnectTimeout,
	}
}

// Validate reports invalid JetStream configuration before connecting.
func (c NATSConfig) Validate() error {
	if strings.TrimSpace(c.URL) == "" {
		return &ValidationError{Field: "url", Reason: "must not be empty"}
	}
	if strings.TrimSpace(c.StreamName) == "" {
		return &ValidationError{Field: "stream_name", Reason: "must not be empty"}
	}
	if strings.ContainsAny(c.StreamName, ".*>\\/") || strings.IndexFunc(c.StreamName, func(character rune) bool {
		return unicode.IsSpace(character) || unicode.IsControl(character)
	}) >= 0 {
		return &ValidationError{Field: "stream_name", Reason: "contains a character JetStream does not allow"}
	}
	if err := validateSubjectPrefix(c.SubjectPrefix); err != nil {
		return err
	}
	if c.AckWait <= 0 {
		return &ValidationError{Field: "ack_wait", Reason: "must be greater than zero"}
	}
	if c.DuplicateWindow <= 0 {
		return &ValidationError{Field: "duplicate_window", Reason: "must be greater than zero"}
	}
	if c.MaxAge <= 0 {
		return &ValidationError{Field: "max_age", Reason: "must be greater than zero"}
	}
	if c.MaxDeliver != -1 && c.MaxDeliver <= 0 {
		return &ValidationError{Field: "max_deliver", Reason: "must be -1 or greater than zero"}
	}
	if c.ConnectTimeout <= 0 {
		return &ValidationError{Field: "connect_timeout", Reason: "must be greater than zero"}
	}
	return nil
}

func validateSubjectPrefix(prefix string) error {
	if strings.TrimSpace(prefix) == "" {
		return &ValidationError{Field: "subject_prefix", Reason: "must not be empty"}
	}
	for _, token := range strings.Split(prefix, ".") {
		if token == "" || strings.ContainsAny(token, "*>") {
			return &ValidationError{Field: "subject_prefix", Reason: "must contain only non-wildcard subject tokens"}
		}
		for _, character := range token {
			if unicode.IsSpace(character) || unicode.IsControl(character) {
				return &ValidationError{Field: "subject_prefix", Reason: "must not contain whitespace or control characters"}
			}
		}
	}
	return nil
}

// NATSBroker transports task messages through a durable JetStream work queue.
// It owns its NATS connection and must be closed when no longer needed.
type NATSBroker struct {
	conn      *nats.Conn
	jetStream jetstream.JetStream
	config    NATSConfig

	mu        sync.Mutex
	consumers map[string]jetstream.Consumer
	closed    bool
}

// OpenNATSBroker connects to NATS and creates or reconciles ForgeFlow's task
// stream. Existing stream messages and durable consumers are preserved.
func OpenNATSBroker(ctx context.Context, config NATSConfig) (*NATSBroker, error) {
	if ctx == nil {
		return nil, &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	connectTimeout := config.ConnectTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remaining < connectTimeout {
			connectTimeout = remaining
		}
	}

	conn, err := nats.Connect(
		config.URL,
		nats.Name("ForgeFlow task broker"),
		nats.Timeout(connectTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	if err := ctx.Err(); err != nil {
		conn.Close()
		return nil, err
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("initialize JetStream client: %w", err)
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        config.StreamName,
		Description: "ForgeFlow durable task dispatch",
		Subjects:    []string{config.SubjectPrefix + ".>"},
		Retention:   jetstream.WorkQueuePolicy,
		Storage:     jetstream.FileStorage,
		Discard:     jetstream.DiscardOld,
		MaxAge:      config.MaxAge,
		Duplicates:  config.DuplicateWindow,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create or update JetStream stream %q: %w", config.StreamName, err)
	}

	return &NATSBroker{
		conn:      conn,
		jetStream: js,
		config:    config,
		consumers: make(map[string]jetstream.Consumer),
	}, nil
}

// Publish durably stores message before returning. Repeated publication of a
// MessageID is deduplicated by JetStream within the configured duplicate
// window; persisted task state handles duplicates outside that window.
func (b *NATSBroker) Publish(ctx context.Context, message TaskMessage) error {
	if ctx == nil {
		return &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	if err := message.Validate(); err != nil {
		return err
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}

	_, err := b.jetStream.Publish(
		ctx,
		b.subject(message.Topic),
		append([]byte(nil), message.Body...),
		jetstream.WithMsgID(string(message.ID)),
		jetstream.WithExpectStream(b.config.StreamName),
	)
	if err != nil {
		return fmt.Errorf("publish task message %q: %w", message.ID, err)
	}
	return nil
}

// Receive waits for one message from a durable pull consumer. Concurrent calls
// with the same subscription compete for deliveries rather than fan them out.
func (b *NATSBroker) Receive(ctx context.Context, subscription Subscription) (Delivery, error) {
	if ctx == nil {
		return nil, &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	if err := subscription.Validate(); err != nil {
		return nil, err
	}
	consumer, err := b.consumer(ctx, subscription)
	if err != nil {
		return nil, err
	}

	message, err := consumer.Next(jetstream.FetchContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("receive task message: %w", err)
	}
	metadata, err := message.Metadata()
	if err != nil {
		return nil, fmt.Errorf("read task message metadata: %w", err)
	}
	return &natsDelivery{
		message: TaskMessage{
			ID:    MessageID(message.Headers().Get(jetstream.MsgIDHeader)),
			Topic: subscription.Topic,
			Body:  append([]byte(nil), message.Data()...),
		},
		deliveryCount: metadata.NumDelivered,
		messageHandle: message,
	}, nil
}

func (b *NATSBroker) consumer(ctx context.Context, subscription Subscription) (jetstream.Consumer, error) {
	key := string(subscription.ConsumerID) + "\x00" + string(subscription.Topic)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrClosed
	}
	if consumer, ok := b.consumers[key]; ok {
		return consumer, nil
	}

	durable := hashedName("FF", key)
	consumer, err := b.jetStream.CreateOrUpdateConsumer(ctx, b.config.StreamName, jetstream.ConsumerConfig{
		Name:          durable,
		Durable:       durable,
		Description:   "ForgeFlow competing task workers",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       b.config.AckWait,
		MaxDeliver:    b.config.MaxDeliver,
		FilterSubject: b.subject(subscription.Topic),
		ReplayPolicy:  jetstream.ReplayInstantPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create or update JetStream consumer %q: %w", durable, err)
	}
	b.consumers[key] = consumer
	return consumer, nil
}

func (b *NATSBroker) subject(topic Topic) string {
	return b.config.SubjectPrefix + "." + hashedName("t", string(topic))
}

func hashedName(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "_" + hex.EncodeToString(sum[:])
}

func (b *NATSBroker) ensureOpen() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	return nil
}

// Close drains and closes the owned NATS connection. It is safe to call more
// than once.
func (b *NATSBroker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	if err := b.conn.Drain(); err != nil {
		b.conn.Close()
		return fmt.Errorf("drain NATS connection: %w", err)
	}
	return nil
}

type natsDelivery struct {
	message       TaskMessage
	deliveryCount uint64
	messageHandle jetstream.Msg

	mu      sync.Mutex
	settled bool
}

func (d *natsDelivery) Message() TaskMessage {
	d.mu.Lock()
	defer d.mu.Unlock()
	return TaskMessage{
		ID:    d.message.ID,
		Topic: d.message.Topic,
		Body:  append([]byte(nil), d.message.Body...),
	}
}

func (d *natsDelivery) DeliveryCount() uint64 {
	return d.deliveryCount
}

func (d *natsDelivery) Ack(ctx context.Context) error {
	if ctx == nil {
		return &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrDeliverySettled
	}
	if err := d.messageHandle.DoubleAck(ctx); err != nil {
		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			return ErrDeliverySettled
		}
		return fmt.Errorf("acknowledge task message %q: %w", d.message.ID, err)
	}
	d.settled = true
	return nil
}

func (d *natsDelivery) Nack(ctx context.Context) error {
	if ctx == nil {
		return &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrDeliverySettled
	}
	if err := d.messageHandle.Nak(); err != nil {
		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			return ErrDeliverySettled
		}
		return fmt.Errorf("negatively acknowledge task message %q: %w", d.message.ID, err)
	}
	d.settled = true
	return nil
}

func (d *natsDelivery) Progress(ctx context.Context) error {
	if ctx == nil {
		return &ValidationError{Field: "context", Reason: "must not be nil"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settled {
		return ErrDeliverySettled
	}
	if err := d.messageHandle.InProgress(); err != nil {
		if errors.Is(err, jetstream.ErrMsgAlreadyAckd) {
			return ErrDeliverySettled
		}
		return fmt.Errorf("extend task message acknowledgement deadline %q: %w", d.message.ID, err)
	}
	return nil
}
