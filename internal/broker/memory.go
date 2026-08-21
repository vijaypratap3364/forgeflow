package broker

import (
	"bytes"
	"context"
	"sync"
)

// InMemoryBroker is a process-local durable-task transport for unit tests and
// lightweight development. Durability lasts for the broker object's lifetime.
type InMemoryBroker struct {
	mu            sync.Mutex
	closed        bool
	messages      map[MessageID]TaskMessage
	topics        map[Topic][]MessageID
	subscriptions map[memorySubscriptionKey]*memorySubscription
}

type memorySubscriptionKey struct {
	consumerID ConsumerID
	topic      Topic
}

type memorySubscription struct {
	queued        []MessageID
	queuedSet     map[MessageID]struct{}
	inFlight      map[MessageID]uint64
	acked         map[MessageID]struct{}
	deliveryCount map[MessageID]uint64
	notify        chan struct{}
}

// NewInMemoryBroker creates an empty process-local broker.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		messages:      make(map[MessageID]TaskMessage),
		topics:        make(map[Topic][]MessageID),
		subscriptions: make(map[memorySubscriptionKey]*memorySubscription),
	}
}

// Publish stores a detached message and makes it available to every existing
// durable subscription for its topic. Re-publishing identical content under
// the same ID is idempotent.
func (broker *InMemoryBroker) Publish(ctx context.Context, message TaskMessage) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := message.Validate(); err != nil {
		return err
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return ErrClosed
	}
	if existing, exists := broker.messages[message.ID]; exists {
		if existing.Topic == message.Topic && bytes.Equal(existing.Body, message.Body) {
			return nil
		}
		return &MessageConflictError{MessageID: message.ID}
	}

	message = cloneMessage(message)
	broker.messages[message.ID] = message
	broker.topics[message.Topic] = append(broker.topics[message.Topic], message.ID)
	for key, subscription := range broker.subscriptions {
		if key.topic == message.Topic {
			enqueueMemoryMessage(subscription, message.ID)
		}
	}
	return nil
}

// Receive waits for the next available message for a durable subscription.
func (broker *InMemoryBroker) Receive(ctx context.Context, subscription Subscription) (Delivery, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := subscription.Validate(); err != nil {
		return nil, err
	}
	key := memorySubscriptionKey{consumerID: subscription.ConsumerID, topic: subscription.Topic}

	for {
		broker.mu.Lock()
		if broker.closed {
			broker.mu.Unlock()
			return nil, ErrClosed
		}
		state := broker.subscriptionLocked(key)
		if len(state.queued) > 0 {
			messageID := state.queued[0]
			state.queued = state.queued[1:]
			delete(state.queuedSet, messageID)
			state.deliveryCount[messageID]++
			count := state.deliveryCount[messageID]
			state.inFlight[messageID] = count
			message := cloneMessage(broker.messages[messageID])
			broker.mu.Unlock()
			return &memoryDelivery{
				broker:  broker,
				key:     key,
				message: message,
				count:   count,
			}, nil
		}
		notify := state.notify
		broker.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

// Close releases blocked receivers. It is safe to call more than once.
func (broker *InMemoryBroker) Close() error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil
	}
	broker.closed = true
	for _, subscription := range broker.subscriptions {
		signalMemorySubscription(subscription)
	}
	return nil
}

func (broker *InMemoryBroker) subscriptionLocked(key memorySubscriptionKey) *memorySubscription {
	if subscription, exists := broker.subscriptions[key]; exists {
		return subscription
	}
	subscription := &memorySubscription{
		queuedSet:     make(map[MessageID]struct{}),
		inFlight:      make(map[MessageID]uint64),
		acked:         make(map[MessageID]struct{}),
		deliveryCount: make(map[MessageID]uint64),
		notify:        make(chan struct{}, 1),
	}
	for _, messageID := range broker.topics[key.topic] {
		enqueueMemoryMessage(subscription, messageID)
	}
	broker.subscriptions[key] = subscription
	return subscription
}

func enqueueMemoryMessage(subscription *memorySubscription, messageID MessageID) {
	if _, acked := subscription.acked[messageID]; acked {
		return
	}
	if _, queued := subscription.queuedSet[messageID]; queued {
		return
	}
	if _, inFlight := subscription.inFlight[messageID]; inFlight {
		return
	}
	subscription.queued = append(subscription.queued, messageID)
	subscription.queuedSet[messageID] = struct{}{}
	signalMemorySubscription(subscription)
}

func signalMemorySubscription(subscription *memorySubscription) {
	select {
	case subscription.notify <- struct{}{}:
	default:
	}
}

type memoryDelivery struct {
	broker  *InMemoryBroker
	key     memorySubscriptionKey
	message TaskMessage
	count   uint64
}

func (delivery *memoryDelivery) Message() TaskMessage {
	return cloneMessage(delivery.message)
}

func (delivery *memoryDelivery) DeliveryCount() uint64 {
	return delivery.count
}

func (delivery *memoryDelivery) Ack(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	delivery.broker.mu.Lock()
	defer delivery.broker.mu.Unlock()
	if delivery.broker.closed {
		return ErrClosed
	}
	subscription := delivery.broker.subscriptions[delivery.key]
	if subscription == nil || subscription.inFlight[delivery.message.ID] != delivery.count {
		return ErrDeliverySettled
	}
	delete(subscription.inFlight, delivery.message.ID)
	subscription.acked[delivery.message.ID] = struct{}{}
	return nil
}

func (delivery *memoryDelivery) Nack(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	delivery.broker.mu.Lock()
	defer delivery.broker.mu.Unlock()
	if delivery.broker.closed {
		return ErrClosed
	}
	subscription := delivery.broker.subscriptions[delivery.key]
	if subscription == nil || subscription.inFlight[delivery.message.ID] != delivery.count {
		return ErrDeliverySettled
	}
	delete(subscription.inFlight, delivery.message.ID)
	enqueueMemoryMessage(subscription, delivery.message.ID)
	return nil
}

func (delivery *memoryDelivery) Progress(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	delivery.broker.mu.Lock()
	defer delivery.broker.mu.Unlock()
	if delivery.broker.closed {
		return ErrClosed
	}
	subscription := delivery.broker.subscriptions[delivery.key]
	if subscription == nil || subscription.inFlight[delivery.message.ID] != delivery.count {
		return ErrDeliverySettled
	}
	return nil
}

var _ Broker = (*InMemoryBroker)(nil)
