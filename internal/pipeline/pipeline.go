package pipeline

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	ErrNoWork         = errors.New("no work available")
	ErrNetworkBlocked = errors.New("network partition blocks pipeline progress")
	ErrOutOfOrder     = errors.New("event is not the next sequence for its key")
	ErrIdempotency    = errors.New("idempotency key was already used with a different request")
	ErrInvalidOrder   = errors.New("order key, idempotency key, and positive amount are required")
)

type State string

const (
	StatePending   State = "outbox-pending"
	StatePublished State = "broker-published"
	StateApplied   State = "consumer-applied"
	StateDead      State = "dead-lettered"
)

type Fault string

const (
	FaultNone           Fault = ""
	FaultBrokerBefore   Fault = "broker-before-publish"
	FaultBrokerAfter    Fault = "broker-after-publish"
	FaultConsumerBefore Fault = "consumer-before-commit"
	FaultConsumerAfter  Fault = "consumer-after-commit"
)

type Order struct {
	ID             string `json:"id"`
	Key            string `json:"key"`
	IdempotencyKey string `json:"idempotencyKey"`
	Sequence       int    `json:"sequence"`
	AmountCents    int64  `json:"amountCents"`
}

type Event struct {
	ID          string `json:"id"`
	Order       Order  `json:"order"`
	State       State  `json:"state"`
	PublishTry  int    `json:"publishTry"`
	DeliveryTry int    `json:"deliveryTry"`
}

type DeadLetter struct {
	EventID string    `json:"eventId"`
	Reason  string    `json:"reason"`
	At      time.Time `json:"at"`
}

type Snapshot struct {
	Orders       []Order      `json:"orders"`
	Events       []Event      `json:"events"`
	Deliveries   []string     `json:"deliveries"`
	Applied      []string     `json:"applied"`
	DeadLetters  []DeadLetter `json:"deadLetters"`
	NetworkBlock bool         `json:"networkBlocked"`
	Stats        Stats        `json:"stats"`
}

type Stats struct {
	Submitted           int `json:"submitted"`
	Applied             int `json:"applied"`
	OutboxPending       int `json:"outboxPending"`
	BrokerDepth         int `json:"brokerDepth"`
	DeadLettered        int `json:"deadLettered"`
	DuplicateRequests   int `json:"duplicateRequests"`
	DuplicateDeliveries int `json:"duplicateDeliveries"`
	Lost                int `json:"lost"`
	OrderingViolations  int `json:"orderingViolations"`
}

type Pipeline struct {
	mu sync.Mutex

	orders              []Order
	events              map[string]*Event
	eventOrder          []string
	deliveries          map[string][]string
	applied             map[string][]string
	seen                map[string]bool
	deadLetters         []DeadLetter
	idempotency         map[string]Order
	nextSequence        map[string]int
	duplicateRequests   int
	duplicateDeliveries int
	networkBlocked      bool
}

func New() *Pipeline {
	return &Pipeline{
		events:       make(map[string]*Event),
		deliveries:   make(map[string][]string),
		applied:      make(map[string][]string),
		seen:         make(map[string]bool),
		idempotency:  make(map[string]Order),
		nextSequence: make(map[string]int),
	}
}

func (p *Pipeline) Submit(key, idempotencyKey string, amountCents int64) (Order, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	key = strings.TrimSpace(key)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if key == "" || idempotencyKey == "" || amountCents <= 0 {
		return Order{}, false, ErrInvalidOrder
	}
	if old, ok := p.idempotency[idempotencyKey]; ok {
		if old.Key != key || old.AmountCents != amountCents {
			return Order{}, false, ErrIdempotency
		}
		p.duplicateRequests++
		return old, false, nil
	}

	p.nextSequence[key]++
	order := Order{
		ID:             fmt.Sprintf("ord-%06d", len(p.orders)+1),
		Key:            key,
		IdempotencyKey: idempotencyKey,
		Sequence:       p.nextSequence[key],
		AmountCents:    amountCents,
	}
	eventID := "evt-" + order.ID
	p.orders = append(p.orders, order)
	p.events[eventID] = &Event{ID: eventID, Order: order, State: StatePending}
	p.eventOrder = append(p.eventOrder, eventID)
	p.idempotency[idempotencyKey] = order
	return order, true, nil
}

func (p *Pipeline) PublishNext(key string, fault Fault) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.networkBlocked {
		return "", ErrNetworkBlocked
	}
	event := p.nextUnappliedLocked(key)
	if event == nil || event.State != StatePending {
		return "", ErrNoWork
	}
	event.PublishTry++
	if fault == FaultBrokerBefore {
		return event.ID, errors.New("broker stopped before accepting publish")
	}
	p.deliveries[key] = append(p.deliveries[key], event.ID)
	if fault == FaultBrokerAfter {
		return event.ID, errors.New("publish acknowledgement lost after broker accepted event")
	}
	event.State = StatePublished
	return event.ID, nil
}

func (p *Pipeline) ConsumeNext(key string, fault Fault) (string, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.networkBlocked {
		return "", false, ErrNetworkBlocked
	}
	queue := p.deliveries[key]
	if len(queue) == 0 {
		return "", false, ErrNoWork
	}
	eventID := queue[0]
	event := p.events[eventID]
	if event == nil {
		return eventID, false, fmt.Errorf("broker references unknown event %q", eventID)
	}
	event.DeliveryTry++

	if p.seen[eventID] {
		p.duplicateDeliveries++
		p.deliveries[key] = queue[1:]
		return eventID, true, nil
	}
	expected := len(p.applied[key]) + 1
	if event.Order.Sequence != expected {
		return eventID, false, fmt.Errorf("%w: key %q got %d, want %d", ErrOutOfOrder, key, event.Order.Sequence, expected)
	}
	if fault == FaultConsumerBefore {
		return eventID, false, errors.New("consumer stopped before transaction commit")
	}

	// The dedup record and business effect represent one database transaction.
	p.seen[eventID] = true
	p.applied[key] = append(p.applied[key], eventID)
	event.State = StateApplied
	if fault == FaultConsumerAfter {
		return eventID, false, errors.New("consumer acknowledgement lost after transaction commit")
	}
	p.deliveries[key] = queue[1:]
	return eventID, false, nil
}

func (p *Pipeline) QuarantineNext(key, reason string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	queue := p.deliveries[key]
	if len(queue) == 0 {
		return "", ErrNoWork
	}
	eventID := queue[0]
	event := p.events[eventID]
	if event == nil {
		return "", fmt.Errorf("broker references unknown event %q", eventID)
	}
	if p.seen[eventID] {
		return "", errors.New("already-applied event cannot be quarantined")
	}
	if reason == "" {
		reason = "consumer rejected event"
	}
	event.State = StateDead
	p.deadLetters = append(p.deadLetters, DeadLetter{EventID: eventID, Reason: reason, At: time.Now().UTC()})
	p.deliveries[key] = removeID(queue, eventID)
	return eventID, nil
}

func (p *Pipeline) Replay(eventID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	event := p.events[eventID]
	if event == nil {
		return fmt.Errorf("unknown event %q", eventID)
	}
	if event.State != StateDead {
		return fmt.Errorf("event %q is not in the dead-letter queue", eventID)
	}
	event.State = StatePending
	p.deadLetters = removeDeadLetter(p.deadLetters, eventID)
	return nil
}

func (p *Pipeline) SetNetworkBlocked(blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.networkBlocked = blocked
}

func (p *Pipeline) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	snapshot := Snapshot{
		Orders:       append([]Order(nil), p.orders...),
		Events:       make([]Event, 0, len(p.eventOrder)),
		Deliveries:   flatten(p.deliveries),
		Applied:      make([]string, 0, len(p.seen)),
		DeadLetters:  append([]DeadLetter{}, p.deadLetters...),
		NetworkBlock: p.networkBlocked,
	}
	for _, id := range p.eventOrder {
		snapshot.Events = append(snapshot.Events, *p.events[id])
	}
	for _, key := range sortedKeys(p.applied) {
		snapshot.Applied = append(snapshot.Applied, p.applied[key]...)
	}
	snapshot.Stats = p.statsLocked()
	return snapshot
}

func (p *Pipeline) CheckInvariants() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.orders) != len(p.events) {
		return fmt.Errorf("outbox coverage mismatch: %d orders, %d events", len(p.orders), len(p.events))
	}
	appliedIDs := make(map[string]bool)
	for key, ids := range p.applied {
		for index, id := range ids {
			if appliedIDs[id] {
				return fmt.Errorf("duplicate business effect for %s", id)
			}
			appliedIDs[id] = true
			event := p.events[id]
			if event == nil || event.Order.Key != key || event.Order.Sequence != index+1 {
				return fmt.Errorf("ordering invariant failed for key %s at position %d", key, index+1)
			}
			if !p.seen[id] || event.State != StateApplied {
				return fmt.Errorf("consumer dedup/state mismatch for %s", id)
			}
		}
	}
	for id, event := range p.events {
		if event.ID != id {
			return fmt.Errorf("event identity mismatch for %s", id)
		}
		if p.seen[id] != appliedIDs[id] {
			return fmt.Errorf("dedup store/effect mismatch for %s", id)
		}
		switch event.State {
		case StatePending, StatePublished, StateApplied, StateDead:
		default:
			return fmt.Errorf("event %s has invalid state %q", id, event.State)
		}
	}
	return nil
}

func (p *Pipeline) nextUnappliedLocked(key string) *Event {
	for _, id := range p.eventOrder {
		event := p.events[id]
		if event.Order.Key == key && event.State != StateApplied {
			return event
		}
	}
	return nil
}

func (p *Pipeline) statsLocked() Stats {
	stats := Stats{
		Submitted:           len(p.orders),
		Applied:             len(p.seen),
		DuplicateRequests:   p.duplicateRequests,
		DuplicateDeliveries: p.duplicateDeliveries,
	}
	for _, event := range p.events {
		switch event.State {
		case StatePending:
			stats.OutboxPending++
		case StateDead:
			stats.DeadLettered++
		}
	}
	for _, queue := range p.deliveries {
		stats.BrokerDepth += len(queue)
	}
	return stats
}

func removeID(ids []string, target string) []string {
	remaining := ids[:0]
	for _, id := range ids {
		if id != target {
			remaining = append(remaining, id)
		}
	}
	return remaining
}

func removeDeadLetter(items []DeadLetter, eventID string) []DeadLetter {
	remaining := items[:0]
	for _, item := range items {
		if item.EventID != eventID {
			remaining = append(remaining, item)
		}
	}
	return remaining
}

func flatten(values map[string][]string) []string {
	keys := sortedKeys(values)
	var result []string
	for _, key := range keys {
		result = append(result, values[key]...)
	}
	return result
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
