package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultTopic        = "order-events"
	DefaultConsumerName = "order-effects-v1"
)

type Config struct {
	DatabaseURL string
	Brokers     []string
	Topic       string
	Consumer    string
}

type Service struct {
	pool     *pgxpool.Pool
	writer   *kafka.Writer
	reader   *kafka.Reader
	topic    string
	consumer string
}

type Order struct {
	ID             string `json:"id"`
	Key            string `json:"key"`
	IdempotencyKey string `json:"idempotencyKey"`
	Sequence       int64  `json:"sequence"`
	AmountCents    int64  `json:"amountCents"`
}

type Event struct {
	ID    string `json:"id"`
	Order Order  `json:"order"`
}

type OrderRequest struct {
	Key            string `json:"key"`
	IdempotencyKey string `json:"idempotencyKey"`
	AmountCents    int64  `json:"amountCents"`
}

type QuarantineRequest struct {
	Reason string `json:"reason"`
}

type DeadLetterRecord struct {
	EventID       string          `json:"eventId"`
	Consumer      string          `json:"consumer"`
	PartitionKey  string          `json:"partitionKey"`
	Sequence      int64           `json:"sequence"`
	Payload       json.RawMessage `json:"payload"`
	FailureReason string          `json:"failureReason"`
	Attempts      int             `json:"attempts"`
	QuarantinedAt time.Time       `json:"quarantinedAt"`
	ReplayedAt    *time.Time      `json:"replayedAt,omitempty"`
}

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

func Open(ctx context.Context, config Config) (*Service, error) {
	if strings.TrimSpace(config.DatabaseURL) == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	if len(config.Brokers) == 0 || strings.TrimSpace(config.Brokers[0]) == "" {
		return nil, errors.New("KAFKA_BROKERS is required")
	}
	if config.Topic == "" {
		config.Topic = DefaultTopic
	}
	if config.Consumer == "" {
		config.Consumer = DefaultConsumerName
	}
	poolConfig, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	brokers := append([]string(nil), config.Brokers...)
	service := &Service{
		pool: pool, topic: config.Topic, consumer: config.Consumer,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        config.Topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchTimeout: 10 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers, Topic: config.Topic, GroupID: config.Consumer,
			MinBytes: 1, MaxBytes: 10e6, MaxWait: 500 * time.Millisecond,
			CommitInterval: 0,
		}),
	}
	return service, nil
}

func (s *Service) Close() {
	if s.reader != nil {
		_ = s.reader.Close()
	}
	if s.writer != nil {
		_ = s.writer.Close()
	}
	s.pool.Close()
}

func (s *Service) Submit(ctx context.Context, request OrderRequest) (Order, bool, error) {
	request.Key = strings.TrimSpace(request.Key)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if request.Key == "" || request.IdempotencyKey == "" || request.AmountCents <= 0 {
		return Order{}, false, errors.New("key, idempotencyKey and positive amountCents are required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Order{}, false, fmt.Errorf("begin order transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	prior, err := getIdempotentOrder(ctx, tx, request.IdempotencyKey)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Order{}, false, fmt.Errorf("check idempotency key: %w", err)
	}
	if err == nil {
		if prior.Key != request.Key || prior.AmountCents != request.AmountCents {
			return Order{}, false, errors.New("idempotency key already belongs to a different order payload")
		}
		if err := tx.Commit(ctx); err != nil {
			return Order{}, false, fmt.Errorf("finish idempotent retry: %w", err)
		}
		return prior, false, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO producer_partition_cursors (partition_key, last_sequence)
		VALUES ($1, 0) ON CONFLICT (partition_key) DO NOTHING`, request.Key); err != nil {
		return Order{}, false, fmt.Errorf("initialize producer cursor: %w", err)
	}
	var sequence int64
	if err := tx.QueryRow(ctx, `
		SELECT last_sequence + 1 FROM producer_partition_cursors
		WHERE partition_key = $1 FOR UPDATE`, request.Key).Scan(&sequence); err != nil {
		return Order{}, false, fmt.Errorf("lock producer cursor: %w", err)
	}

	orderID, err := randomID("ord")
	if err != nil {
		return Order{}, false, err
	}
	order := Order{
		ID: orderID, Key: request.Key, IdempotencyKey: request.IdempotencyKey,
		Sequence: sequence, AmountCents: request.AmountCents,
	}
	event := Event{ID: "evt-" + orderID, Order: order}
	payload, err := json.Marshal(event)
	if err != nil {
		return Order{}, false, fmt.Errorf("encode outbox event: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (order_id, partition_key, idempotency_key, sequence_number, amount_cents)
		VALUES ($1, $2, $3, $4, $5)`, order.ID, order.Key, order.IdempotencyKey, sequence, order.AmountCents)
	if err != nil {
		if isUniqueViolation(err) {
			_ = tx.Rollback(ctx)
			prior, lookupErr := getIdempotentOrder(ctx, s.pool, request.IdempotencyKey)
			if lookupErr == nil && prior.Key == request.Key && prior.AmountCents == request.AmountCents {
				return prior, false, nil
			}
			if lookupErr == nil {
				return Order{}, false, errors.New("idempotency key already belongs to a different order payload")
			}
		}
		return Order{}, false, fmt.Errorf("insert order: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (event_id, order_id, partition_key, sequence_number, payload)
		VALUES ($1, $2, $3, $4, $5)`, event.ID, order.ID, order.Key, sequence, payload)
	if err != nil {
		return Order{}, false, fmt.Errorf("insert outbox event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE producer_partition_cursors SET last_sequence = $2
		WHERE partition_key = $1`, request.Key, sequence); err != nil {
		return Order{}, false, fmt.Errorf("advance producer cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Order{}, false, fmt.Errorf("commit order and outbox: %w", err)
	}
	return order, true, nil
}

type rowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func getIdempotentOrder(ctx context.Context, queryer rowQueryer, key string) (Order, error) {
	var order Order
	err := queryer.QueryRow(ctx, `
		SELECT order_id, partition_key, idempotency_key, sequence_number, amount_cents
		FROM orders WHERE idempotency_key = $1`, key).Scan(
		&order.ID, &order.Key, &order.IdempotencyKey, &order.Sequence, &order.AmountCents,
	)
	return order, err
}

func (s *Service) PublishOne(ctx context.Context) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin outbox publish transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var eventID, key string
	var sequence int64
	var payload []byte
	err = tx.QueryRow(ctx, `
		SELECT current.event_id, current.partition_key, current.sequence_number, current.payload
		FROM outbox AS current
		WHERE current.published_at IS NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM outbox AS earlier
		      WHERE earlier.partition_key = current.partition_key
		        AND earlier.sequence_number < current.sequence_number
		        AND earlier.published_at IS NULL
		  )
		ORDER BY current.created_at, current.partition_key, current.sequence_number
		FOR UPDATE OF current SKIP LOCKED
		LIMIT 1`).Scan(&eventID, &key, &sequence, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim next outbox event: %w", err)
	}
	message := kafka.Message{
		Key: []byte(key), Value: payload,
		Headers: []kafka.Header{
			{Key: "event-id", Value: []byte(eventID)},
			{Key: "sequence", Value: []byte(fmt.Sprint(sequence))},
		},
	}
	if err := s.writer.WriteMessages(ctx, message); err != nil {
		return false, fmt.Errorf("publish outbox event %s: %w", eventID, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE outbox SET published_at = now() WHERE event_id = $1`, eventID); err != nil {
		return false, fmt.Errorf("mark outbox event %s published: %w", eventID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit publish marker for %s: %w", eventID, err)
	}
	return true, nil
}

func (s *Service) RunRelay(ctx context.Context, poll time.Duration) error {
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		published, err := s.PublishOne(ctx)
		if err != nil {
			log.Printf("outbox publish failed; event remains retryable: %v", err)
		}
		if published {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Service) RunConsumer(ctx context.Context) error {
	for {
		message, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch broker message: %w", err)
		}
		eventID := header(message.Headers, "event-id")
		if eventID == "" {
			return errors.New("broker event has no event-id header; refusing to commit offset")
		}
		err = s.Apply(ctx, eventID, message.Value)
		if err != nil {
			var permanent PermanentError
			if !errors.As(err, &permanent) {
				return fmt.Errorf("apply event %s; broker offset left uncommitted: %w", eventID, err)
			}
			if err := s.Quarantine(ctx, eventID, permanent.Error()); err != nil {
				return fmt.Errorf("quarantine invalid event %s: %w", eventID, err)
			}
		}
		if err := s.reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit broker offset after database decision for %s: %w", eventID, err)
		}
	}
}

func (s *Service) Apply(ctx context.Context, eventID string, payload []byte) error {
	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return PermanentError{Err: fmt.Errorf("invalid event JSON: %w", err)}
	}
	if event.ID != eventID || event.Order.ID == "" || event.Order.Key == "" ||
		event.Order.Sequence < 1 || event.Order.AmountCents <= 0 {
		return PermanentError{Err: errors.New("event identity or order fields are invalid")}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin consumer transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	var active bool
	err = tx.QueryRow(ctx, `
		SELECT replayed_at IS NULL FROM dead_letters
		WHERE consumer_name = $1 AND event_id = $2`, s.consumer, eventID).Scan(&active)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check quarantine state: %w", err)
	}
	if err == nil && active {
		return tx.Commit(ctx)
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_events (consumer_name, event_id, partition_key, sequence_number)
		VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, event_id) DO NOTHING`,
		s.consumer, event.ID, event.Order.Key, event.Order.Sequence)
	if err != nil {
		return fmt.Errorf("insert consumer dedup record: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	var orderMatches bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM orders
			WHERE order_id = $1 AND partition_key = $2
			  AND sequence_number = $3 AND amount_cents = $4
		)`, event.Order.ID, event.Order.Key, event.Order.Sequence, event.Order.AmountCents).Scan(&orderMatches); err != nil {
		return fmt.Errorf("validate order event: %w", err)
	}
	if !orderMatches {
		return PermanentError{Err: fmt.Errorf("event %s does not match its committed order", eventID)}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO consumer_partition_cursors (consumer_name, partition_key, last_sequence)
		VALUES ($1, $2, 0) ON CONFLICT DO NOTHING`, s.consumer, event.Order.Key); err != nil {
		return fmt.Errorf("initialize consumer cursor: %w", err)
	}
	var lastSequence int64
	if err := tx.QueryRow(ctx, `
		SELECT last_sequence FROM consumer_partition_cursors
		WHERE consumer_name = $1 AND partition_key = $2 FOR UPDATE`,
		s.consumer, event.Order.Key).Scan(&lastSequence); err != nil {
		return fmt.Errorf("lock consumer cursor: %w", err)
	}
	if event.Order.Sequence != lastSequence+1 {
		return fmt.Errorf("event %s is out of order for key %s: got %d, want %d",
			event.ID, event.Order.Key, event.Order.Sequence, lastSequence+1)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO order_effects (consumer_name, event_id, partition_key, sequence_number, order_id)
		VALUES ($1, $2, $3, $4, $5)`,
		s.consumer, event.ID, event.Order.Key, event.Order.Sequence, event.Order.ID)
	if err != nil {
		return fmt.Errorf("apply business effect: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE consumer_partition_cursors SET last_sequence = $3
		WHERE consumer_name = $1 AND partition_key = $2`,
		s.consumer, event.Order.Key, event.Order.Sequence); err != nil {
		return fmt.Errorf("advance consumer cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dedup record and business effect: %w", err)
	}
	return nil
}

func (s *Service) Quarantine(ctx context.Context, eventID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin quarantine transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		INSERT INTO dead_letters (event_id, consumer_name, partition_key, sequence_number,
			payload, failure_reason, attempts)
		SELECT event_id, $2, partition_key, sequence_number, payload, $3, 1
		FROM outbox WHERE event_id = $1
		ON CONFLICT (consumer_name, event_id) DO UPDATE
		SET attempts = dead_letters.attempts + 1, failure_reason = EXCLUDED.failure_reason,
		    replayed_at = NULL`,
		eventID, s.consumer, reason)
	if err != nil {
		return fmt.Errorf("write dead-letter record: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("cannot quarantine event %s: it is not present in the outbox", eventID)
	}
	return tx.Commit(ctx)
}

func (s *Service) Replay(ctx context.Context, eventID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin replay transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	var processed bool
	err = tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM processed_events WHERE consumer_name = $1 AND event_id = $2)`,
		s.consumer, eventID).Scan(&processed)
	if err != nil {
		return fmt.Errorf("check replay dedup state: %w", err)
	}
	if processed {
		return fmt.Errorf("event %s already has a committed business effect and cannot be replayed", eventID)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE dead_letters SET replayed_at = now()
		WHERE consumer_name = $1 AND event_id = $2 AND replayed_at IS NULL`,
		s.consumer, eventID)
	if err != nil {
		return fmt.Errorf("mark dead letter replayed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("event %s is not an active dead letter", eventID)
	}
	tag, err = tx.Exec(ctx, `UPDATE outbox SET published_at = NULL WHERE event_id = $1`, eventID)
	if err != nil {
		return fmt.Errorf("requeue outbox event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("event %s is missing from the outbox", eventID)
	}
	return tx.Commit(ctx)
}

func (s *Service) Stats(ctx context.Context) (map[string]int64, error) {
	const query = `
		SELECT
			(SELECT count(*) FROM orders),
			(SELECT count(*) FROM order_effects WHERE consumer_name = $1),
			(SELECT count(*) FROM outbox WHERE published_at IS NULL),
			(SELECT count(*) FROM dead_letters WHERE consumer_name = $1 AND replayed_at IS NULL),
			(SELECT count(*) FROM processed_events WHERE consumer_name = $1)`
	var orders, effects, pending, dlq, processed int64
	if err := s.pool.QueryRow(ctx, query, s.consumer).Scan(&orders, &effects, &pending, &dlq, &processed); err != nil {
		return nil, fmt.Errorf("read pipeline stats: %w", err)
	}
	return map[string]int64{
		"orders": orders, "effects": effects, "outboxPending": pending,
		"deadLettered": dlq, "processedEvents": processed,
	}, nil
}

func (s *Service) DeadLetters(ctx context.Context) ([]DeadLetterRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, consumer_name, partition_key, sequence_number, payload,
		       failure_reason, attempts, quarantined_at, replayed_at
		FROM dead_letters
		WHERE consumer_name = $1 AND replayed_at IS NULL
		ORDER BY quarantined_at, event_id`, s.consumer)
	if err != nil {
		return nil, fmt.Errorf("list active dead letters: %w", err)
	}
	defer rows.Close()
	items := make([]DeadLetterRecord, 0)
	for rows.Next() {
		var item DeadLetterRecord
		if err := rows.Scan(
			&item.EventID, &item.Consumer, &item.PartitionKey, &item.Sequence,
			&item.Payload, &item.FailureReason, &item.Attempts, &item.QuarantinedAt, &item.ReplayedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters: %w", err)
	}
	return items, nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if err := s.pool.Ping(r.Context()); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := s.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, stats)
	})
	mux.HandleFunc("GET /dlq", func(w http.ResponseWriter, r *http.Request) {
		items, err := s.DeadLetters(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, items)
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		var request OrderRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid JSON order request", http.StatusBadRequest)
			return
		}
		order, created, err := s.Submit(r.Context(), request)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		status := http.StatusCreated
		if !created {
			status = http.StatusOK
		}
		writeJSON(w, status, map[string]any{"order": order, "created": created})
	})
	mux.HandleFunc("POST /admin/events/{eventID}/quarantine", func(w http.ResponseWriter, r *http.Request) {
		var request QuarantineRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, "invalid quarantine request", http.StatusBadRequest)
			return
		}
		if request.Reason == "" {
			request.Reason = "operator requested quarantine"
		}
		if err := s.Quarantine(r.Context(), r.PathValue("eventID"), request.Reason); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"eventId": r.PathValue("eventID"), "state": "quarantined"})
	})
	mux.HandleFunc("POST /admin/events/{eventID}/replay", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Replay(r.Context(), r.PathValue("eventID")); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"eventId": r.PathValue("eventID"), "state": "requeued"})
	})
	return mux
}

func header(headers []kafka.Header, name string) string {
	for _, item := range headers {
		if item.Key == name {
			return string(item.Value)
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write JSON response: %v", err)
	}
}
