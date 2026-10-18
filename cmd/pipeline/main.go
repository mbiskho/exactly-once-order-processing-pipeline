package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"exactly-once-order-processing-pipeline/internal/durable"
	"exactly-once-order-processing-pipeline/internal/pipeline"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "demo":
		err = runDemo()
	case "chaos":
		err = runChaos(os.Args[2:])
	case "replay":
		err = runReplay(os.Args[2:])
	case "replay-live":
		err = runReplayLive(os.Args[2:])
	case "dlq":
		err = runDLQ()
	case "serve-api":
		err = runAPI()
	case "relay":
		err = runRelay()
	case "consume":
		err = runConsumer()
	case "submit":
		err = runSubmit(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
}

func runAPI() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	address := env("API_ADDR", ":8080")
	server := &http.Server{
		Addr: address, Handler: service.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP server shutdown: %v", err)
		}
	}()
	log.Printf("order API listening on %s", address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("serve API: %w", err)
	}
	return nil
}

func runRelay() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	log.Printf("outbox relay started for topic %s", env("KAFKA_TOPIC", durable.DefaultTopic))
	err = service.RunRelay(ctx, 200*time.Millisecond)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func runConsumer() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	log.Printf("consumer started with group %s", env("KAFKA_CONSUMER", durable.DefaultConsumerName))
	err = service.RunConsumer(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

func runSubmit(args []string) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	key := flags.String("key", "customer-42", "partition key")
	idempotencyKey := flags.String("idempotency-key", "", "client-generated idempotency key")
	amount := flags.Int64("amount-cents", 2599, "positive order amount in cents")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*idempotencyKey) == "" {
		return fmt.Errorf("provide -idempotency-key")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	order, created, err := service.Submit(ctx, durable.OrderRequest{
		Key: *key, IdempotencyKey: *idempotencyKey, AmountCents: *amount,
	})
	if err != nil {
		return err
	}
	return writeJSON(map[string]any{"order": order, "created": created})
}

func runReplayLive(args []string) error {
	flags := flag.NewFlagSet("replay-live", flag.ContinueOnError)
	eventID := flags.String("event-id", "", "event ID in the active dead-letter queue")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*eventID) == "" {
		return fmt.Errorf("provide -event-id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	if err := service.Replay(ctx, *eventID); err != nil {
		return err
	}
	return writeJSON(map[string]string{"eventId": *eventID, "state": "requeued"})
}

func runDLQ() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	service, err := openDurable(ctx)
	if err != nil {
		return err
	}
	defer service.Close()
	items, err := service.DeadLetters(ctx)
	if err != nil {
		return err
	}
	return writeJSON(items)
}

func openDurable(ctx context.Context) (*durable.Service, error) {
	return durable.Open(ctx, durable.Config{
		DatabaseURL: env("DATABASE_URL", "postgres://pipeline:pipeline@localhost:5432/pipeline?sslmode=disable"),
		Brokers:     strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ","),
		Topic:       env("KAFKA_TOPIC", durable.DefaultTopic),
		Consumer:    env("KAFKA_CONSUMER", durable.DefaultConsumerName),
	})
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func runDemo() error {
	system := pipeline.New()
	order, _, err := system.Submit("customer-42", "checkout-1042", 2599)
	if err != nil {
		return err
	}
	eventID := "evt-" + order.ID
	if _, err := system.PublishNext(order.Key, pipeline.FaultBrokerAfter); err == nil {
		return fmt.Errorf("expected simulated lost publish acknowledgement")
	}
	if _, err := system.PublishNext(order.Key, pipeline.FaultNone); err != nil {
		return err
	}
	if _, _, err := system.ConsumeNext(order.Key, pipeline.FaultConsumerAfter); err == nil {
		return fmt.Errorf("expected simulated lost consumer acknowledgement")
	}
	for {
		_, duplicate, consumeErr := system.ConsumeNext(order.Key, pipeline.FaultNone)
		if consumeErr == pipeline.ErrNoWork {
			break
		}
		if consumeErr != nil {
			return consumeErr
		}
		if !duplicate {
			return fmt.Errorf("redelivered event %s was not deduplicated", eventID)
		}
	}
	if err := system.CheckInvariants(); err != nil {
		return err
	}
	return writeJSON(system.Snapshot())
}

func runChaos(args []string) error {
	flags := flag.NewFlagSet("chaos", flag.ContinueOnError)
	runs := flags.Int("runs", 20, "number of independently fault-injected runs")
	seed := flags.Int64("seed", 42, "deterministic random seed")
	if err := flags.Parse(args); err != nil {
		return err
	}
	report, err := pipeline.RunChaos(*runs, *seed)
	if err != nil {
		return err
	}
	return writeJSON(report)
}

func runReplay(args []string) error {
	flags := flag.NewFlagSet("replay", flag.ContinueOnError)
	key := flags.String("key", "customer-replay", "order partition key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	system := pipeline.New()
	order, _, err := system.Submit(*key, "replay-demo-1", 5000)
	if err != nil {
		return err
	}
	eventID, err := system.PublishNext(*key, pipeline.FaultNone)
	if err != nil {
		return err
	}
	if _, err := system.QuarantineNext(*key, "operator requested replay demonstration"); err != nil {
		return err
	}
	if err := system.Replay(eventID); err != nil {
		return err
	}
	if _, err := system.PublishNext(*key, pipeline.FaultNone); err != nil {
		return err
	}
	if _, _, err := system.ConsumeNext(*key, pipeline.FaultNone); err != nil {
		return err
	}
	if err := system.CheckInvariants(); err != nil {
		return err
	}
	return writeJSON(struct {
		ReplayedOrder string            `json:"replayedOrder"`
		Snapshot      pipeline.Snapshot `json:"snapshot"`
	}{ReplayedOrder: order.ID, Snapshot: system.Snapshot()})
}

func writeJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pipeline <demo|chaos|replay|replay-live|dlq|serve-api|relay|consume|submit> [flags]")
}
