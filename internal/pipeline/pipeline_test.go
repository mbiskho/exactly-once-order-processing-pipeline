package pipeline

import (
	"errors"
	"sync"
	"testing"
)

func TestOutboxAndIdempotentSubmission(t *testing.T) {
	system := New()
	first, created, err := system.Submit("customer-a", "checkout-1", 1299)
	if err != nil || !created {
		t.Fatalf("Submit() = (%v, %v), want created order without error", first, created)
	}
	retry, created, err := system.Submit("customer-a", "checkout-1", 1299)
	if err != nil || created || retry.ID != first.ID {
		t.Fatalf("idempotent retry = (%v, %v, %v)", retry, created, err)
	}
	if got := system.Snapshot().Stats.OutboxPending; got != 1 {
		t.Fatalf("outbox rows = %d, want 1", got)
	}
	if _, _, err := system.Submit("customer-a", "checkout-1", 2000); !errors.Is(err, ErrIdempotency) {
		t.Fatalf("payload mismatch error = %v, want ErrIdempotency", err)
	}
}

func TestAmbiguousPublishAndConsumerCrashDoNotDuplicateEffect(t *testing.T) {
	system := New()
	_, _, _ = system.Submit("customer-a", "checkout-1", 1299)

	if _, err := system.PublishNext("customer-a", FaultBrokerAfter); err == nil {
		t.Fatal("publish acknowledgement loss should be reported")
	}
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatalf("retry publish: %v", err)
	}
	if _, _, err := system.ConsumeNext("customer-a", FaultConsumerAfter); err == nil {
		t.Fatal("consumer acknowledgement loss should be reported")
	}
	if _, duplicate, err := system.ConsumeNext("customer-a", FaultNone); err != nil || !duplicate {
		t.Fatalf("redelivery = (duplicate %v, error %v), want deduplicated success", duplicate, err)
	}
	if _, duplicate, err := system.ConsumeNext("customer-a", FaultNone); err != nil || !duplicate {
		t.Fatalf("second duplicate delivery = (duplicate %v, error %v), want deduplicated success", duplicate, err)
	}
	snapshot := system.Snapshot()
	if snapshot.Stats.Applied != 1 || snapshot.Stats.DuplicateDeliveries != 2 {
		t.Fatalf("stats = %+v, want one effect and two suppressed duplicates", snapshot.Stats)
	}
	if err := system.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestPerKeyOrderingBlocksLaterEventsUntilPriorEventCommits(t *testing.T) {
	system := New()
	_, _, _ = system.Submit("customer-a", "checkout-1", 100)
	_, _, _ = system.Submit("customer-a", "checkout-2", 200)
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	if _, err := system.PublishNext("customer-a", FaultNone); !errors.Is(err, ErrNoWork) {
		t.Fatalf("second publish error = %v, want ErrNoWork while sequence 1 is in flight", err)
	}
	if _, _, err := system.ConsumeNext("customer-a", FaultConsumerBefore); err == nil {
		t.Fatal("consumer crash should be returned")
	}
	if _, duplicate, err := system.ConsumeNext("customer-a", FaultNone); err != nil || duplicate {
		t.Fatalf("first event consume = (%v, %v)", duplicate, err)
	}
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatalf("publish next sequence after commit: %v", err)
	}
	if _, duplicate, err := system.ConsumeNext("customer-a", FaultNone); err != nil || duplicate {
		t.Fatalf("second event consume = (%v, %v)", duplicate, err)
	}
	if err := system.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterCanBeReplayed(t *testing.T) {
	system := New()
	_, _, _ = system.Submit("customer-a", "checkout-1", 100)
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	id, err := system.QuarantineNext("customer-a", "invalid order payload")
	if err != nil {
		t.Fatal(err)
	}
	if err := system.Replay(id); err != nil {
		t.Fatal(err)
	}
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	if _, _, err := system.ConsumeNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	if snapshot := system.Snapshot(); snapshot.Stats.Applied != 1 || snapshot.Stats.DeadLettered != 0 {
		t.Fatalf("replayed stats = %+v", snapshot.Stats)
	}
}

func TestConcurrentRetriesCreateOneOrderAndOutboxEvent(t *testing.T) {
	system := New()
	const attempts = 30
	var wait sync.WaitGroup
	errorsFound := make(chan error, attempts)
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, _, err := system.Submit("customer-a", "same-checkout", 100)
			if err != nil {
				errorsFound <- err
			}
		}()
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
	snapshot := system.Snapshot()
	if len(snapshot.Orders) != 1 || len(snapshot.Events) != 1 || snapshot.Stats.DuplicateRequests != attempts-1 {
		t.Fatalf("concurrent retry state = %+v", snapshot.Stats)
	}
}

func TestNetworkPartitionPausesPublishingAndConsumption(t *testing.T) {
	system := New()
	_, _, _ = system.Submit("customer-a", "checkout-1", 100)
	system.SetNetworkBlocked(true)
	if _, err := system.PublishNext("customer-a", FaultNone); !errors.Is(err, ErrNetworkBlocked) {
		t.Fatalf("publish while partitioned = %v, want ErrNetworkBlocked", err)
	}
	system.SetNetworkBlocked(false)
	if _, err := system.PublishNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	system.SetNetworkBlocked(true)
	if _, _, err := system.ConsumeNext("customer-a", FaultNone); !errors.Is(err, ErrNetworkBlocked) {
		t.Fatalf("consume while partitioned = %v, want ErrNetworkBlocked", err)
	}
	system.SetNetworkBlocked(false)
	if _, _, err := system.ConsumeNext("customer-a", FaultNone); err != nil {
		t.Fatal(err)
	}
	if err := system.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestTwentyChaosRunsPreserveInvariants(t *testing.T) {
	report, err := RunChaos(20, 42)
	if err != nil {
		t.Fatal(err)
	}
	if report.Orders != 240 || report.Lost != 0 || report.Duplicates != 0 ||
		report.OrderingViolations != 0 || report.FaultsInjected == 0 || report.RedeliveriesSuppressed == 0 {
		t.Fatalf("chaos report = %+v", report)
	}
}
