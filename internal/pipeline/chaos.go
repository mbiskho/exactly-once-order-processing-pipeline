package pipeline

import (
	"fmt"
	"math/rand"
)

type ChaosReport struct {
	Runs                   int `json:"runs"`
	Orders                 int `json:"orders"`
	Lost                   int `json:"lost"`
	Duplicates             int `json:"duplicates"`
	OrderingViolations     int `json:"orderingViolations"`
	RedeliveriesSuppressed int `json:"redeliveriesSuppressed"`
	FaultsInjected         int `json:"faultsInjected"`
}

func RunChaos(runs int, seed int64) (ChaosReport, error) {
	if runs < 1 {
		return ChaosReport{}, fmt.Errorf("runs must be positive")
	}
	random := rand.New(rand.NewSource(seed))
	report := ChaosReport{Runs: runs}
	for run := 0; run < runs; run++ {
		system := New()
		for i := 0; i < 12; i++ {
			key := fmt.Sprintf("account-%d", i%3)
			_, _, err := system.Submit(key, fmt.Sprintf("run-%d-order-%d", run, i), int64(100+i))
			if err != nil {
				return report, err
			}
		}

		for progress := 0; progress < 2000 && system.Snapshot().Stats.Applied < 12; progress++ {
			snapshot := system.Snapshot()
			for _, key := range []string{"account-0", "account-1", "account-2"} {
				if random.Intn(35) == 0 {
					system.SetNetworkBlocked(true)
					_, err := system.PublishNext(key, FaultNone)
					if err != ErrNetworkBlocked {
						return report, fmt.Errorf("run %d network partition was not enforced: %v", run+1, err)
					}
					system.SetNetworkBlocked(false)
					report.FaultsInjected++
				}

				state := firstUnappliedState(snapshot, key)
				if state == StatePending {
					fault := FaultNone
					if random.Intn(8) == 0 {
						if random.Intn(2) == 0 {
							fault = FaultBrokerBefore
						} else {
							fault = FaultBrokerAfter
						}
						report.FaultsInjected++
					}
					_, err := system.PublishNext(key, fault)
					if err == nil || fault == FaultBrokerAfter {
						continue
					}
					if fault == FaultBrokerBefore {
						continue
					}
					if err != nil && err != ErrNoWork {
						return report, err
					}
				}
				if state == StatePublished {
					fault := FaultNone
					if random.Intn(8) == 0 {
						if random.Intn(2) == 0 {
							fault = FaultConsumerBefore
						} else {
							fault = FaultConsumerAfter
						}
						report.FaultsInjected++
					}
					_, _, err := system.ConsumeNext(key, fault)
					if err != nil && err != ErrNoWork && fault != FaultConsumerBefore && fault != FaultConsumerAfter {
						return report, err
					}
				}
			}
			if err := system.CheckInvariants(); err != nil {
				return report, fmt.Errorf("run %d invariant check: %w", run+1, err)
			}
		}

		snapshot := system.Snapshot()
		if snapshot.Stats.Applied != 12 {
			return report, fmt.Errorf("run %d stalled after applying %d/12 orders", run+1, snapshot.Stats.Applied)
		}
		if err := system.CheckInvariants(); err != nil {
			return report, fmt.Errorf("run %d invariant check: %w", run+1, err)
		}
		lost, duplicates, orderingViolations := countInvariantFailures(snapshot)
		report.Lost += lost
		report.Duplicates += duplicates
		report.OrderingViolations += orderingViolations
		report.Orders += snapshot.Stats.Submitted
		report.RedeliveriesSuppressed += snapshot.Stats.DuplicateDeliveries
	}
	if report.Lost != 0 || report.Duplicates != 0 || report.OrderingViolations != 0 {
		return report, fmt.Errorf(
			"chaos invariants failed: lost=%d duplicates=%d ordering=%d",
			report.Lost, report.Duplicates, report.OrderingViolations,
		)
	}
	return report, nil
}

func countInvariantFailures(snapshot Snapshot) (lost, duplicates, orderingViolations int) {
	lost = snapshot.Stats.Submitted - snapshot.Stats.Applied
	events := make(map[string]Event, len(snapshot.Events))
	for _, event := range snapshot.Events {
		events[event.ID] = event
	}
	seen := make(map[string]bool, len(snapshot.Applied))
	nextSequence := make(map[string]int)
	for _, eventID := range snapshot.Applied {
		if seen[eventID] {
			duplicates++
			continue
		}
		seen[eventID] = true
		event, ok := events[eventID]
		if !ok {
			lost++
			continue
		}
		nextSequence[event.Order.Key]++
		if event.Order.Sequence != nextSequence[event.Order.Key] {
			orderingViolations++
		}
	}
	return lost, duplicates, orderingViolations
}

func firstUnappliedState(snapshot Snapshot, key string) State {
	for _, event := range snapshot.Events {
		if event.Order.Key == key && event.State != StateApplied {
			return event.State
		}
	}
	return ""
}
