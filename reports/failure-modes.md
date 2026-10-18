# Failure-mode analysis

The pipeline has two atomicity boundaries: producer order+outbox commit, and consumer dedup+business-effect commit. Broker delivery is deliberately treated as at-least-once. Exactly-once describes the unique committed business effect, not exactly one network delivery.

| Fault | Expected behavior | Invariant |
|---|---|---|
| Same client request is retried | Return the prior order for the same key and payload; reject key reuse with a different payload | One order and one outbox row per idempotency key |
| Publisher stops before broker acceptance | Keep the outbox event pending | Accepted order remains recoverable |
| Publisher acknowledgement is lost after acceptance | Retrying can enqueue a duplicate delivery | One consumer business effect per event |
| Consumer stops before transaction commit | Leave delivery available; commit neither dedup row nor effect | Event is safely retryable |
| Consumer stops after transaction commit | Redeliver; the consumer finds the dedup record and acknowledges without reapplying | No duplicate business effect |
| Network partition between services | Stop publishing/consuming while preserving local state | No acknowledged loss; recovery resumes from pending/queued state |
| Poison payload | Quarantine in DLQ, alert/operator review, do not advance that key | Later sequence for that key cannot overtake it |
| DLQ event replay | Requeue through the outbox after remediation | Original sequence gate remains in force |

## Transaction contract

The producer commits the order and outbox event in one PostgreSQL transaction.
The ordered relay publishes at least once using the partition key. The consumer
inserts `(consumer_name, event_id)` into `processed_events`, verifies the next
partition sequence, and applies the business effect in one transaction. A
conflict on that exact dedup key means the effect already committed and the
delivery is safe to acknowledge. A different unique-constraint failure is an
integrity error and must not be treated as a duplicate.

`../sql/schema.sql` records the relational constraints. `internal/durable`
implements the PostgreSQL + Kafka-compatible runtime used by Docker Compose.
The separate Go reference model and browser lab exercise state transitions in
memory and do not pretend to provide database durability or broker semantics.
