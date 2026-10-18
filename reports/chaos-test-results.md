# Chaos-test results

Reproduce the invariant suite with:

```bash
go test -race ./...
go run ./cmd/pipeline chaos -runs 20 -seed 42
```

The harness creates 12 orders across three partition keys per run and injects deterministic random failures at broker acceptance, broker acknowledgement, consumer commit, consumer acknowledgement, and network connectivity. It checks the model's outbox coverage, dedup/effect correspondence, and per-key ordering after each round.

| Measure | Target | How it is measured |
|---|---:|---|
| Independent chaos runs | 20 | `RunChaos(20, 42)` |
| Accepted orders | 240 | 12 per run |
| Lost accepted orders | 0 | Each run must apply all 12 before completion |
| Duplicate business effects | 0 | Unique applied event IDs and dedup/effect invariant |
| Per-key ordering violations | 0 | Applied sequence must equal 1..N for each key |
| Faults injected | 84 | Seed `42`; broker, consumer, and network failures |
| Safe duplicate deliveries suppressed | 20 | Ambiguous acknowledgements exercise consumer deduplication |

This report describes a deterministic, in-memory reference model. It is not a benchmark and does not validate a live broker, database, network, or Kubernetes deployment.
