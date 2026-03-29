# Bet Settlement Trigger Service

A Spring Boot backend service that simulates sports betting event outcome handling and bet settlement via **Apache Kafka** and **Apache RocketMQ**, with **Redis** as the in-memory data store.

## Tech Stack

- Java 21, Spring Boot 3.4
- Apache Kafka (event streaming)
- Apache RocketMQ (settlement messaging)
- Redis (in-memory bet storage)
- Gradle 9.4.1
- Docker Compose (local infrastructure)
- Testcontainers, JUnit 5, K6

## Prerequisites

- Java 21+
- Docker & Docker Compose
- Gradle 9.4.1+ (or use the wrapper)

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
```

This starts Redis, Kafka (KRaft mode), RocketMQ NameServer, and RocketMQ Broker.

### 2. Build the application

```bash
./gradlew build
```

### 3. Run the application

**With RocketMQ (full flow):**
```bash
./gradlew bootRun
```

**Without RocketMQ (mock mode — logs settlement payload and settles directly):**
```bash
ROCKETMQ_ENABLED=false ./gradlew bootRun --args='--spring.profiles.active=local'
```

### 4. Publish an event outcome

```bash
curl -X POST http://localhost:8080/api/v1/event-outcomes \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "event-100",
    "eventName": "Champions League Final",
    "eventWinnerId": "team-a"
  }'
```

Response: `202 Accepted`

```json
{
  "eventId": "event-100",
  "status": "ACCEPTED",
  "message": "Event outcome published for settlement processing"
}
```

### 5. API Documentation

Swagger UI: [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)

OpenAPI spec: [http://localhost:8080/api-docs](http://localhost:8080/api-docs)

## How It Works

```
POST /api/v1/event-outcomes
        │
        ▼
   Kafka Producer ──▶ event-outcomes topic
                          │
                          ▼
                    Kafka Consumer
                          │
                          ▼
                   BetMatchingService
                   (Redis: PENDING → SETTLING via WATCH/MULTI/EXEC CAS)
                          │
                          ▼
                  RocketMQ Producer ──▶ bet-settlements topic
                                            │
                                            ▼
                                     RocketMQ Consumer
                                            │
                                            ▼
                                    BetSettlementService
                                    (SETTLING → WON/LOST)
```

### Exactly-Once Semantics

| Stage | Mechanism |
|-------|-----------|
| Kafka Producer | Idempotent + transactional writes |
| Kafka Consumer | `read_committed` isolation, manual offset commit |
| Bet Matching | Redis `WATCH`/`MULTI`/`EXEC` CAS: `PENDING → SETTLING` |
| RocketMQ Consumer | Idempotent: status guard only processes `SETTLING` bets |

### Why Keying Alone Isn't Enough

Kafka partitions by `eventId` and RocketMQ can route by `betId`, ensuring the same event/bet is handled by a single consumer under normal conditions. However, message keying alone does not guarantee exactly-once processing:

1. **Consumer rebalancing** — If a consumer crashes mid-processing and its partition is reassigned, the new consumer re-processes the same event outcome. Two instances then race on the same bets.
2. **Producer retries / duplicate messages** — Network hiccups can cause the broker to receive the same message twice, even with idempotent producers (e.g., across producer restarts).
3. **Multiple consumer groups or service restarts** — Any scenario where more than one thread processes the same event at overlapping times breaks the single-writer assumption that keying provides.

This is why the Redis CAS (`WATCH`/`MULTI`/`EXEC`) on the `PENDING → SETTLING` transition is the true correctness guarantee. Keying **reduces contention**; CAS **prevents double-settlement**.

### Bet Status State Machine

```
PENDING ──(CAS)──▶ SETTLING ──(settle)──▶ WON
                                     └──▶ LOST
```

## Seeded Test Data

On startup, 4 bets are seeded into Redis:

| User | Event | Predicted Winner | Amount |
|------|-------|-----------------|--------|
| user-1 | event-100 | team-a | $50 |
| user-2 | event-100 | team-b | $100 |
| user-3 | event-200 | team-c | $75 |
| user-4 | event-200 | team-d | $200 |

## Testing

### Unit tests
```bash
./gradlew test --tests "com.sporty.betting.unit.*"
```

### Integration tests (requires Docker)
```bash
./gradlew test --tests "com.sporty.betting.integration.*"
```

### Acceptance tests (requires Docker)
```bash
./gradlew test --tests "com.sporty.betting.acceptance.*"
```

### All tests
```bash
./gradlew test
```

### Load tests (requires K6)
```bash
# Install K6: brew install k6
k6 run k6/load-test.js
```

## Project Structure

```
src/main/java/com/sporty/betting/
├── api/                    # REST controllers & DTOs
├── config/                 # Kafka, Redis, OpenAPI configuration
├── domain/
│   ├── model/              # Bet, BetStatus, EventOutcome, BetSettlement
│   └── service/            # Business logic
├── messaging/
│   ├── kafka/              # Kafka producer & consumer
│   └── rocketmq/           # RocketMQ producer (real + mock) & consumer
└── persistence/            # Redis repository & data seeder
```

## Architecture

See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for C4 diagrams and detailed design decisions.
