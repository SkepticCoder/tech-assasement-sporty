# Bet Settlement Trigger Service

A Spring Boot backend service that simulates sports betting event outcome handling and bet settlement via **Apache Kafka** and **Apache RocketMQ**, with **Redis** as the in-memory data store.

## Tech Stack

- Java 21, Spring Boot 3.4
- Apache Kafka (event streaming + Dead-Letter Queue)
- Apache RocketMQ (settlement messaging)
- Redis (in-memory bet storage + distributed cache)
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

## Settlement Strategies

The service supports **two complementary settlement approaches**:

### 1. Redis Strategy (Cache-Driven)
- Uses Redis as a distributed cache and state store
- Traditional Kafka listener with Redis deduplication
- Direct synchronous settlement processing
- Best for: Simple, low-latency requirements

**Enable:**
```bash
export SETTLEMENT_STRATEGY=REDIS
export SETTLEMENT_REDIS_ENABLED=true
export SETTLEMENT_KAFKA_STREAMS_ENABLED=false
```

### 2. Kafka Streams Strategy (Stream-Processing) — Default
- Declarative stream processing via Kafka Streams topology
- Built-in fault tolerance and exactly-once semantics
- Scalable horizontal processing
- Best for: Enterprise, high-volume scenarios

**Enable (default):**
```bash
export SETTLEMENT_STRATEGY=KAFKA_STREAMS
export SETTLEMENT_KAFKA_STREAMS_ENABLED=true
```

### Run Both Strategies Simultaneously
```bash
export SETTLEMENT_REDIS_ENABLED=true
export SETTLEMENT_KAFKA_STREAMS_ENABLED=true
```

For detailed strategy comparison and configuration, see [SETTLEMENT_STRATEGIES.md](SETTLEMENT_STRATEGIES.md) and [DUAL_STRATEGY_IMPLEMENTATION.md](DUAL_STRATEGY_IMPLEMENTATION.md).

## Dead-Letter Queue (DLQ)

Failed event outcomes are automatically routed to Dead-Letter Queue topics for monitoring, debugging, and manual replay.

### DLQ Topics

| Topic | Purpose | Consumer Group |
|-------|---------|---|
| `event-outcomes-dlq` | All failures from both Redis and Kafka Streams strategies | `bet-settlement-dlq-monitor` |

### DLQ Configuration

```yaml
dlq:
  enabled: true
  max-retries: 3  # Redis strategy: retries before DLQ
  monitoring:
    enabled: true
    alert-enabled: false  # Enable for production alerts
```

**Environment Variables:**
```bash
export DLQ_ENABLED=true
export DLQ_MAX_RETRIES=3
export DLQ_MONITORING_ENABLED=true
export DLQ_ALERT_ENABLED=false
```

### Monitor DLQ Messages

**View all DLQ messages:**
```bash
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning
```


### Check DLQ Consumer Lag

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group bet-settlement-dlq-monitor \
  --describe
```

### Replay Failed Events from DLQ

```bash
# Copy DLQ messages back to event-outcomes topic
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning | \
kafka-console-producer.sh --topic event-outcomes \
  --bootstrap-server localhost:9092
```

### What's Captured in DLQ

Each failed event in DLQ contains:
- Original EventOutcome (eventId, name, winner)
- Exception message and full stack trace
- Failure timestamp
- Retry count
- Settlement strategy (REDIS/KAFKA_STREAMS)
- Failure reason

For comprehensive DLQ documentation, see [DEAD_LETTER_QUEUE.md](DEAD_LETTER_QUEUE.md).

## Bet Status State Machine

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
│   ├── kafka/
│   │   ├── dlq/            # Dead-Letter Queue producer & consumer
│   │   └── streams/        # Kafka Streams topology
│   ├── redis/              # Redis-based settlement processor
│   └── rocketmq/           # RocketMQ producer (real + mock) & consumer
└── persistence/            # Redis repository & data seeder
```

## Troubleshooting DLQ

### DLQ Topics Don't Exist

**Error:** Events not reaching DLQ, topic not found
```bash
# Create DLQ topic
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic event-outcomes-dlq \
  --partitions 3 --replication-factor 1
```

### DLQ Consumer Lag Growing

**Check consumer group status:**
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group bet-settlement-dlq-monitor \
  --describe
```

**Restart service if stuck:**
```bash
docker compose restart bet-settlement-service
# Or if running locally:
pkill -f bet-settlement-service
./gradlew bootRun
```

### Test DLQ with Simulated Failures

```bash
# Disable RocketMQ to cause settlement failures
export ROCKETMQ_ENABLED=false
./gradlew bootRun

# Send event (will fail and go to DLQ)
curl -X POST http://localhost:8080/api/v1/event-outcomes \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "event-fail-test",
    "eventName": "Test Event",
    "eventWinnerId": "team-a"
  }'

# Monitor DLQ messages
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092
```


## Architecture

See [docs/architecture/ARCHITECTURE.md](docs/architecture/ARCHITECTURE.md) for C4 diagrams and detailed design decisions.
