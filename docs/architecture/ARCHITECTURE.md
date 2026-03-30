# Bet Settlement Trigger Service - Architecture

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 21 (Spring Native Image) |
| Framework | Spring Boot 3.x |
| Build | Gradle 9.4.1 |
| Messaging | Apache Kafka, Apache RocketMQ |
| Database | Redis (in-memory) |
| API Docs | OpenAPI 3.0 (SpringDoc) |
| Local Infra | Docker Compose |
| Prod Infra | Kubernetes |
| CI/CD | GitHub Actions |
| Testing | JUnit 5, Testcontainers, Spring Integration Tests, K6 |

---

## C4 Model

All diagrams are defined in [`c4-diagrams.puml`](c4-diagrams.puml) (PlantUML + C4-PlantUML stdlib).

### Level 1 - System Context

```
┌──────────┐         ┌─────────────────────────────────────┐
│ Operator │───REST──▶│  Bet Settlement Trigger Service     │
└──────────┘         │  (Java 21 / Spring Boot)            │
                     └──────┬──────────────┬───────────────┘
                            │              │
                     ┌──────▼──────┐ ┌─────▼───────┐
                     │ Apache Kafka│ │Apache RocketMQ│
                     │ (event-     │ │ (bet-        │
                     │  outcomes)  │ │  settlements)│
                     └─────────────┘ └──────────────┘
```

The service is a single deployable unit. An **Operator** publishes event outcomes via REST API. The service uses **Kafka** for event outcome streaming and **RocketMQ** for bet settlement command dispatch.

---

### Level 2 - Container Diagram

```
                          ┌─────────────────────────────────────────────────────────────┐
                          │              Bet Settlement Trigger Service                  │
                          │                                                             │
 ┌──────────┐   REST      │  ┌──────────┐    ┌───────────────┐    ┌─────────────────┐  │
 │ Operator │────────────▶│  │ REST API │───▶│ Kafka Producer│───▶│  Apache Kafka   │  │
 └──────────┘             │  └──────────┘    └───────────────┘    │ (event-outcomes)│  │
                          │                                       └────────┬────────┘  │
                          │                                                │           │
                          │                                       ┌────────▼────────┐  │
                          │                                       │ Kafka Consumer  │  │
                          │                                       └────────┬────────┘  │
                          │                                                │           │
                          │  ┌────────┐    ┌──────────────┐       ┌────────▼────────┐  │
                          │  │ Redis  │◀───│ Bet Matcher  │◀──────│ Bet Matching    │  │
                          │  │(in-mem)│    │  Service     │       │ Service         │  │
                          │  └───┬────┘    └──────┬───────┘       └─────────────────┘  │
                          │      │                │                                    │
                          │      │         ┌──────▼───────────┐                        │
                          │      │         │ RocketMQ Producer│                        │
                          │      │         └──────┬───────────┘                        │
                          │      │                │                                    │
                          │      │         ┌──────▼───────────┐                        │
                          │      │         │ Apache RocketMQ  │                        │
                          │      │         │ (bet-settlements)│                        │
                          │      │         └──────┬───────────┘                        │
                          │      │                │                                    │
                          │      │         ┌──────▼───────────┐                        │
                          │      │◀────────│ RocketMQ Consumer│                        │
                          │      │ settle  │ (Settlement Svc) │                        │
                          │      │         └──────────────────┘                        │
                          └─────────────────────────────────────────────────────────────┘
```

---

### Level 3 - Component Diagram

#### API Layer
- **EventOutcomeController** - `@RestController`, exposes `POST /api/v1/event-outcomes`
- **OpenAPI Config** - SpringDoc auto-generates Swagger UI at `/swagger-ui.html`

#### Messaging Layer
- **EventOutcomeKafkaProducer** - Publishes `EventOutcome` to Kafka `event-outcomes` topic
- **EventOutcomeKafkaConsumer** - `@KafkaListener` on `event-outcomes`, delegates to `BetMatchingService`
- **BetSettlementRocketProducer** - Publishes `BetSettlement` to RocketMQ `bet-settlements` topic
- **BetSettlementRocketConsumer** - `@RocketMQMessageListener` on `bet-settlements`, delegates to `BetSettlementService`

#### Domain / Service Layer
- **EventOutcomeService** - Orchestrates event outcome publishing flow
- **BetMatchingService** - Queries bets by `eventId` + `PENDING` status, creates settlement commands
- **BetSettlementService** - Settles bets: compares `eventWinnerId`, marks bet as `WON` or `LOST`

#### Persistence Layer
- **BetRepository** - Spring Data Redis repository backed by `RedisTemplate` / `HashOperations`
- **Bet Entity** - `@RedisHash("bet")`: `betId`, `userId`, `eventId`, `eventMarketId`, `eventWinnerId`, `betAmount`, `status`
- **Secondary Index** - Redis secondary index on `eventId` + `status` for efficient bet lookup via `@Indexed`

---

### Level 4 - Dynamic / Sequence Flow

```
Operator                 API              Kafka           BetMatcher       RocketMQ         Settlement
   │                      │                 │                │                │                │
   │ POST event-outcome   │                 │                │                │                │
   │─────────────────────▶│                 │                │                │                │
   │                      │ produce         │                │                │                │
   │                      │────────────────▶│                │                │                │
   │     202 Accepted     │                 │                │                │                │
   │◀─────────────────────│                 │                │                │                │
   │                      │                 │ consume        │                │                │
   │                      │                 │───────────────▶│                │                │
   │                      │                 │                │ query bets     │                │
   │                      │                 │                │───▶ DB         │                │
   │                      │                 │                │◀─── List<Bet>  │                │
   │                      │                 │                │                │                │
   │                      │                 │                │ produce per bet│                │
   │                      │                 │                │───────────────▶│                │
   │                      │                 │                │                │ consume        │
   │                      │                 │                │                │───────────────▶│
   │                      │                 │                │                │                │ settle
   │                      │                 │                │                │                │──▶ DB
```

---

## Data Model

### Bet Entity

| Field | Type | Description |
|-------|------|-------------|
| `id` | `UUID` | Primary key (Bet ID) |
| `userId` | `String` | User who placed the bet |
| `eventId` | `String` | Event the bet is placed on |
| `eventMarketId` | `String` | Market within the event |
| `eventWinnerId` | `String` | Predicted winner by the user |
| `betAmount` | `BigDecimal` | Wager amount |
| `status` | `Enum` | `PENDING` → `SETTLING` → `WON` / `LOST` |
| `createdAt` | `Instant` | When the bet was placed |
| `settledAt` | `Instant` | When the bet was settled (nullable) |

### EventOutcome (Kafka message)

| Field | Type |
|-------|------|
| `eventId` | `String` |
| `eventName` | `String` |
| `eventWinnerId` | `String` |

### BetSettlement (RocketMQ message)

| Field | Type |
|-------|------|
| `betId` | `UUID` |
| `userId` | `String` |
| `eventId` | `String` |
| `eventWinnerId` | `String` |
| `predictedWinnerId` | `String` |
| `betAmount` | `BigDecimal` |

---

## Project Structure

```
bet-settlement-service/
├── build.gradle.kts
├── settings.gradle.kts
├── docker-compose.yml                    # Kafka + RocketMQ + Redis
├── k8s/                                  # Kubernetes manifests
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── .github/
│   └── workflows/
│       ├── ci.yml                        # Build, test, native-image
│       └── cd.yml                        # Deploy
├── src/
│   ├── main/
│   │   ├── java/com/sporty/betting/
│   │   │   ├── BetSettlementApplication.java
│   │   │   ├── api/
│   │   │   │   ├── EventOutcomeController.java
│   │   │   │   └── dto/
│   │   │   │       ├── EventOutcomeRequest.java
│   │   │   │       └── EventOutcomeResponse.java
│   │   │   ├── config/
│   │   │   │   ├── KafkaConfig.java
│   │   │   │   ├── RocketMQConfig.java
│   │   │   │   ├── RedisConfig.java
│   │   │   │   └── OpenApiConfig.java
│   │   │   ├── domain/
│   │   │   │   ├── model/
│   │   │   │   │   ├── Bet.java
│   │   │   │   │   ├── BetStatus.java
│   │   │   │   │   └── EventOutcome.java
│   │   │   │   └── service/
│   │   │   │       ├── EventOutcomeService.java
│   │   │   │       ├── BetMatchingService.java
│   │   │   │       └── BetSettlementService.java
│   │   │   ├── messaging/
│   │   │   │   ├── kafka/
│   │   │   │   │   ├── EventOutcomeKafkaProducer.java
│   │   │   │   │   └── EventOutcomeKafkaConsumer.java
│   │   │   │   └── rocketmq/
│   │   │   │       ├── BetSettlementRocketProducer.java
│   │   │   │       └── BetSettlementRocketConsumer.java
│   │   │   └── persistence/
│   │   │       ├── BetRepository.java
│   │   │       └── BetDataSeeder.java    # Seed bets on startup
│   │   └── resources/
│   │       ├── application.yml
│   │       └── application-local.yml
│   └── test/
│       ├── java/com/sporty/betting/
│       │   ├── unit/
│       │   │   ├── BetMatchingServiceTest.java
│       │   │   └── BetSettlementServiceTest.java
│       │   ├── integration/
│       │   │   ├── KafkaIntegrationTest.java
│       │   │   ├── RocketMQIntegrationTest.java
│       │   │   └── EndToEndSettlementTest.java
│       │   └── acceptance/
│       │       └── BetSettlementAcceptanceTest.java
│       └── resources/
│           └── application-test.yml
├── k6/
│   └── load-test.js                      # High-load test script
└── docs/
    └── architecture/
        ├── ARCHITECTURE.md
        └── c4-diagrams.puml
```

---

## Infrastructure

### Local Development (Docker Compose)

Services:
- **Kafka** (KRaft mode, no Zookeeper) - port 9092
- **RocketMQ NameServer** - port 9876
- **RocketMQ Broker** - port 10911
- **Redis** - port 6379
- **Application** - port 8080

### Production (Kubernetes)

- Spring Native Image for fast startup and low memory footprint
- Horizontal Pod Autoscaler based on consumer lag metrics
- ConfigMap for externalized configuration
- Health checks via Spring Actuator (`/actuator/health`)

### CI/CD (GitHub Actions)

Pipeline stages:
1. **Build** - Gradle build + unit tests
2. **Integration Test** - Testcontainers (Kafka + RocketMQ)
3. **Native Image** - GraalVM native-image compilation
4. **Load Test** - K6 against ephemeral environment
5. **Deploy** - Push image to registry, kubectl apply

---

## Testing Strategy

| Type | Framework | Scope |
|------|-----------|-------|
| Unit | JUnit 5 + Mockito | Service logic (matching, settlement) |
| Integration | Testcontainers + Spring Boot Test | Kafka/RocketMQ producers & consumers |
| Acceptance | Spring Integration Test | Full flow: API → Kafka → Match → RocketMQ → Settle |
| Load | K6 | Throughput & latency under high concurrency |

---

## Key Design Decisions

1. **Single deployable service** - All components (API, consumers, producers) in one Spring Boot app. Simplifies deployment for an assessment while the architecture supports future extraction into microservices.

2. **Async flow with 202 Accepted** - The API returns immediately after publishing to Kafka. Settlement happens asynchronously through the message pipeline.

3. **Redis as in-memory store** - Uses Spring Data Redis with `@RedisHash` and `@Indexed` for the Bet entity. Secondary index on `eventId` enables fast lookup. Redis provides sub-millisecond reads for bet matching under high throughput.

4. **Exactly-once semantics (end-to-end)** - Achieved through a combination of mechanisms at each stage of the pipeline:

   | Stage | Mechanism | Detail |
   |-------|-----------|--------|
   | **Kafka Producer** | Idempotent producer + transactional writes | `enable.idempotence=true`, `transactional.id` configured. Prevents duplicate publishes on retries. |
   | **Kafka Consumer** | `read_committed` isolation + manual offset commit | Consumer only reads committed messages. Offsets are committed after successful processing, inside a Kafka transaction. |
   | **Kafka → Redis (Bet Matching)** | Atomic CAS on bet status via Redis `WATCH`/`MULTI`/`EXEC` | Before sending to RocketMQ, atomically transition bet status from `PENDING` → `SETTLING` using Redis optimistic locking. If another consumer already claimed the bet, the transaction aborts — no duplicate settlement message is produced. |
   | **RocketMQ Producer** | Transactional message | Uses RocketMQ transaction message (`TransactionMQProducer`). The local transaction (Redis status update) and RocketMQ send are coordinated — message is only committed if local state succeeds. |
   | **RocketMQ Consumer** | Idempotent consumer with dedup key | Settlement consumer checks bet status is `SETTLING` before applying. If already `WON`/`LOST`, the message is acknowledged without re-processing. The `betId` serves as the natural dedup key. |

   **Flow with exactly-once guarantees:**
   ```
   API → Kafka (idempotent producer, transactional)
       → Consumer (read_committed, manual offset)
           → Redis WATCH/MULTI: PENDING → SETTLING (CAS, prevents double-match)
               → RocketMQ transactional message (coordinated with Redis)
                   → Consumer: SETTLING → WON/LOST (idempotent, status guard)
   ```

5. **Win/Loss determination** - The `BetSettlementService` compares the bet's `eventWinnerId` with the outcome's `eventWinnerId` to determine `WON` vs `LOST`.

6. **RocketMQ fallback** - If RocketMQ setup proves complex, the producer is behind an interface, allowing a mock/logging implementation to be swapped in.

---

## Exactly-Once Deep Dive

### Why not "at-least-once + idempotency" alone?

Simple idempotency (check-then-act) has a TOCTOU race window in concurrent environments. With multiple consumer instances, two could both read a bet as `PENDING` and both produce settlement messages. Redis `WATCH`/`MULTI`/`EXEC` provides a true compare-and-swap that eliminates this window.

### Bet Status State Machine

```
  PENDING ──────────────────▶ SETTLING ──────────────────▶ WON
     │          (CAS via              │      (settlement
     │        Redis WATCH)            │       consumer)
     │                                │
     │                                └──────────────────▶ LOST
     │
     └──────────────────────▶ (no match — stays PENDING)
```

- `PENDING` → `SETTLING`: Claimed by bet matcher (exactly one consumer wins the CAS)
- `SETTLING` → `WON`/`LOST`: Applied by settlement consumer (idempotent — only from `SETTLING`)
- Any re-delivery at any stage is safe: status guards prevent double-processing

### Failure Scenarios

| Failure | Recovery |
|---------|----------|
| App crashes after Kafka consume, before Redis CAS | Kafka redelivers (offset not committed). Bet is still `PENDING`. CAS succeeds on retry. |
| App crashes after Redis CAS (`SETTLING`), before RocketMQ send | RocketMQ transaction message is in `PREPARED` state — transaction check callback queries Redis, sees `SETTLING`, commits the message. |
| App crashes after RocketMQ send, before Kafka offset commit | Kafka redelivers. Redis CAS fails (bet is `SETTLING`). Message is skipped. No duplicate. |
| RocketMQ consumer crashes after consume, before status update | RocketMQ redelivers. Bet is still `SETTLING`. Settlement applies normally. |
| RocketMQ consumer crashes after status update (`WON`/`LOST`) | RocketMQ redelivers. Status guard sees `WON`/`LOST`, ACKs without re-processing. |
