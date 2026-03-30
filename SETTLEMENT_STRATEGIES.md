# Settlement Strategies: Redis vs Kafka Streams

This project supports two complementary approaches for bet settlement processing:

## 1. Redis-Based Strategy (Cache-Driven)

**Enabled by:** `settlement.strategy=REDIS`

### How it works:
- Event outcomes are consumed via Kafka `@KafkaListener`
- Redis is used as a distributed cache and state store
- Each event outcome is cached to prevent duplicate processing
- Processed outcomes are tracked in a Redis Set
- Direct settlement processing on each event

### Configuration:
```yaml
settlement:
  strategy: REDIS
  redis-enabled: true
  kafka-streams-enabled: false

spring:
  data:
    redis:
      host: localhost
      port: 6379
```

### Advantages:
✅ Simpler architecture
✅ Direct event processing
✅ Leverages Redis for deduplication
✅ Easy to debug and monitor
✅ Lower latency per event

### Disadvantages:
❌ Requires external Redis instance
❌ Limited stream transformations
❌ No built-in windowing/joining

### Usage in Tests:
```bash
SETTLEMENT_STRATEGY=REDIS ./gradlew test
```

---

## 2. Kafka Streams Strategy (Stream-Processing)

**Enabled by:** `settlement.strategy=KAFKA_STREAMS` (default)

### How it works:
- Event outcomes are consumed via `StreamsBuilder` topology
- Kafka Streams provides distributed stream processing
- Built-in fault tolerance and exactly-once semantics
- Topology can include transformations, joins, and windowing
- State stores managed by Kafka Streams internally

### Configuration:
```yaml
settlement:
  strategy: KAFKA_STREAMS
  redis-enabled: true
  kafka-streams-enabled: true

spring:
  kafka:
    streams:
      application-id: bet-settlement-streams-app
      state-dir: /tmp/kafka-streams-state
      cache-max-bytes-buffering: 10485760
```

### Advantages:
✅ Distributed stream processing
✅ Exactly-once semantics
✅ Built-in scalability and parallelism
✅ Advanced topology transformations
✅ Interactive queries on state stores
✅ No external state store required

### Disadvantages:
❌ More complex topology management
❌ Steeper learning curve
❌ Higher operational overhead

### Usage in Tests:
```bash
SETTLEMENT_STRATEGY=KAFKA_STREAMS ./gradlew test
```

---

## Running Both Strategies Simultaneously

You can enable both strategies to run together:

```yaml
settlement:
  redis-enabled: true
  kafka-streams-enabled: true
```

In this mode:
- **Kafka Streams** is the primary processor (via StreamsBuilder)
- **Redis** acts as an additional cache and deduplication layer
- Both can process events independently

---

## Switching Strategies at Runtime

### Via Environment Variables:
```bash
# Use Redis
export SETTLEMENT_STRATEGY=REDIS
export SETTLEMENT_REDIS_ENABLED=true
export SETTLEMENT_KAFKA_STREAMS_ENABLED=false

# Use Kafka Streams
export SETTLEMENT_STRATEGY=KAFKA_STREAMS
export SETTLEMENT_REDIS_ENABLED=false
export SETTLEMENT_KAFKA_STREAMS_ENABLED=true

# Use Both
export SETTLEMENT_REDIS_ENABLED=true
export SETTLEMENT_KAFKA_STREAMS_ENABLED=true
```

### Via application.yml:
```yaml
settlement:
  strategy: ${SETTLEMENT_STRATEGY:KAFKA_STREAMS}
  redis-enabled: ${SETTLEMENT_REDIS_ENABLED:true}
  kafka-streams-enabled: ${SETTLEMENT_KAFKA_STREAMS_ENABLED:true}
```

---

## Architecture Comparison

| Aspect | Redis | Kafka Streams |
|--------|-------|---------------|
| **Processing Model** | Direct/Imperative | Stream/Declarative |
| **State Management** | External (Redis) | Internal (Kafka topics) |
| **Parallelism** | Limited | High |
| **Semantics** | At-least-once | Exactly-once |
| **Complexity** | Low | Medium-High |
| **Dependencies** | Redis server | Kafka cluster |
| **Scalability** | Vertical | Horizontal |

---

## Monitoring

### Redis Strategy
- Monitor Redis memory usage: `redis-cli INFO memory`
- Check processed outcomes: `redis-cli SMEMBERS processed-outcomes`
- Clear cache: Application endpoint (if implemented)

### Kafka Streams Strategy
- Monitor topology via Spring Boot Actuator: `/actuator/metrics`
- Check state store lag
- Monitor consumer group lag: `./bin/kafka-consumer-groups.sh --describe --group bet-settlement-streams-app`

---

## Testing

### Run tests with specific strategy:

**Redis Strategy Tests:**
```bash
SETTLEMENT_STRATEGY=REDIS ./gradlew test
```

**Kafka Streams Strategy Tests:**
```bash
SETTLEMENT_STRATEGY=KAFKA_STREAMS ./gradlew test
```

**Both Strategies:**
```bash
SETTLEMENT_REDIS_ENABLED=true SETTLEMENT_KAFKA_STREAMS_ENABLED=true ./gradlew test
```

---

## Future Enhancements

- [ ] Metrics and KPIs for each strategy
- [ ] Dynamic strategy switching without restart
- [ ] Circuit breaker for fallback strategy
- [ ] Performance benchmarking
- [ ] Event replay capability
- [ ] Dead-letter queue handling for failed events

