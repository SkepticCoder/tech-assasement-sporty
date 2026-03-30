# Dead-Letter Queue (DLQ) Implementation

## Overview

The Bet Settlement Service now includes a comprehensive **Dead-Letter Queue (DLQ)** system for handling, tracking, and monitoring failed event outcomes across both settlement strategies (Redis and Kafka Streams).

---

## Components

### 1. FailedEventOutcome Model
**File:** `com.sporty.betting.messaging.kafka.dlq.FailedEventOutcome`

Represents a failed event with full context:
```java
- eventOutcome: EventOutcome         // Original event that failed
- errorMessage: String               // Exception message
- stackTrace: String                 // Full exception stack trace
- failedAt: Instant                  // Timestamp of failure
- retryCount: Integer                // Number of retry attempts
- reason: String                     // Reason for DLQ routing
- strategy: String                   // Settlement strategy (REDIS/KAFKA_STREAMS)
```

### 2. DeadLetterQueueProducer
**File:** `com.sporty.betting.messaging.kafka.dlq.DeadLetterQueueProducer`

Sends failed events to appropriate DLQ topics:
- **DLQ_TOPIC** (`event-outcomes-dlq`) - Default DLQ for all failures
- **REDIS_DLQ_TOPIC** (`event-outcomes-redis-dlq`) - Redis-specific failures
- **STREAMS_DLQ_TOPIC** (`event-outcomes-streams-dlq`) - Kafka Streams-specific failures

**Key Methods:**
```java
sendToDLQ(EventOutcome, Exception, retryCount, strategy)
sendToRedisDLQ(EventOutcome, Exception, retryCount)
sendToStreamsDLQ(EventOutcome, Exception, retryCount)
```

### 3. DeadLetterQueueConsumer
**File:** `com.sporty.betting.messaging.kafka.dlq.DeadLetterQueueConsumer`

Consumes and monitors DLQ messages:
- Logs all failures with full context
- Tracks operational metrics
- Routes to alert systems (future enhancement)

### 4. Integration with Settlement Strategies

#### Redis Strategy
- Tracks retry count in Redis under `retry-count:{eventId}`
- After `MAX_RETRIES` (default: 3), sends to Redis DLQ
- Stores failed outcome IDs in Redis Set: `failed-outcomes`

#### Kafka Streams Strategy
- Catches processing exceptions immediately
- Sends to Kafka Streams DLQ with context
- Allows reprocessing via topic replay

---

## Configuration

### Enable/Disable DLQ
```yaml
dlq:
  enabled: true  # Enable DLQ functionality
  max-retries: 3 # Max retries before sending to DLQ
  monitoring:
    enabled: true
    alert-enabled: false  # Enable for production alerting
```

### Environment Variables
```bash
# Enable DLQ
export DLQ_ENABLED=true

# Set max retries
export DLQ_MAX_RETRIES=3

# Enable monitoring
export DLQ_MONITORING_ENABLED=true

# Enable alerts (requires alert service integration)
export DLQ_ALERT_ENABLED=false
```

---

## DLQ Topics

### Default DLQ Topic: `event-outcomes-dlq`
- Used for all settlement strategy failures
- Contains full FailedEventOutcome context
- Available for monitoring and replay

### Strategy-Specific Topics

#### `event-outcomes-redis-dlq`
- Failures from Redis settlement processor
- Contains all details of Redis processing failure
- Monitored by: `bet-settlement-redis-dlq-monitor` consumer group

#### `event-outcomes-streams-dlq`
- Failures from Kafka Streams topology
- Contains stream processing context
- Monitored by: `bet-settlement-streams-dlq-monitor` consumer group

---

## Error Handling Flow

### Redis Strategy Flow
```
EventOutcome from Kafka
    ↓
RedisBetSettlementProcessor
    ↓
Check Redis cache
    ↓
Process settlement
    ├─ SUCCESS → Acknowledge & mark processed
    └─ ERROR → Increment retry count
        ├─ Retries < MAX → Don't acknowledge (will retry)
        └─ Retries >= MAX → Send to Redis DLQ, Acknowledge
```

### Kafka Streams Strategy Flow
```
EventOutcome from Kafka
    ↓
Kafka Streams Topology
    ↓
Process settlement
    ├─ SUCCESS → Continue
    └─ ERROR → Send to Streams DLQ immediately
```

---

## Monitoring & Operations

### View DLQ Messages
```bash
# List all DLQ messages
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning

# View only Redis DLQ
kafka-console-consumer.sh --topic event-outcomes-redis-dlq \
  --bootstrap-server localhost:9092

# View only Streams DLQ
kafka-console-consumer.sh --topic event-outcomes-streams-dlq \
  --bootstrap-server localhost:9092
```

### Check DLQ Consumer Group Lag
```bash
# Check all DLQ monitor groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --list | grep dlq

# Check specific group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group bet-settlement-dlq-monitor \
  --describe
```

### Inspect FailedEventOutcome Details
```bash
# Use kafka-console-consumer with formatting
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --property print.key=true \
  --property print.value=true
```

---

## Replay Failed Events

### Manual Replay from DLQ
```bash
# Copy DLQ messages back to event-outcomes topic
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 \
  --from-beginning | \
kafka-console-producer.sh --topic event-outcomes \
  --bootstrap-server localhost:9092
```

### Programmatic Replay (Future Enhancement)
```java
// TODO: Create REST endpoint for DLQ event replay
@PostMapping("/admin/dlq/replay/{eventId}")
public ResponseEntity<String> replayFailedEvent(@PathVariable String eventId) {
    // Retrieve from DLQ topic
    // Restore original EventOutcome
    // Send to event-outcomes topic
    return ResponseEntity.ok("Event requeued for processing");
}
```

---

## Metrics & Alerts

### Metrics to Track
- **DLQ Messages** - Total messages in each DLQ topic
- **Failure Rate** - Percentage of failed events
- **Retry Count Distribution** - How many events exceed max retries
- **Time to DLQ** - Latency from event to DLQ
- **Strategy-Specific Failures** - Redis vs Streams failure rates

### Example Prometheus Metrics (Future)
```
# Number of failed events per strategy
bet_settlement_dlq_total{strategy="REDIS",topic="event-outcomes-redis-dlq"}
bet_settlement_dlq_total{strategy="KAFKA_STREAMS",topic="event-outcomes-streams-dlq"}

# Retry count histogram
bet_settlement_dlq_retries_histogram{strategy="REDIS"}
bet_settlement_dlq_retries_histogram{strategy="KAFKA_STREAMS"}

# Time to DLQ
bet_settlement_dlq_latency_seconds{strategy="REDIS"}
```

### Alerting Rules (Future)
```yaml
- name: HighDLQRate
  condition: rate(bet_settlement_dlq_total[5m]) > 0.1
  severity: warning
  notification: ops-team

- name: MaxRetriesExceeded
  condition: bet_settlement_dlq_total > 100
  severity: critical
  notification: on-call
```

---

## Testing DLQ

### Simulate Failures
```bash
# Disable RocketMQ to simulate settlement failure
export ROCKETMQ_ENABLED=false

# Send event that will fail
kafka-console-producer.sh --topic event-outcomes \
  --bootstrap-server localhost:9092 \
  --property "key.separator=:" \
  --value-serializer org.apache.kafka.common.serialization.StringSerializer

# Message content (as JSON string)
event-1:{"eventId":"event-1","eventName":"Test","eventWinnerId":"team-a"}
```

### View DLQ Output
```bash
# Monitor DLQ consumption in real-time
kubectl logs -f deployment/bet-settlement-service | grep "DLQ Message"
```

---

## Troubleshooting

### Issue: Events Not Reaching DLQ
**Cause:** DLQ topic doesn't exist
```bash
# Create DLQ topics
kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic event-outcomes-dlq \
  --partitions 3 \
  --replication-factor 1

kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic event-outcomes-redis-dlq \
  --partitions 3 \
  --replication-factor 1

kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic event-outcomes-streams-dlq \
  --partitions 3 \
  --replication-factor 1
```

### Issue: DLQ Consumer Lag Growing
**Solution:** Check consumer health and logs
```bash
# Check consumer group status
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group bet-settlement-dlq-monitor \
  --describe

# Restart consumer if stuck
kubectl restart deployment bet-settlement-service
```

### Issue: Missing Stack Traces in DLQ
**Cause:** Serialization issue with stack trace field
```bash
# Check FailedEventOutcome deserialization
# Ensure JsonDeserializer trusted packages includes dlq package
# In KafkaConfig:
props.put(JsonDeserializer.TRUSTED_PACKAGES, 
    "com.sporty.betting.*,com.sporty.betting.messaging.kafka.dlq");
```

---

## Future Enhancements

- [ ] REST API endpoint `/admin/dlq` for viewing DLQ messages
- [ ] Manual event replay functionality
- [ ] DLQ retention policy (cleanup old messages)
- [ ] Metrics dashboard for DLQ monitoring
- [ ] Email/Slack alerts for critical failures
- [ ] Event transformation and correction UI
- [ ] Automatic retry with exponential backoff
- [ ] Dead-letter queue archival and analysis tools
- [ ] Analytics on failure patterns and root causes
- [ ] Integration with external error tracking (Sentry, Rollbar)

---

## Related Files

- `DeadLetterQueueProducer.java` - Sends failures to DLQ
- `DeadLetterQueueConsumer.java` - Monitors DLQ topics
- `FailedEventOutcome.java` - DLQ message model
- `RedisBetSettlementProcessor.java` - Redis strategy error handling
- `BetSettlementStreamsConfiguration.java` - Kafka Streams error handling
- `KafkaConfig.java` - DLQ template and consumer factory setup
- `application.yml` - DLQ configuration properties

