# Quick Reference: Using Kafka DLQ

## Overview

The Bet Settlement Service automatically routes failed event outcomes to Kafka Dead-Letter Queue (DLQ) topics for monitoring and recovery.

## DLQ Topics at a Glance

```
event-outcomes-dlq           ← All failures
event-outcomes-redis-dlq     ← Redis strategy failures  
event-outcomes-streams-dlq   ← Kafka Streams failures
```

## Enable DLQ (Default: ON)

```bash
export DLQ_ENABLED=true
export DLQ_MAX_RETRIES=3
export DLQ_MONITORING_ENABLED=true
```

## Common Commands

### 1. View Failed Events in DLQ
```bash
# All failures
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 --from-beginning

# Redis failures only
kafka-console-consumer.sh --topic event-outcomes-redis-dlq \
  --bootstrap-server localhost:9092

# Kafka Streams failures only
kafka-console-consumer.sh --topic event-outcomes-streams-dlq \
  --bootstrap-server localhost:9092
```

### 2. Monitor DLQ Consumer Progress
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group bet-settlement-dlq-monitor --describe
```

### 3. Replay Failed Events
```bash
# Send DLQ events back to event-outcomes for reprocessing
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092 --from-beginning | \
kafka-console-producer.sh --topic event-outcomes \
  --bootstrap-server localhost:9092
```

### 4. Create DLQ Topics (if needed)
```bash
# Create all three DLQ topics
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic event-outcomes-dlq --partitions 3 --replication-factor 1

kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic event-outcomes-redis-dlq --partitions 3 --replication-factor 1

kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic event-outcomes-streams-dlq --partitions 3 --replication-factor 1
```

## Test DLQ

### Simulate Failures
```bash
# Disable RocketMQ to trigger settlement failures
export ROCKETMQ_ENABLED=false
./gradlew bootRun

# Send event that will fail
curl -X POST http://localhost:8080/api/v1/event-outcomes \
  -H "Content-Type: application/json" \
  -d '{
    "eventId": "test-fail",
    "eventName": "Test Event",
    "eventWinnerId": "team-a"
  }'

# Monitor DLQ in real-time
kafka-console-consumer.sh --topic event-outcomes-dlq \
  --bootstrap-server localhost:9092
```

## What's in DLQ Messages

Each message contains:
- **eventOutcome** - Original event (eventId, name, winner)
- **errorMessage** - Exception that caused failure
- **stackTrace** - Full stack trace for debugging
- **failedAt** - Timestamp of failure
- **retryCount** - How many retries were attempted
- **strategy** - REDIS or KAFKA_STREAMS
- **reason** - Why it was sent to DLQ

## Troubleshooting

| Issue | Solution |
|-------|----------|
| No DLQ messages received | Create DLQ topics (see "Create DLQ Topics" above) |
| Consumer lag growing | Check `kafka-consumer-groups.sh --describe` |
| Service stuck | Restart: `docker compose restart bet-settlement-service` |
| Can't find events | They may have been reprocessed already |

## Documentation

- Full details: [DEAD_LETTER_QUEUE.md](DEAD_LETTER_QUEUE.md)
- Configuration: [application.yml](src/main/resources/application.yml)
- Implementation: [DLQ_IMPLEMENTATION_SUMMARY.md](DLQ_IMPLEMENTATION_SUMMARY.md)

