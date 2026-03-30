package com.sporty.betting.messaging.kafka.dlq;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Dead-Letter Queue Consumer for monitoring and tracking failed event outcomes. Logs and tracks all
 * failures for operational visibility and debugging.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterQueueConsumer {

  /** Consume failed events from the DLQ topic. Logs all failures for monitoring and alerting. */
  @KafkaListener(
      topics = DeadLetterQueueProducer.DLQ_TOPIC,
      groupId = "bet-settlement-dlq-monitor",
      containerFactory = "dlqKafkaListenerContainerFactory")
  public void consumeDLQ(FailedEventOutcome failedEvent) {
    log.error(
        "DLQ Message Received: eventId={}, strategy={}, reason={}, retries={}, error={}",
        failedEvent.getEventOutcome().getEventId(),
        failedEvent.getStrategy(),
        failedEvent.getReason(),
        failedEvent.getRetryCount(),
        failedEvent.getErrorMessage());

    log.debug(
        "DLQ Stack Trace for eventId={}: \n{}",
        failedEvent.getEventOutcome().getEventId(),
        failedEvent.getStackTrace());

    // TODO: Send alert/notification to operations team
    // TODO: Persist to monitoring database for analytics
    // TODO: Expose metrics for Prometheus/Grafana
  }
}
