package com.sporty.betting.messaging.kafka.dlq;

import com.sporty.betting.domain.model.EventOutcome;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Dead-Letter Queue Producer for failed event outcomes. Sends failed settlements to the DLQ topic
 * for monitoring and replay.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeadLetterQueueProducer {

  private final KafkaTemplate<String, FailedEventOutcome> kafkaTemplate;

  public static final String DLQ_TOPIC = "event-outcomes-dlq";

  /**
   * Send failed event outcome to the DLQ topic.
   *
   * @param eventOutcome the event that failed
   * @param exception the exception that caused the failure
   * @param retryCount number of retry attempts
   * @param strategy the settlement strategy that failed
   */
  public void sendToDLQ(
      EventOutcome eventOutcome, Exception exception, Integer retryCount, String strategy) {
    try {
      FailedEventOutcome failedEvent =
          FailedEventOutcome.builder()
              .eventOutcome(eventOutcome)
              .errorMessage(exception.getMessage())
              .stackTrace(getStackTrace(exception))
              .failedAt(Instant.now())
              .retryCount(retryCount)
              .strategy(strategy)
              .reason("Processing failure")
              .build();

      kafkaTemplate.send(DLQ_TOPIC, eventOutcome.getEventId(), failedEvent);

      log.warn(
          "Failed event outcome sent to DLQ: eventId={}, strategy={}, retries={}",
          eventOutcome.getEventId(),
          strategy,
          retryCount);

    } catch (Exception e) {
      log.error("Failed to send event outcome to DLQ: eventId={}", eventOutcome.getEventId(), e);
    }
  }

  /** Extract stack trace from exception as a string. */
  private String getStackTrace(Exception exception) {
    StringWriter sw = new StringWriter();
    exception.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }
}
