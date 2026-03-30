package com.sporty.betting.messaging.kafka.dlq;

import com.sporty.betting.domain.model.EventOutcome;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Dead-Letter Queue message for failed event outcomes. Tracks failed settlements for monitoring and
 * replay.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailedEventOutcome {

  /** The original event outcome that failed */
  private EventOutcome eventOutcome;

  /** Exception message that caused the failure */
  private String errorMessage;

  /** Full exception stack trace */
  private String stackTrace;

  /** Timestamp when the failure occurred */
  private Instant failedAt;

  /** Number of retry attempts */
  private Integer retryCount;

  /** Reason for sending to DLQ */
  private String reason;

  /** The settlement strategy that failed */
  private String strategy;
}
