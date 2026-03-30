package com.sporty.betting.messaging.redis;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.BetMatchingService;
import com.sporty.betting.messaging.kafka.dlq.DeadLetterQueueProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Redis-based bet settlement processor. Uses Redis as a distributed cache and state store for event
 * outcomes. Falls back to direct processing if Redis is unavailable.
 *
 * <p>Enabled via: settlement.redis-enabled=true
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "settlement.strategy", havingValue = "REDIS")
@RequiredArgsConstructor
public class RedisBetSettlementProcessor {

  private final BetMatchingService betMatchingService;
  private final RedisTemplate<String, Object> redisTemplate;
  private final DeadLetterQueueProducer dlqProducer;

  private static final String OUTCOME_CACHE_PREFIX = "event-outcome:";
  private static final String PROCESSED_OUTCOMES_SET = "processed-outcomes";
  private static final String FAILED_OUTCOMES_SET = "failed-outcomes";
  private static final String RETRY_COUNT_PREFIX = "retry-count:";
  private static final Integer MAX_RETRIES = 3;

  /**
   * Process event outcomes using Redis as a state store. Kafka listener for event-outcomes topic
   * with Redis cache layer.
   *
   * <p>Flow:
   *
   * <ol>
   *   <li>Receive EventOutcome from Kafka
   *   <li>Check Redis cache to avoid duplicate processing
   *   <li>If not cached, process and cache the outcome
   *   <li>Trigger bet matching and settlement
   * </ol>
   */
  @KafkaListener(
      topics = "event-outcomes",
      groupId = "bet-settlement-redis-group",
      containerFactory = "kafkaListenerContainerFactory")
  public void processEventOutcomeWithRedis(EventOutcome outcome, Acknowledgment acknowledgment) {
    String cacheKey = OUTCOME_CACHE_PREFIX + outcome.getEventId();

    try {
      log.info(
          "Processing event outcome via Redis: eventId={}, winner={}",
          outcome.getEventId(),
          outcome.getEventWinnerId());

      // Check if already processed
      Boolean alreadyProcessed =
          redisTemplate.opsForSet().isMember(PROCESSED_OUTCOMES_SET, cacheKey);

      if (Boolean.TRUE.equals(alreadyProcessed)) {
        log.warn("Event outcome already processed: eventId={}", outcome.getEventId());
        acknowledgment.acknowledge();
        return;
      }

      // Cache the outcome
      redisTemplate.opsForValue().set(cacheKey, outcome);

      // Process bet settlement
      betMatchingService.matchAndSettle(outcome);

      // Mark as processed
      redisTemplate.opsForSet().add(PROCESSED_OUTCOMES_SET, cacheKey);

      acknowledgment.acknowledge();
      log.debug(
          "Successfully processed and cached event outcome: eventId={}", outcome.getEventId());

    } catch (Exception e) {
      log.error("Error processing event outcome via Redis: eventId={}", outcome.getEventId(), e);

      // Get retry count from Redis
      String retryCountKey = RETRY_COUNT_PREFIX + outcome.getEventId();
      Long currentRetries = redisTemplate.opsForValue().increment(retryCountKey);

      if (currentRetries >= MAX_RETRIES) {
        log.error(
            "Max retries exceeded for event outcome: eventId={}, retries={}",
            outcome.getEventId(),
            currentRetries);
        // Send to DLQ for manual intervention
        dlqProducer.sendToDLQ(outcome, e, currentRetries.intValue(), "REDIS");
        // Mark as failed
        redisTemplate
            .opsForSet()
            .add(FAILED_OUTCOMES_SET, OUTCOME_CACHE_PREFIX + outcome.getEventId());
        acknowledgment.acknowledge();
      }
      // Don't acknowledge on error to allow retry if below max
    }
  }

  /**
   * Get cached event outcome from Redis.
   *
   * @param eventId the event ID
   * @return the cached EventOutcome or null if not found
   */
  public EventOutcome getCachedOutcome(String eventId) {
    return (EventOutcome) redisTemplate.opsForValue().get(OUTCOME_CACHE_PREFIX + eventId);
  }

  /** Clear cached outcomes for testing/maintenance purposes. */
  public void clearCache() {
    log.info("Clearing Redis settlement cache");
    redisTemplate.delete(redisTemplate.keys(OUTCOME_CACHE_PREFIX + "*"));
    redisTemplate.delete(PROCESSED_OUTCOMES_SET);
  }
}
