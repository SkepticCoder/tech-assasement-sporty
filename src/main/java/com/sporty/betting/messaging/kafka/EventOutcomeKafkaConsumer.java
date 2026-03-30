package com.sporty.betting.messaging.kafka;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.BetMatchingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Legacy Kafka consumer using @KafkaListener annotation. This is kept for reference but Kafka
 * Streams topology is preferred. To use this instead, disable the Kafka Streams topology in
 * BetSettlementStreamsConfiguration.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventOutcomeKafkaConsumer {

  private final BetMatchingService betMatchingService;

  /**
   * Legacy consumer method - disabled in favor of Kafka Streams topology. Uncomment @KafkaListener
   * to enable.
   */
  // @KafkaListener(
  //     topics = EventOutcomeKafkaProducer.TOPIC,
  //     groupId = "${spring.kafka.consumer.group-id}",
  //     containerFactory = "kafkaListenerContainerFactory")
  public void consume(EventOutcome outcome, Acknowledgment acknowledgment) {
    log.info(
        "Consumed event outcome from Kafka: eventId={}, winner={}",
        outcome.getEventId(),
        outcome.getEventWinnerId());
    betMatchingService.matchAndSettle(outcome);
    acknowledgment.acknowledge();
    log.debug("Offset committed for eventId={}", outcome.getEventId());
  }
}
