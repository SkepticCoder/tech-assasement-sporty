package com.sporty.betting.messaging.kafka;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.BetMatchingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventOutcomeKafkaConsumer {

    private final BetMatchingService betMatchingService;

    @KafkaListener(
            topics = EventOutcomeKafkaProducer.TOPIC,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(EventOutcome outcome, Acknowledgment acknowledgment) {
        log.info("Consumed event outcome from Kafka: eventId={}, winner={}",
                outcome.getEventId(), outcome.getEventWinnerId());
        betMatchingService.matchAndSettle(outcome);
        acknowledgment.acknowledge();
        log.debug("Offset committed for eventId={}", outcome.getEventId());
    }
}
