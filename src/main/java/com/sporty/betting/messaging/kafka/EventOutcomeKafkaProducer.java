package com.sporty.betting.messaging.kafka;

import com.sporty.betting.domain.model.EventOutcome;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventOutcomeKafkaProducer implements EventOutcomeProducer {

    public static final String TOPIC = "event-outcomes";

    private final KafkaTemplate<String, EventOutcome> kafkaTemplate;

    @Override
    public void send(EventOutcome outcome) {
        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(TOPIC, outcome.getEventId(), outcome);
            log.info("Published event outcome to Kafka: eventId={}", outcome.getEventId());
            return true;
        });
    }
}
