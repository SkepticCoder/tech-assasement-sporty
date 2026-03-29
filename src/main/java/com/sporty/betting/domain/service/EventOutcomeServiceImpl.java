package com.sporty.betting.domain.service;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.messaging.kafka.EventOutcomeProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventOutcomeServiceImpl implements EventOutcomeService {

    private final EventOutcomeProducer kafkaProducer;

    @Override
    public void publishOutcome(EventOutcome outcome) {
        log.info("Publishing event outcome for eventId={}", outcome.getEventId());
        kafkaProducer.send(outcome);
    }
}
