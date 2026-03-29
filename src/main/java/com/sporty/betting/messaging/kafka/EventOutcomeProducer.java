package com.sporty.betting.messaging.kafka;

import com.sporty.betting.domain.model.EventOutcome;

public interface EventOutcomeProducer {

    void send(EventOutcome outcome);
}
