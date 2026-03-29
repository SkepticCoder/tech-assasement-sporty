package com.sporty.betting.domain.service;

import com.sporty.betting.domain.model.EventOutcome;

public interface EventOutcomeService {

    void publishOutcome(EventOutcome outcome);
}
