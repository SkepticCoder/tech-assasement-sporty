package com.sporty.betting.domain.service;

import com.sporty.betting.domain.model.EventOutcome;

public interface BetMatchingService {

  void matchAndSettle(EventOutcome outcome);
}
