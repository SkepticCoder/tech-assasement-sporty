package com.sporty.betting.messaging.rocketmq;

import com.sporty.betting.domain.model.BetSettlement;

public interface BetSettlementRocketProducer {

    String TOPIC = "bet-settlements";

    void send(BetSettlement settlement);
}
