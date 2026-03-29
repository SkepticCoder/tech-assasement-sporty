package com.sporty.betting.messaging.rocketmq;

import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.service.BetSettlementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "rocketmq.enabled", havingValue = "true", matchIfMissing = true)
@RocketMQMessageListener(
        topic = BetSettlementRocketProducer.TOPIC,
        consumerGroup = "bet-settlement-consumer-group"
)
public class BetSettlementRocketConsumer implements RocketMQListener<BetSettlement> {

    private final BetSettlementService betSettlementService;

    @Override
    public void onMessage(BetSettlement settlement) {
        log.info("Consumed bet settlement from RocketMQ: betId={}", settlement.getBetId());
        betSettlementService.settle(settlement);
    }
}
