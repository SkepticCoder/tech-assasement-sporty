package com.sporty.betting.messaging.rocketmq;

import com.sporty.betting.domain.model.BetSettlement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "rocketmq.enabled", havingValue = "true", matchIfMissing = true)
public class RocketMQBetSettlementProducer implements BetSettlementRocketProducer {

    private final RocketMQTemplate rocketMQTemplate;

    @Override
    public void send(BetSettlement settlement) {
        rocketMQTemplate.convertAndSend(TOPIC, settlement);
        log.info("Sent bet settlement to RocketMQ: betId={}", settlement.getBetId());
    }
}
