package com.sporty.betting.messaging.rocketmq;

import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.service.BetSettlementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Fallback producer when RocketMQ is not available. Logs the payload and directly invokes the
 * settlement service to simulate the full flow without RocketMQ infrastructure.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "rocketmq.enabled", havingValue = "false")
public class MockBetSettlementProducer implements BetSettlementRocketProducer {

  private final BetSettlementService betSettlementService;

  @Override
  public void send(BetSettlement settlement) {
    log.info("[MOCK RocketMQ] Would send to topic '{}': {}", TOPIC, settlement);
    // Directly settle to simulate the consumer side
    betSettlementService.settle(settlement);
  }
}
