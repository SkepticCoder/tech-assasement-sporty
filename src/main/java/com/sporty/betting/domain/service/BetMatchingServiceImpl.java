package com.sporty.betting.domain.service;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.model.BetStatus;
import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.messaging.rocketmq.BetSettlementRocketProducer;
import com.sporty.betting.persistence.BetRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BetMatchingServiceImpl implements BetMatchingService {

  private final BetRepository betRepository;
  private final BetSettlementRocketProducer settlementProducer;

  @Override
  public void matchAndSettle(EventOutcome outcome) {
    List<Bet> pendingBets =
        betRepository.findByEventIdAndStatus(outcome.getEventId(), BetStatus.PENDING);

    if (pendingBets.isEmpty()) {
      log.info("No pending bets found for eventId={}", outcome.getEventId());
      return;
    }

    log.info("Found {} pending bets for eventId={}", pendingBets.size(), outcome.getEventId());

    for (Bet bet : pendingBets) {
      if (claimBet(bet)) {
        BetSettlement settlement =
            BetSettlement.builder()
                .betId(bet.getId())
                .userId(bet.getUserId())
                .eventId(bet.getEventId())
                .eventWinnerId(outcome.getEventWinnerId())
                .predictedWinnerId(bet.getEventWinnerId())
                .betAmount(bet.getBetAmount())
                .build();

        settlementProducer.send(settlement);
        log.info("Sent settlement for betId={}", bet.getId());
      } else {
        log.info("Bet {} already claimed by another consumer, skipping", bet.getId());
      }
    }
  }

  /**
   * Claims a bet by transitioning its status from PENDING to SETTLING. Re-reads from repository to
   * guard against concurrent consumers. The SETTLING intermediate state prevents double-settlement:
   * only one consumer can successfully transition PENDING → SETTLING.
   */
  private boolean claimBet(Bet bet) {
    return betRepository
        .findById(bet.getId())
        .filter(b -> b.getStatus() == BetStatus.PENDING)
        .map(
            b -> {
              b.setStatus(BetStatus.SETTLING);
              betRepository.save(b);
              return true;
            })
        .orElse(false);
  }
}
