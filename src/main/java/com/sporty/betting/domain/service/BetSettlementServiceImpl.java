package com.sporty.betting.domain.service;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.model.BetStatus;
import com.sporty.betting.persistence.BetRepository;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class BetSettlementServiceImpl implements BetSettlementService {

  private final BetRepository betRepository;

  @Override
  public void settle(BetSettlement settlement) {
    Optional<Bet> optionalBet = betRepository.findById(settlement.getBetId());

    if (optionalBet.isEmpty()) {
      log.warn("Bet {} not found, skipping settlement", settlement.getBetId());
      return;
    }

    Bet bet = optionalBet.get();

    if (bet.getStatus() != BetStatus.SETTLING) {
      log.info(
          "Bet {} is in status {}, not SETTLING — idempotent skip", bet.getId(), bet.getStatus());
      return;
    }

    BetStatus result =
        settlement.getEventWinnerId().equals(settlement.getPredictedWinnerId())
            ? BetStatus.WON
            : BetStatus.LOST;

    bet.setStatus(result);
    bet.setSettledAt(Instant.now());
    betRepository.save(bet);

    log.info(
        "Bet {} settled as {} (predicted={}, actual={})",
        bet.getId(),
        result,
        settlement.getPredictedWinnerId(),
        settlement.getEventWinnerId());
  }
}
