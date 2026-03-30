package com.sporty.betting.unit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.model.BetStatus;
import com.sporty.betting.domain.service.BetSettlementServiceImpl;
import com.sporty.betting.persistence.BetRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Disabled
class BetSettlementServiceTest {

  @Mock private BetRepository betRepository;

  @InjectMocks private BetSettlementServiceImpl betSettlementService;

  @Test
  void shouldSettleBetAsWonWhenPredictionMatches() {
    Bet bet = createBet("bet-1", BetStatus.SETTLING, "team-a");
    BetSettlement settlement = createSettlement("bet-1", "team-a", "team-a");

    when(betRepository.findById("bet-1")).thenReturn(Optional.of(bet));

    betSettlementService.settle(settlement);

    ArgumentCaptor<Bet> captor = ArgumentCaptor.forClass(Bet.class);
    verify(betRepository).save(captor.capture());

    assertThat(captor.getValue().getStatus()).isEqualTo(BetStatus.WON);
    assertThat(captor.getValue().getSettledAt()).isNotNull();
  }

  @Test
  void shouldSettleBetAsLostWhenPredictionDoesNotMatch() {
    Bet bet = createBet("bet-2", BetStatus.SETTLING, "team-b");
    BetSettlement settlement = createSettlement("bet-2", "team-a", "team-b");

    when(betRepository.findById("bet-2")).thenReturn(Optional.of(bet));

    betSettlementService.settle(settlement);

    ArgumentCaptor<Bet> captor = ArgumentCaptor.forClass(Bet.class);
    verify(betRepository).save(captor.capture());

    assertThat(captor.getValue().getStatus()).isEqualTo(BetStatus.LOST);
    assertThat(captor.getValue().getSettledAt()).isNotNull();
  }

  @Test
  void shouldSkipSettlementWhenBetNotInSettlingStatus() {
    Bet bet = createBet("bet-3", BetStatus.WON, "team-a");
    BetSettlement settlement = createSettlement("bet-3", "team-a", "team-a");

    when(betRepository.findById("bet-3")).thenReturn(Optional.of(bet));

    betSettlementService.settle(settlement);

    verify(betRepository, never()).save(any());
  }

  @Test
  void shouldSkipSettlementWhenBetNotFound() {
    BetSettlement settlement = createSettlement("nonexistent", "team-a", "team-a");

    when(betRepository.findById("nonexistent")).thenReturn(Optional.empty());

    betSettlementService.settle(settlement);

    verify(betRepository, never()).save(any());
  }

  private Bet createBet(String id, BetStatus status, String winnerId) {
    return Bet.builder()
        .id(id)
        .userId("user-1")
        .eventId("event-100")
        .eventMarketId("market-1")
        .eventWinnerId(winnerId)
        .betAmount(new BigDecimal("50.00"))
        .status(status)
        .createdAt(Instant.now())
        .build();
  }

  private BetSettlement createSettlement(
      String betId, String eventWinnerId, String predictedWinnerId) {
    return BetSettlement.builder()
        .betId(betId)
        .userId("user-1")
        .eventId("event-100")
        .eventWinnerId(eventWinnerId)
        .predictedWinnerId(predictedWinnerId)
        .betAmount(new BigDecimal("50.00"))
        .build();
  }
}
