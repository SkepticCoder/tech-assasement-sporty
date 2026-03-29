package com.sporty.betting.unit;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetSettlement;
import com.sporty.betting.domain.model.BetStatus;
import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.BetMatchingServiceImpl;
import com.sporty.betting.messaging.rocketmq.BetSettlementRocketProducer;
import com.sporty.betting.persistence.BetRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BetMatchingServiceTest {

    @Mock
    private BetRepository betRepository;

    @Mock
    private BetSettlementRocketProducer settlementProducer;

    @InjectMocks
    private BetMatchingServiceImpl betMatchingService;

    @Test
    void shouldNotSendSettlementWhenNoPendingBetsExist() {
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-999")
                .eventName("Test Match")
                .eventWinnerId("team-a")
                .build();

        when(betRepository.findByEventIdAndStatus("event-999", BetStatus.PENDING))
                .thenReturn(Collections.emptyList());

        betMatchingService.matchAndSettle(outcome);

        verifyNoInteractions(settlementProducer);
    }

    @Test
    void shouldSendSettlementForClaimedBets() {
        Bet bet = createBet("bet-1", "user-1", "event-100", "team-a");
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-100")
                .eventName("Final Match")
                .eventWinnerId("team-a")
                .build();

        when(betRepository.findByEventIdAndStatus("event-100", BetStatus.PENDING))
                .thenReturn(List.of(bet));
        when(betRepository.findById("bet-1"))
                .thenReturn(Optional.of(bet));

        betMatchingService.matchAndSettle(outcome);

        ArgumentCaptor<BetSettlement> captor = ArgumentCaptor.forClass(BetSettlement.class);
        verify(settlementProducer).send(captor.capture());

        BetSettlement settlement = captor.getValue();
        assertThat(settlement.getBetId()).isEqualTo("bet-1");
        assertThat(settlement.getEventWinnerId()).isEqualTo("team-a");
        assertThat(settlement.getPredictedWinnerId()).isEqualTo("team-a");
        assertThat(settlement.getBetAmount()).isEqualByComparingTo(new BigDecimal("50.00"));

        // Verify bet was saved with SETTLING status
        ArgumentCaptor<Bet> betCaptor = ArgumentCaptor.forClass(Bet.class);
        verify(betRepository).save(betCaptor.capture());
        assertThat(betCaptor.getValue().getStatus()).isEqualTo(BetStatus.SETTLING);
    }

    @Test
    void shouldSkipBetWhenAlreadyClaimed() {
        Bet bet = createBet("bet-1", "user-1", "event-100", "team-a");
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-100")
                .eventName("Final Match")
                .eventWinnerId("team-a")
                .build();

        when(betRepository.findByEventIdAndStatus("event-100", BetStatus.PENDING))
                .thenReturn(List.of(bet));

        // Simulate that bet was already claimed (status changed to SETTLING)
        Bet claimedBet = createBet("bet-1", "user-1", "event-100", "team-a");
        claimedBet.setStatus(BetStatus.SETTLING);
        when(betRepository.findById("bet-1"))
                .thenReturn(Optional.of(claimedBet));

        betMatchingService.matchAndSettle(outcome);

        verifyNoInteractions(settlementProducer);
    }

    @Test
    void shouldSkipBetWhenNotFoundOnReRead() {
        Bet bet = createBet("bet-1", "user-1", "event-100", "team-a");
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-100")
                .eventName("Final Match")
                .eventWinnerId("team-a")
                .build();

        when(betRepository.findByEventIdAndStatus("event-100", BetStatus.PENDING))
                .thenReturn(List.of(bet));
        when(betRepository.findById("bet-1"))
                .thenReturn(Optional.empty());

        betMatchingService.matchAndSettle(outcome);

        verifyNoInteractions(settlementProducer);
    }

    @Test
    void shouldSendSettlementsForMultipleBets() {
        Bet bet1 = createBet("bet-1", "user-1", "event-100", "team-a");
        Bet bet2 = createBet("bet-2", "user-2", "event-100", "team-b");
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-100")
                .eventName("Final Match")
                .eventWinnerId("team-a")
                .build();

        when(betRepository.findByEventIdAndStatus("event-100", BetStatus.PENDING))
                .thenReturn(List.of(bet1, bet2));
        when(betRepository.findById("bet-1")).thenReturn(Optional.of(bet1));
        when(betRepository.findById("bet-2")).thenReturn(Optional.of(bet2));

        betMatchingService.matchAndSettle(outcome);

        verify(settlementProducer, times(2)).send(any(BetSettlement.class));
    }

    private Bet createBet(String id, String userId, String eventId, String winnerId) {
        return Bet.builder()
                .id(id)
                .userId(userId)
                .eventId(eventId)
                .eventMarketId("market-1")
                .eventWinnerId(winnerId)
                .betAmount(new BigDecimal("50.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build();
    }
}
