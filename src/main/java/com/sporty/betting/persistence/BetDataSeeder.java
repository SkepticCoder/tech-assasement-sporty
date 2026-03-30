package com.sporty.betting.persistence;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetStatus;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class BetDataSeeder implements CommandLineRunner {

  private final BetRepository betRepository;

  @Override
  public void run(String... args) {
    if (betRepository.count() > 0) {
      log.info("Bets already seeded, skipping");
      return;
    }

    List<Bet> bets =
        List.of(
            Bet.builder()
                .id(UUID.randomUUID().toString())
                .userId("user-1")
                .eventId("event-100")
                .eventMarketId("market-1")
                .eventWinnerId("team-a")
                .betAmount(new BigDecimal("50.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build(),
            Bet.builder()
                .id(UUID.randomUUID().toString())
                .userId("user-2")
                .eventId("event-100")
                .eventMarketId("market-1")
                .eventWinnerId("team-b")
                .betAmount(new BigDecimal("100.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build(),
            Bet.builder()
                .id(UUID.randomUUID().toString())
                .userId("user-3")
                .eventId("event-200")
                .eventMarketId("market-2")
                .eventWinnerId("team-c")
                .betAmount(new BigDecimal("75.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build(),
            Bet.builder()
                .id(UUID.randomUUID().toString())
                .userId("user-4")
                .eventId("event-200")
                .eventMarketId("market-2")
                .eventWinnerId("team-d")
                .betAmount(new BigDecimal("200.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build());

    betRepository.saveAll(bets);
    log.info("Seeded {} bets into Redis", bets.size());
  }
}
