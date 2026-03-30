package com.sporty.betting.domain.model;

import java.math.BigDecimal;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.index.Indexed;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@RedisHash("bet")
public class Bet {

  @Id private String id;

  private String userId;

  @Indexed private String eventId;

  private String eventMarketId;

  private String eventWinnerId;

  private BigDecimal betAmount;

  @Indexed private BetStatus status;

  private Instant createdAt;

  private Instant settledAt;
}
