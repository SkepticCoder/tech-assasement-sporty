package com.sporty.betting.domain.model;

import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BetSettlement {

  private String betId;
  private String userId;
  private String eventId;
  private String eventWinnerId;
  private String predictedWinnerId;
  private BigDecimal betAmount;
}
