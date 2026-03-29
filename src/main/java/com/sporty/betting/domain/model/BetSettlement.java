package com.sporty.betting.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

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
