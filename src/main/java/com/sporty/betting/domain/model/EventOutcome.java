package com.sporty.betting.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventOutcome {

    private String eventId;
    private String eventName;
    private String eventWinnerId;
}
