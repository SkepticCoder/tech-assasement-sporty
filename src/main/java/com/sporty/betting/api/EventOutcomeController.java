package com.sporty.betting.api;

import com.sporty.betting.api.dto.EventOutcomeRequest;
import com.sporty.betting.api.dto.EventOutcomeResponse;
import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.EventOutcomeService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class EventOutcomeController implements EventOutcomesApi {

    private final EventOutcomeService eventOutcomeService;

    @Override
    public ResponseEntity<EventOutcomeResponse> publishEventOutcome(EventOutcomeRequest eventOutcomeRequest) {
        EventOutcome outcome = EventOutcome.builder()
                .eventId(eventOutcomeRequest.getEventId())
                .eventName(eventOutcomeRequest.getEventName())
                .eventWinnerId(eventOutcomeRequest.getEventWinnerId())
                .build();

        eventOutcomeService.publishOutcome(outcome);

        EventOutcomeResponse response = new EventOutcomeResponse()
                .eventId(eventOutcomeRequest.getEventId())
                .status("ACCEPTED")
                .message("Event outcome published for settlement processing");

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
