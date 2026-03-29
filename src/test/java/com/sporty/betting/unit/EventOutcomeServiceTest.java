package com.sporty.betting.unit;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.EventOutcomeServiceImpl;
import com.sporty.betting.messaging.kafka.EventOutcomeProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventOutcomeServiceTest {

    @Mock
    private EventOutcomeProducer kafkaProducer;

    @InjectMocks
    private EventOutcomeServiceImpl eventOutcomeService;

    @Test
    void shouldDelegateToKafkaProducer() {
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-100")
                .eventName("Test Match")
                .eventWinnerId("team-a")
                .build();

        eventOutcomeService.publishOutcome(outcome);

        verify(kafkaProducer).send(outcome);
    }
}
