package com.sporty.betting.unit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sporty.betting.api.EventOutcomeController;
import com.sporty.betting.api.dto.EventOutcomeRequest;
import com.sporty.betting.domain.service.EventOutcomeService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventOutcomeController.class)
class EventOutcomeControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private EventOutcomeService eventOutcomeService;

    @Test
    void shouldReturn202WhenValidRequest() throws Exception {
        EventOutcomeRequest request = new EventOutcomeRequest()
                .eventId("event-100")
                .eventName("Champions League Final")
                .eventWinnerId("team-a");

        mockMvc.perform(post("/api/v1/event-outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value("event-100"))
                .andExpect(jsonPath("$.status").value("ACCEPTED"));
    }

    @Test
    void shouldReturn400WhenEventIdMissing() throws Exception {
        EventOutcomeRequest request = new EventOutcomeRequest()
                .eventName("Champions League Final")
                .eventWinnerId("team-a");

        mockMvc.perform(post("/api/v1/event-outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldReturn400WhenEmptyBody() throws Exception {
        mockMvc.perform(post("/api/v1/event-outcomes")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{}"))
                .andExpect(status().isBadRequest());
    }
}
