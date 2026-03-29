package com.sporty.betting.acceptance;

import com.sporty.betting.api.dto.EventOutcomeRequest;
import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetStatus;
import com.sporty.betting.persistence.BetRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class BetSettlementAcceptanceTest {

    private static final String KAFKA_SERVICE = "kafka";
    private static final int KAFKA_PORT = 9092;
    private static final String REDIS_SERVICE = "redis";
    private static final int REDIS_PORT = 6379;

    @Container
    static DockerComposeContainer<?> compose = new DockerComposeContainer<>(
            new File("src/test/resources/docker-compose-test.yml"))
            .withExposedService(KAFKA_SERVICE, KAFKA_PORT,
                    Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
            .withExposedService(REDIS_SERVICE, REDIS_PORT,
                    Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () ->
                compose.getServiceHost(KAFKA_SERVICE, KAFKA_PORT) + ":" +
                        compose.getServicePort(KAFKA_SERVICE, KAFKA_PORT));
        registry.add("spring.data.redis.host", () ->
                compose.getServiceHost(REDIS_SERVICE, REDIS_PORT));
        registry.add("spring.data.redis.port", () ->
                compose.getServicePort(REDIS_SERVICE, REDIS_PORT));
        registry.add("rocketmq.enabled", () -> "false");
        registry.add("rocketmq.name-server", () -> "localhost:9876");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private BetRepository betRepository;

    private String betWinnerId;
    private String betLoserId;

    @BeforeEach
    void setUp() {
        betRepository.deleteAll();

        betWinnerId = UUID.randomUUID().toString();
        betLoserId = UUID.randomUUID().toString();

        Bet winningBet = Bet.builder()
                .id(betWinnerId)
                .userId("user-acceptance-1")
                .eventId("event-acceptance-1")
                .eventMarketId("market-1")
                .eventWinnerId("team-alpha")
                .betAmount(new BigDecimal("100.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build();

        Bet losingBet = Bet.builder()
                .id(betLoserId)
                .userId("user-acceptance-2")
                .eventId("event-acceptance-1")
                .eventMarketId("market-1")
                .eventWinnerId("team-beta")
                .betAmount(new BigDecimal("75.00"))
                .status(BetStatus.PENDING)
                .createdAt(Instant.now())
                .build();

        betRepository.saveAll(List.of(winningBet, losingBet));
    }

    @Test
    void shouldSettleBetsEndToEndWhenEventOutcomePublished() {
        EventOutcomeRequest request = new EventOutcomeRequest()
                .eventId("event-acceptance-1")
                .eventName("Acceptance Test Match")
                .eventWinnerId("team-alpha");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/event-outcomes", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Wait for async processing: Kafka consume -> bet match -> mock RocketMQ -> settle
        await().atMost(30, SECONDS).untilAsserted(() -> {
            Bet winner = betRepository.findById(betWinnerId).orElseThrow();
            assertThat(winner.getStatus()).isEqualTo(BetStatus.WON);
            assertThat(winner.getSettledAt()).isNotNull();
        });

        await().atMost(30, SECONDS).untilAsserted(() -> {
            Bet loser = betRepository.findById(betLoserId).orElseThrow();
            assertThat(loser.getStatus()).isEqualTo(BetStatus.LOST);
            assertThat(loser.getSettledAt()).isNotNull();
        });
    }

    @Test
    void shouldReturn202ForValidRequest() {
        EventOutcomeRequest request = new EventOutcomeRequest()
                .eventId("event-no-bets")
                .eventName("No Bets Match")
                .eventWinnerId("team-z");

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/event-outcomes", request, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
    }
}
