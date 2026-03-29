package com.sporty.betting.integration;

import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.messaging.kafka.EventOutcomeKafkaProducer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class KafkaIntegrationTest {

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
    private EventOutcomeKafkaProducer kafkaProducer;

    @Test
    void shouldProduceAndConsumeEventOutcome() {
        EventOutcome outcome = EventOutcome.builder()
                .eventId("event-integration-1")
                .eventName("Integration Test Match")
                .eventWinnerId("team-x")
                .build();

        kafkaProducer.send(outcome);

        String bootstrapServers = compose.getServiceHost(KAFKA_SERVICE, KAFKA_PORT) + ":" +
                compose.getServicePort(KAFKA_SERVICE, KAFKA_PORT);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(
                bootstrapServers, "test-verify-group", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.sporty.betting.domain.model");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventOutcome.class.getName());

        DefaultKafkaConsumerFactory<String, EventOutcome> cf =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        Consumer<String, EventOutcome> consumer = cf.createConsumer();
        consumer.subscribe(Collections.singletonList("event-outcomes"));

        ConsumerRecords<String, EventOutcome> records =
                consumer.poll(Duration.ofSeconds(10));

        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        EventOutcome received = records.iterator().next().value();
        assertThat(received.getEventId()).isEqualTo("event-integration-1");
        assertThat(received.getEventWinnerId()).isEqualTo("team-x");

        consumer.close();
    }
}
