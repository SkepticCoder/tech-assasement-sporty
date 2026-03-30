package com.sporty.betting.messaging.kafka.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.domain.service.BetMatchingService;
import com.sporty.betting.messaging.kafka.dlq.DeadLetterQueueProducer;
import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

/**
 * Kafka Streams topology for bet settlement event processing. Processes event outcomes from Kafka
 * and triggers bet matching and settlement.
 *
 * <p>Enabled via: settlement.kafka-streams-enabled=true
 */
@Slf4j
@Configuration
@EnableKafkaStreams
@ConditionalOnProperty(name = "settlement.strategy", havingValue = "KAFKA_STREAMS")
@RequiredArgsConstructor
public class BetSettlementStreamsConfiguration {

  private final BetMatchingService betMatchingService;
  private final ObjectMapper objectMapper;
  private final DeadLetterQueueProducer dlqProducer;

  private static final String EVENT_OUTCOMES_TOPIC = "event-outcomes";

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kafkaStreamsConfiguration(
      @Value("${spring.kafka.streams.application-id}") String applicationId,
      @Value("${spring.kafka.streams.bootstrap-servers}") String bootstrapServers,
      @Value("${spring.kafka.streams.state-dir}") String stateDir) {
    Map<String, Object> props = new HashMap<>();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
    props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    return new KafkaStreamsConfiguration(props);
  }

  /**
   * Build the Kafka Streams topology for bet settlement processing.
   *
   * <p>Flow:
   *
   * <ol>
   *   <li>Consume EventOutcome messages from event-outcomes topic
   *   <li>Process each event: match bets and trigger settlement via RocketMQ
   * </ol>
   *
   * @param streamsBuilder the StreamsBuilder for building the topology
   */
  @Bean
  public KStream<String, EventOutcome> eventOutcomeStream(StreamsBuilder streamsBuilder) {
    log.info("Initializing Kafka Streams topology for bet settlement processing");

    // Create custom Serde for EventOutcome using Jackson
    var eventOutcomeSerde =
        Serdes.serdeFrom(
            new Serializer<EventOutcome>() {
              @Override
              public byte[] serialize(String topic, EventOutcome data) {
                try {
                  return objectMapper.writeValueAsBytes(data);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            },
            new Deserializer<EventOutcome>() {
              @Override
              public EventOutcome deserialize(String topic, byte[] data) {
                try {
                  return objectMapper.readValue(data, EventOutcome.class);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });

    KStream<String, EventOutcome> stream =
        streamsBuilder.stream(
            EVENT_OUTCOMES_TOPIC,
            org.apache.kafka.streams.kstream.Consumed.with(Serdes.String(), eventOutcomeSerde));

    stream
        .peek(
            (key, value) ->
                log.info(
                    "Processing event outcome from stream: eventId={}, winner={}",
                    value.getEventId(),
                    value.getEventWinnerId()))
        .foreach(
            (key, value) -> {
              try {
                betMatchingService.matchAndSettle(value);
                log.debug("Successfully processed event outcome: eventId={}", value.getEventId());
              } catch (Exception e) {
                log.error(
                    "Error processing event outcome in Kafka Streams: eventId={}",
                    value.getEventId(),
                    e);
                // Send to DLQ for failed events
                dlqProducer.sendToDLQ(value, e, 0, "KAFKA_STREAMS");
              }
            });

    return stream;
  }
}
