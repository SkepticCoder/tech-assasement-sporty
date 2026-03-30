package com.sporty.betting.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sporty.betting.domain.model.EventOutcome;
import com.sporty.betting.messaging.kafka.dlq.FailedEventOutcome;
import com.sporty.betting.messaging.kafka.serialization.JsonDeserializer;
import com.sporty.betting.messaging.kafka.serialization.JsonSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapServers;

  @Value("${spring.kafka.consumer.group-id}")
  private String groupId;

  @Value("${spring.kafka.producer.transaction-id-prefix}")
  private String transactionIdPrefix;

  @Bean
  public ProducerFactory<String, EventOutcome> producerFactory(ObjectMapper objectMapper) {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);

    DefaultKafkaProducerFactory<String, EventOutcome> factory =
        new DefaultKafkaProducerFactory<>(props);
    factory.setValueSerializer(new JsonSerializer<>(objectMapper));
    factory.setTransactionIdPrefix(transactionIdPrefix);
    return factory;
  }

  @Bean
  public KafkaTemplate<String, EventOutcome> kafkaTemplate(
      ProducerFactory<String, EventOutcome> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public ProducerFactory<String, FailedEventOutcome> dlqProducerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 3);
    return new DefaultKafkaProducerFactory<>(props);
  }

  @Bean
  public KafkaTemplate<String, FailedEventOutcome> dlqKafkaTemplate(
      ProducerFactory<String, FailedEventOutcome> dlqProducerFactory) {
    return new KafkaTemplate<>(dlqProducerFactory);
  }

  @Bean
  public ConsumerFactory<String, EventOutcome> consumerFactory(ObjectMapper objectMapper) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    DefaultKafkaConsumerFactory<String, EventOutcome> factory =
        new DefaultKafkaConsumerFactory<>(props);
    factory.setValueDeserializer(new JsonDeserializer<>(objectMapper, EventOutcome.class));

    return factory;
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, EventOutcome>
      kafkaListenerContainerFactory(ConsumerFactory<String, EventOutcome> consumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, EventOutcome> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }

  @Bean
  public ConsumerFactory<String, FailedEventOutcome> dlqConsumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    return new DefaultKafkaConsumerFactory<>(props);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, FailedEventOutcome>
      dlqKafkaListenerContainerFactory(
          ConsumerFactory<String, FailedEventOutcome> dlqConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<String, FailedEventOutcome> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(dlqConsumerFactory);
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    return factory;
  }
}
