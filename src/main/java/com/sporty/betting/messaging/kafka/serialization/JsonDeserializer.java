package com.sporty.betting.messaging.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Generic JSON Deserializer for Kafka messages using Jackson ObjectMapper. Replaces the deprecated
 * Spring Kafka JsonDeserializer.
 */
@RequiredArgsConstructor
public class JsonDeserializer<T> implements Deserializer<T> {

  private final ObjectMapper objectMapper;
  private final Class<T> targetType;

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null || data.length == 0) {
      return null;
    }
    try {
      String json = new String(data, StandardCharsets.UTF_8);
      return objectMapper.readValue(json, targetType);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize object", e);
    }
  }
}
