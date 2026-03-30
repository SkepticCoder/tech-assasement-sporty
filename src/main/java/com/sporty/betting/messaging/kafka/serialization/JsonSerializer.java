package com.sporty.betting.messaging.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Generic JSON Serializer for Kafka messages using Jackson ObjectMapper. Replaces the deprecated
 * Spring Kafka JsonSerializer.
 */
@RequiredArgsConstructor
public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper;

  public JsonSerializer() {
    this(new ObjectMapper());
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }
    try {
      return objectMapper.writeValueAsString(data).getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize object", e);
    }
  }
}
