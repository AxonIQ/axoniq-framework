package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.runtime.api.workflow.WorkflowContext;

import java.util.LinkedHashMap;
import java.util.Map;

public class Payload {

  private final Map<String, Object> payload;

  public static Payload payload(Map<String, Object> payload) {
    return new Payload(payload);
  }

  public static Payload payload() {
    return payload(new LinkedHashMap<>());
  }

  public static Payload payload(WorkflowContext context) {
    return payload(context.getPayload());
  }

  public static Payload payload(String key, Object value) {
    return payload().with(key, value);
  }

  public static Payload payload(String key, Object value, String key2, Object value2) {
    return payload(key, value).with(key2, value2);
  }

  public static Payload payload(String key, Object value, String key2, Object value2, String key3, Object value3) {
    return payload(key, value, key2, value2).with(key3, value3);
  }

  public Payload(Map<String, Object> payload) {
    this.payload = payload;
  }

  public <T> T get(String key) {
    //noinspection unchecked
    return (T) payload.get(key);
  }

  public Payload set(String key, Object value) {
    payload.put(key, value);
    return this;
  }

  public Payload with(String key, Object value) {
    return set(key, value);
  }

  public Map<String, Object> getValues() {
    return payload;
  }

}
