/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.dsl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import org.axonframework.conversion.Converter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class Payload {

  private final Map<String, Object> payload;

  public static Payload payload(WorkflowContext context, Object value) {
    Map<String, Object> map = context.processingContext().component(Converter.class).convert(
      value, new TypeReference<Map<String, Object>>() {
      }.getType());
    return payload(map);
  }

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

  public <T> T getPayloadAs(String key, Function<Map<String, Object>, T> converter) {
    Object value = payload.get(key);
    if (value instanceof Map) {
      //noinspection unchecked
      return (T) converter.apply((Map<String, Object>) value);
    }
    return (T) value;
  }


  public Payload set(String key, Object value) {
    payload.put(key, value);
    return this;
  }

  public Payload with(Payload other) {
    payload.putAll(other.getValues());
    return this;
  }

  public Payload with(String key, Object value) {
    return set(key, value);
  }

  public Map<String, Object> getValues() {
    return payload;
  }

}
