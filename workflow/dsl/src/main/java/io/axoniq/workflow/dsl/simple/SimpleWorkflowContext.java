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

package io.axoniq.workflow.dsl.simple;

import io.axoniq.workflow.dsl.Payload;
import io.axoniq.workflow.runtime.api.*;
import io.axoniq.workflow.runtime.engine.impl.WorkflowInstance;
import jakarta.annotation.Nonnull;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.axoniq.workflow.dsl.Payload.payload;
import static io.axoniq.workflow.runtime.api.PayloadReducer.all;
import static io.axoniq.workflow.runtime.api.PayloadReducer.local;
import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName;


public class SimpleWorkflowContext extends WorkflowInstance
  implements WaitForPrimitive, ExecutePrimitive {

  public SimpleWorkflowContext(
    @Nonnull String workflowId,
    @Nonnull Map<String, Object> payload,
    @Nonnull ProcessingContext processingContext,
    @Nonnull EventNameCustomizer parentCustomizer,
    @Nonnull WorkflowServices workflowServices
  ) {
    super(workflowId, payload, processingContext, parentCustomizer, workflowServices);
  }

  public <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(eventType), e -> predicate.test(e.payloadAs(eventType)), timeout, eventNameCustomizer);
    return fromResult(result, eventType);
  }

  public <T> T waitForEvent(String stepName, Class<T> eventType, Predicate<T> predicate, Duration timeout) {
    var result = waitFor(stepName, new QualifiedName(eventType), e -> predicate.test(e.payloadAs(eventType)), timeout, eventName());
    return fromResult(result, eventType);
  }

  public <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(eventType), (e) -> true, timeout, eventNameCustomizer);
    return fromResult(result, eventType);
  }

  public <T> T waitForEvent(String stepName, Class<T> eventType, Duration timeout) {
    var result = waitFor(stepName, new QualifiedName(eventType), (e) -> true, timeout, eventName());
    return fromResult(result, eventType);
  }

  public <T> T waitForEvent(String stepName, Class<T> eventType) {
    return this.waitForEvent(stepName, eventType, Duration.ofSeconds(5)); // TODO default
  }

  public void wait(String stepName, Duration timeout, EventNameCustomizer eventNameCustomizer) {
    var result = waitFor(stepName, new QualifiedName(Void.class), (e) -> false, timeout, eventNameCustomizer);
    if (result.isFailure()) {
      throw result.error().get();
    }
  }

  public void wait(String stepName, Duration timeout) {
    wait(stepName, timeout, eventName());
  }

  public WorkflowStepResult executeWithResult(String stepName, Map<String, Object> payload, PayloadProcessor action, Duration duration) {
    return execute(stepName, payload, action, local(), all(), duration, eventName());
  }

  public Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action, EventNameCustomizer eventNameCustomizer) {
    var result = execute(stepName, payload, action, local(), all(), Duration.ofSeconds(5), eventNameCustomizer);
    return fromResult(result);
  }

  public Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action, Duration timeout) {
    var result = execute(stepName, payload, action, local(), all(), timeout, eventName());
    return fromResult(result);
  }

  public <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action, EventNameCustomizer eventNameCustomizer) {
    var stepSpecificName = "__" + stepName;
    var result = execute(
      stepName,
      payload,
      (c, p) -> {
        var stepResult = action.apply(p);
        if (stepResult != null) {
          return Map.of(stepSpecificName, stepResult);
        } else {
          return Map.of();
        }
      },
      local(),
      all(),
      Duration.ofSeconds(5),
      eventNameCustomizer
    );
    //noinspection unchecked
    return (T) fromResult(result).get(stepSpecificName);
  }

  // taskqueue overloads

  public Map<String, Object> execute(String stepName, Map<String, Object> payload, PayloadProcessor action) {
    return this.execute(stepName, payload, action, Duration.ofSeconds(5));
  }

  public Payload execute(String stepName, Payload payload, Function<Payload, Payload> action, EventNameCustomizer eventNameCustomizer) {
    return payload(this.execute(stepName, payload.getValues(), (c, p) -> action.apply(payload(p)).getValues(), eventNameCustomizer));
  }

  public Payload execute(String stepName, Payload payload, Function<Payload, Payload> action) {
    return payload(this.execute(stepName, payload.getValues(), (c, p) -> action.apply(payload(p)).getValues()));
  }

  public <T> T execute(String stepName, Map<String, Object> payload, Class<T> returnType, Function<Map<String, Object>, T> action) {
    return this.execute(stepName, payload, returnType, action, eventName());
  }

  public <T> T execute(String stepName, Class<T> returnType, Supplier<T> action) {
    return this.execute(stepName, Map.of(), returnType, (p) -> action.get());
  }

  public <T> T execute(String stepName, Class<T> returnType, Supplier<T> action, EventNameCustomizer eventNameCustomizer) {
    return this.execute(stepName, Map.of(), returnType, (p) -> action.get(), eventNameCustomizer);
  }

  public void execute(String stepName, Payload payload, Consumer<Payload> action, EventNameCustomizer eventNameCustomizer) {
    execute(stepName, payload, (p) -> {
      action.accept(p);
      return payload();
    }, eventNameCustomizer);
  }

  public void execute(String stepName, Payload payload, Consumer<Payload> action) {
    this.execute(stepName, payload, (p) -> {
      action.accept(p);
      return payload();
    });
  }

  public void execute(String stepName, Runnable action, EventNameCustomizer eventNameCustomizer) {
    this.execute(stepName, Void.class, () -> {
      action.run();
      return null;
    }, eventNameCustomizer);
  }

  public void execute(String stepName, Runnable action) {
    this.execute(stepName, Void.class, () -> {
      action.run();
      return null;
    });
  }

  public <T> T execute(String stepName, Payload payload, Class<T> returnType, Function<Payload, T> action) {
    return this.execute(stepName, payload.getValues(), returnType, (m) -> action.apply(payload(m)));
  }

  public void addPayload(Object object) {
    addPayload(payload(this, object));
  }

  public void addPayload(Payload payload) {
    applyPayloadModification(p -> payload(p).with(payload).getValues());
  }

  <T> T fromResult(WorkflowStepResult result, Class<T> eventType) {
    if (result.isSuccess() && result.payload().isPresent()) {
      return super.processingContext().component(Converter.class)
        .convert(result.payload().get(), eventType);
    } else {
      throw result.error().orElseThrow();
    }
  }

  Map<String, Object> fromResult(WorkflowStepResult result) {
    if (result.isSuccess() && result.<Map<String, Object>>payload().isPresent()) {
      return result.<Map<String, Object>>payload().get();
    } else {
      throw result.error().orElseThrow();
    }
  }

}
