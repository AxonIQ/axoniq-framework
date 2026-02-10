package io.axoniq.workflow.runtime.engine.step;

import org.axonframework.messaging.core.Context;

import java.time.Instant;

public record StepExecution(
  String stepName,
  StepStatus status,
  Object result,
  Throwable error,
  Instant timestamp,
  Context context
) {
  public static StepExecution started(String name, Object parameters, Instant timestamp, Context context) {
    return new StepExecution(name, StepStatus.STARTED, parameters, null, timestamp, context);
  }

  public static StepExecution completed(String name, Object result, Instant timestamp, Context context) {
    return new StepExecution(name, StepStatus.COMPLETED, result, null, timestamp, context);
  }

  public static StepExecution failed(String name, Throwable error, Instant timestamp, Context context) {
    return new StepExecution(name, StepStatus.FAILED, null, error, timestamp, context);
  }

  public static StepExecution timedOut(String name, Object payload, Instant timestamp, Context context) {
    return new StepExecution(name, StepStatus.TIMED_OUT, payload, null, timestamp, context);
  }

  public static StepExecution cancelled(String name, Instant timestamp, Context context) {
    return new StepExecution(name, StepStatus.CANCELLED, null, null, timestamp, context);
  }
}
