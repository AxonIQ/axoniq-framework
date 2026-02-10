package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import org.axonframework.common.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

// TODO -> allow to have this on the workflow level
// FIXME: make sure we use MessageType as return
public class DefaultEventNameCustomizer implements EventNameCustomizer {

  private final Map<StepStatus, String> stepStatusToName = new HashMap<>();
  private final Map<WorkflowStatus, String> workflowStatusToName = new HashMap<>();
  private String namespace = "io.axoniq.workflow";
  private String baseName = null;
  private String baseVersion = "#0.0.1";
  private boolean appendToBaseName = true;
  private boolean appendVersion = true;
  private boolean capitalizeSimpleName = true;
  private Function<PayloadCustomization, String> payloadCustomization = (pc) -> pc.template;

  public record PayloadCustomization(
    Map<String, Object> payload,
    String status,
    String template
  ) {
  }

  public static class Builder {

    public static DefaultEventNameCustomizer eventName() {
      return new DefaultEventNameCustomizer();
    }

    public static DefaultEventNameCustomizer baseName(String baseName) {
      return eventName().baseName(baseName);
    }

    public static DefaultEventNameCustomizer baseVersion(String baseVersion) {
      return eventName().baseVersion(baseVersion);
    }

    public static DefaultEventNameCustomizer namespace(String namespace) {
      return eventName().namespace(namespace);
    }

    public static DefaultEventNameCustomizer stepCompleted(String completed) {
      return eventName().stepCompleted(completed);
    }

    public static DefaultEventNameCustomizer stepStarted(String started) {
      return eventName().stepStarted(started);
    }

    public static DefaultEventNameCustomizer stepFailed(String failed) {
      return eventName().stepFailed(failed);
    }

    public static DefaultEventNameCustomizer stepTimedOut(String timedOut) {
      return eventName().stepTimedOut(timedOut);
    }

    public static DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
      return eventName().appendToBaseName(appendToBaseName);
    }

    public static DefaultEventNameCustomizer capitalizeSimpleName(boolean capitalizeSimpleName) {
      return eventName().capitalizeSimpleName(capitalizeSimpleName);
    }

    public static DefaultEventNameCustomizer appendVersion(boolean appendVersion) {
      return eventName().appendVersion(appendVersion);
    }

    public static DefaultEventNameCustomizer payloadCustomization(Function<PayloadCustomization, String> payloadCustomization) {
      return eventName().payloadCustomization(payloadCustomization);
    }
  }

  DefaultEventNameCustomizer() {
    stepStarted("Started");
    stepCompleted("Completed");
    stepTimedOut("TimedOut");
    stepFailed("Failed");

    workflowStarted("Started");
    workflowCompleted("Completed");
    workflowTimedOut("TimedOut");
    workflowFailed("Failed");
  }

  public DefaultEventNameCustomizer baseName(String baseName) {
    this.baseName = baseName;
    return this;
  }

  public DefaultEventNameCustomizer baseVersion(String baseVersion) {
    this.baseVersion = baseVersion;
    return this;
  }

  public DefaultEventNameCustomizer namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
    this.appendToBaseName = appendToBaseName;
    return this;
  }

  public DefaultEventNameCustomizer appendVersion(boolean appendVersion) {
    this.appendVersion = appendVersion;
    return this;
  }

  public DefaultEventNameCustomizer capitalizeSimpleName(boolean capitalizeSimpleName) {
    this.capitalizeSimpleName = capitalizeSimpleName;
    return this;
  }

  public DefaultEventNameCustomizer stepCompleted(String completed) {
    this.stepStatusToName.put(StepStatus.COMPLETED, completed);
    return this;
  }

  public DefaultEventNameCustomizer stepStarted(String started) {
    this.stepStatusToName.put(StepStatus.STARTED, started);
    return this;
  }

  public DefaultEventNameCustomizer stepTimedOut(String timedOut) {
    this.stepStatusToName.put(StepStatus.TIMED_OUT, timedOut);
    return this;
  }

  public DefaultEventNameCustomizer stepFailed(String failed) {
    this.stepStatusToName.put(StepStatus.FAILED, failed);
    return this;
  }

  public DefaultEventNameCustomizer workflowCompleted(String completed) {
    this.workflowStatusToName.put(WorkflowStatus.COMPLETED, completed);
    return this;
  }

  public DefaultEventNameCustomizer workflowStarted(String started) {
    this.workflowStatusToName.put(WorkflowStatus.STARTED, started);
    return this;
  }

  public DefaultEventNameCustomizer workflowTimedOut(String timedOut) {
    this.workflowStatusToName.put(WorkflowStatus.TIMED_OUT, timedOut);
    return this;
  }

  public DefaultEventNameCustomizer workflowFailed(String failed) {
    this.workflowStatusToName.put(WorkflowStatus.FAILED, failed);
    return this;
  }

  public DefaultEventNameCustomizer payloadCustomization(Function<PayloadCustomization, String> payloadCustomization) {
    this.payloadCustomization = payloadCustomization;
    return this;
  }

  @Override
  public String getEventName(String stepName, Map<String, Object> parameters, StepStatus stepStatus) {
    final StringBuilder eventNameTemplate = new StringBuilder();
    if (appendToBaseName) {
      eventNameTemplate
        .append(namespace != null ? (namespace.endsWith(".") ? namespace : namespace + ".") : "")
        .append(capitalize(baseName != null ? baseName : stepName))
        .append(Objects.requireNonNull(stepStatusToName.get(stepStatus)));
    } else {
      eventNameTemplate
        .append(Objects.requireNonNull(stepStatusToName.get(stepStatus)));
    }
    if (appendVersion) {
      eventNameTemplate.append(baseVersion);
    }

    return payloadCustomization.apply(new PayloadCustomization(parameters, stepStatus.name(), eventNameTemplate.toString()));
  }

  @Override
  public String getEventName(String stepName, Map<String, Object> parameters, WorkflowStatus workflowStatus) {
    final StringBuilder eventNameTemplate = new StringBuilder();
    if (appendToBaseName) {
      eventNameTemplate
        .append(namespace != null ? (namespace.endsWith(".") ? namespace : namespace + ".") : "")
        .append(capitalize(baseName != null ? baseName : stepName))
        .append(Objects.requireNonNull(workflowStatusToName.get(workflowStatus)));
    } else {
      eventNameTemplate
        .append(Objects.requireNonNull(workflowStatusToName.get(workflowStatus)));
    }
    if (appendVersion) {
      eventNameTemplate.append(baseVersion);
    }

    return payloadCustomization.apply(new PayloadCustomization(parameters, workflowStatus.name(), eventNameTemplate.toString()));
  }

  private String capitalize(String string) {
    if (capitalizeSimpleName) {
      return StringUtils.capitalize(string);
    } else {
      return string;
    }
  }
}
