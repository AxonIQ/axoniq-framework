package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.step.StepStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.StringUtils;
import org.axonframework.messaging.core.QualifiedName;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DefaultEventNameCustomizer implements EventNameCustomizer {

  private final Map<StepStatus, String> stepStatusToName = new HashMap<>();
  private final Map<WorkflowStatus, String> workflowStatusToName = new HashMap<>();
  private String namespace = "io.axoniq.workflow";
  private String baseName = null;
  private boolean appendToBaseName = true;
  private boolean capitalizeSimpleName = true;
  private Function<PayloadCustomization, QualifiedName> payloadCustomization = pc -> new QualifiedName(pc.namespaceTemplate, pc.localNameTemplate);

  public record PayloadCustomization(
    Map<String, Object> payload,
    String status,
    String namespaceTemplate,
    String localNameTemplate
  ) {
  }

  public static class Builder {

    public static DefaultEventNameCustomizer eventName() {
      return new DefaultEventNameCustomizer();
    }

    public static DefaultEventNameCustomizer baseName(String baseName) {
      return eventName().baseName(baseName);
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

    public static DefaultEventNameCustomizer payloadCustomization(Function<PayloadCustomization, QualifiedName> payloadCustomization) {
      return eventName().payloadCustomization(payloadCustomization);
    }

    public static EventNameCustomizer merge(@Nonnull EventNameCustomizer parent, @Nonnull EventNameCustomizer child) {
      var defaultCustomizer = new DefaultEventNameCustomizer();
      return new EventNameCustomizer() {
        @NotNull
        @Override
        public QualifiedName getEventName(@NotNull String stepName, @NotNull Map<String, Object> parameters, @NotNull StepStatus stepStatus) {
          var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
          String resultingNamespace = defaultName.namespace();
          String resultingName = defaultName.localName();
          var parentName = parent.getEventName(stepName, parameters, stepStatus);
          if (parentName.namespace() != null && !parentName.namespace().equals(defaultName.namespace())) {
            resultingNamespace = parentName.namespace();
          }
          if (!parentName.localName().equals(defaultName.localName())) {
            resultingName = parentName.localName();
          }
          var childName = child.getEventName(stepName, parameters, stepStatus);
          if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
            resultingNamespace = childName.namespace();
          }
          if (!childName.localName().equals(defaultName.localName())) {
            resultingName = childName.localName();
          }
          return new QualifiedName(resultingNamespace, resultingName);
        }

        @NotNull
        @Override
        public QualifiedName getEventName(@NotNull String stepName, @NotNull Map<String, Object> parameters, @NotNull WorkflowStatus stepStatus) {
          var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
          String resultingNamespace = defaultName.namespace();
          String resultingName = defaultName.localName();
          var parentName = parent.getEventName(stepName, parameters, stepStatus);
          if (parentName.namespace() != null && !parentName.namespace().equals(defaultName.namespace())) {
            resultingNamespace = parentName.namespace();
          }
          if (!parentName.localName().equals(defaultName.localName())) {
            resultingName = parentName.localName();
          }
          var childName = child.getEventName(stepName, parameters, stepStatus);
          if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
            resultingNamespace = childName.namespace();
          }
          if (!childName.localName().equals(defaultName.localName())) {
            resultingName = childName.localName();
          }
          return new QualifiedName(resultingNamespace, resultingName);
        }
      };
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

  public DefaultEventNameCustomizer namespace(String namespace) {
    this.namespace = namespace;
    return this;
  }

  public DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
    this.appendToBaseName = appendToBaseName;
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

  public DefaultEventNameCustomizer payloadCustomization(Function<PayloadCustomization, QualifiedName> payloadCustomization) {
    this.payloadCustomization = payloadCustomization;
    return this;
  }

  @Override
  @Nonnull
  public QualifiedName getEventName(
    @Nonnull String stepName,
    @Nonnull Map<String, Object> parameters,
    @Nonnull StepStatus stepStatus
  ) {
    String namespaceTemplate = namespace != null ? namespace : "";
    final StringBuilder eventNameTemplate = new StringBuilder();
    if (appendToBaseName) {
      eventNameTemplate
        .append(capitalize(baseName != null ? baseName : stepName));
    }
    eventNameTemplate
      .append(Objects.requireNonNull(stepStatusToName.get(stepStatus)));

    return payloadCustomization.apply(
      new PayloadCustomization(parameters, stepStatus.name(), namespaceTemplate, eventNameTemplate.toString())
    );
  }

  @Override
  @Nonnull
  public QualifiedName getEventName(
    @Nonnull String stepName,
    @Nonnull Map<String, Object> parameters,
    @Nonnull WorkflowStatus workflowStatus
  ) {
    String namespaceTemplate = (namespace != null ? (namespace.endsWith(".") ? namespace : namespace + ".") : "");
    final StringBuilder eventNameTemplate = new StringBuilder();
    if (appendToBaseName) {
      eventNameTemplate
        .append(capitalize(baseName != null ? baseName : stepName));
    }
    eventNameTemplate
      .append(Objects.requireNonNull(workflowStatusToName.get(workflowStatus)));

    return payloadCustomization.apply(
      new PayloadCustomization(parameters, workflowStatus.name(), namespaceTemplate, eventNameTemplate.toString())
    );
  }

  private String capitalize(String string) {
    if (capitalizeSimpleName) {
      return StringUtils.capitalize(string);
    } else {
      return string;
    }
  }
}
