package io.axoniq.workflow.runtime.context;

import io.axoniq.workflow.runtime.api.primitives.EventNameCustomizer;
import io.axoniq.workflow.runtime.engine.StepStatus;
import org.axonframework.common.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class DefaultEventNameCustomizer implements EventNameCustomizer {

  private final Map<StepStatus, String> stepStatusToName = new HashMap<>();
  private String namespace = "io.axoniq.workflow";
  private String baseName = null;
  private String baseVersion = "#0.1";
  private boolean appendToBaseName = true;
  private boolean appendVersion = true;
  private boolean capitalizeSimpleName = true;
  private Function<PayloadCustomization, String> payloadCustomization = (pc) -> pc.template;

  public record PayloadCustomization(
    Map<String, Object> payload,
    StepStatus status,
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

    public static DefaultEventNameCustomizer completed(String completed) {
      return eventName().completed(completed);
    }

    public static DefaultEventNameCustomizer started(String started) {
      return eventName().started(started);
    }

    public static DefaultEventNameCustomizer failed(String failed) {
      return eventName().failed(failed);
    }

    public static DefaultEventNameCustomizer timedOut(String timedOut) {
      return eventName().timedOut(timedOut);
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
    started("Started");
    completed("Completed");
    timedOut("TimedOut");
    failed("Failed");
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

  public DefaultEventNameCustomizer completed(String completed) {
    this.stepStatusToName.put(StepStatus.COMPLETED, completed);
    return this;
  }

  public DefaultEventNameCustomizer started(String started) {
    this.stepStatusToName.put(StepStatus.STARTED, started);
    return this;
  }

  public DefaultEventNameCustomizer timedOut(String timedOut) {
    this.stepStatusToName.put(StepStatus.TIMED_OUT, timedOut);
    return this;
  }

  public DefaultEventNameCustomizer failed(String failed) {
    this.stepStatusToName.put(StepStatus.FAILED, failed);
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

    return payloadCustomization.apply(new PayloadCustomization(parameters, stepStatus, eventNameTemplate.toString()));
  }

  private String capitalize(String string) {
    if (capitalizeSimpleName) {
      return StringUtils.capitalize(string);
    } else {
      return string;
    }
  }
}
