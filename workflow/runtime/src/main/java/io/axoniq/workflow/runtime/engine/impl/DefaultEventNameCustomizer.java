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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.engine.impl;

import io.axoniq.workflow.runtime.api.EventNameCustomizer;
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
    private String workflowBaseName = null;
    private boolean appendToBaseName = true;
    private boolean capitalizeSimpleName = true;
    private Function<PayloadCustomization, QualifiedName> payloadCustomization = pc -> new QualifiedName(pc.namespaceTemplate,
                                                                                                         pc.localNameTemplate);

    DefaultEventNameCustomizer() {
        stepStarted("Started");
        stepCompleted("Completed");
        stepTimedOut("TimedOut");
        stepFailed("Failed");
        stepCancelled("Cancelled");

        workflowStarted("Started");
        workflowCompleted("Completed");
        workflowTimedOut("TimedOut");
        workflowFailed("Failed");
        workflowCancelled("Cancelled");
    }

    public DefaultEventNameCustomizer baseName(String baseName) {
        this.baseName = baseName;
        return this;
    }

    public DefaultEventNameCustomizer workflowBaseName(String workflowBaseName) {
        this.workflowBaseName = workflowBaseName;
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

    public DefaultEventNameCustomizer stepCancelled(String cancelled) {
        this.stepStatusToName.put(StepStatus.CANCELLED, cancelled);
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

    public DefaultEventNameCustomizer workflowCancelled(String cancelled) {
        this.workflowStatusToName.put(WorkflowStatus.CANCELLED, cancelled);
        return this;
    }

    public DefaultEventNameCustomizer payloadCustomization(
            Function<PayloadCustomization, QualifiedName> payloadCustomization) {
        this.payloadCustomization = payloadCustomization;
        return this;
    }

    @Override
    @Nonnull
    public EventNameCustomizer forStepInheritance() {
        var inheritable = new DefaultEventNameCustomizer();
        inheritable.namespace(this.namespace);
        inheritable.capitalizeSimpleName(this.capitalizeSimpleName);
        inheritable.stepStatusToName.putAll(this.stepStatusToName);
        // baseName, workflowBaseName, appendToBaseName, workflowStatusToName, payloadCustomization are NOT copied
        return inheritable;
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
        String namespaceTemplate = namespace != null ? namespace : "";
        final StringBuilder eventNameTemplate = new StringBuilder();
        if (appendToBaseName) {
            eventNameTemplate
                    .append(capitalize(workflowBaseName != null ? workflowBaseName : stepName));
        }
        eventNameTemplate
                .append(Objects.requireNonNull(workflowStatusToName.get(workflowStatus)));

        return payloadCustomization.apply(
                new PayloadCustomization(parameters,
                                         workflowStatus.name(),
                                         namespaceTemplate,
                                         eventNameTemplate.toString())
        );
    }

    private String capitalize(String string) {
        if (capitalizeSimpleName) {
            return StringUtils.capitalize(string);
        } else {
            return string;
        }
    }

    public record PayloadCustomization(
            Map<String, Object> payload,
            String status,
            String namespaceTemplate,
            String localNameTemplate
    ) {

    }

    public static class Builder {

        public static DefaultEventNameCustomizer defaults() {
            return new DefaultEventNameCustomizer();
        }

        public static DefaultEventNameCustomizer baseName(String baseName) {
            return defaults().baseName(baseName);
        }

        public static DefaultEventNameCustomizer workflowBaseName(String workflowBaseName) {
            return defaults().workflowBaseName(workflowBaseName);
        }

        public static DefaultEventNameCustomizer namespace(String namespace) {
            return defaults().namespace(namespace);
        }

        public static DefaultEventNameCustomizer stepCompleted(String completed) {
            return defaults().stepCompleted(completed);
        }

        public static DefaultEventNameCustomizer stepStarted(String started) {
            return defaults().stepStarted(started);
        }

        public static DefaultEventNameCustomizer stepFailed(String failed) {
            return defaults().stepFailed(failed);
        }

        public static DefaultEventNameCustomizer stepTimedOut(String timedOut) {
            return defaults().stepTimedOut(timedOut);
        }

        public static DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
            return defaults().appendToBaseName(appendToBaseName);
        }

        public static DefaultEventNameCustomizer capitalizeSimpleName(boolean capitalizeSimpleName) {
            return defaults().capitalizeSimpleName(capitalizeSimpleName);
        }

        public static DefaultEventNameCustomizer payloadCustomization(
                Function<PayloadCustomization, QualifiedName> payloadCustomization) {
            return defaults().payloadCustomization(payloadCustomization);
        }

        public static EventNameCustomizer merge(@Nonnull EventNameCustomizer parent,
                                                @Nonnull EventNameCustomizer child) {
            var defaultCustomizer = new DefaultEventNameCustomizer();
            return new EventNameCustomizer() {
                @NotNull
                @Override
                public QualifiedName getEventName(@NotNull String stepName, @NotNull Map<String, Object> parameters,
                                                  @NotNull StepStatus stepStatus) {
                    var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
                    var parentName = parent.getEventName(stepName, parameters, stepStatus);
                    var childName = child.getEventName(stepName, parameters, stepStatus);

                    String resultingNamespace = defaultName.namespace();
                    if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = childName.namespace();
                    } else if (parentName.namespace() != null && !parentName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = parentName.namespace();
                    }

                    String resultingName = defaultName.localName();
                    if (!childName.localName().equals(defaultName.localName())) {
                        resultingName = childName.localName();
                    } else if (!parentName.localName().equals(defaultName.localName())) {
                        resultingName = parentName.localName();
                    }

                    return new QualifiedName(resultingNamespace, resultingName);
                }

                @NotNull
                @Override
                public QualifiedName getEventName(@NotNull String stepName, @NotNull Map<String, Object> parameters,
                                                  @NotNull WorkflowStatus stepStatus) {
                    var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
                    var parentName = parent.getEventName(stepName, parameters, stepStatus);
                    var childName = child.getEventName(stepName, parameters, stepStatus);

                    String resultingNamespace = defaultName.namespace();
                    if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = childName.namespace();
                    } else if (parentName.namespace() != null && !parentName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = parentName.namespace();
                    }

                    String resultingName = defaultName.localName();
                    if (!childName.localName().equals(defaultName.localName())) {
                        resultingName = childName.localName();
                    } else if (!parentName.localName().equals(defaultName.localName())) {
                        resultingName = parentName.localName();
                    }

                    return new QualifiedName(resultingNamespace, resultingName);
                }

                @NotNull
                @Override
                public EventNameCustomizer forStepInheritance() {
                    return this;
                }
            };
        }
    }
}
