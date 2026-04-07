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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.runtime.api.execution.context.EventNameCustomizer;
import io.axoniq.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import jakarta.annotation.Nonnull;
import org.axonframework.common.StringUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Default implementation of {@link EventNameCustomizer} that builds event names from a base name and status suffix.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
 */
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

    /**
     * Constructs new event name customizer. Use {@link DefaultEventNameCustomizer} static factory methods instead.
     */
    DefaultEventNameCustomizer() {
        stepStarted("Started");
        stepRetrying("Retrying");
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

    /**
     * Sets the base name for the step.
     *
     * @param baseName base name of the step.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer baseName(String baseName) {
        this.baseName = baseName;
        return this;
    }

    /**
     * Sets the workflow base name.
     *
     * @param workflowBaseName base name for the workflow.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowBaseName(String workflowBaseName) {
        this.workflowBaseName = workflowBaseName;
        return this;
    }

    /**
     * Sets the namespace for the events.
     *
     * @param namespace namespace for the events.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer namespace(String namespace) {
        this.namespace = namespace;
        return this;
    }

    /**
     * Sets the flag controlling if the step name is a suffix or is the entire name of the event.
     *
     * @param appendToBaseName if true, the step name is considered as suffix.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
        this.appendToBaseName = appendToBaseName;
        return this;
    }

    /**
     * Sets the flag controlling if the simple name of the step is capitalized.
     *
     * @param capitalizeSimpleName if true, the first name of the status is capitalized.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer capitalizeSimpleName(boolean capitalizeSimpleName) {
        this.capitalizeSimpleName = capitalizeSimpleName;
        return this;
    }

    /**
     * Sets the step status suffix for retrying status.
     *
     * @param retrying suffix appended to the stap base name if it is in retrying status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepRetrying(String retrying) {
        this.stepStatusToName.put(StepStatus.RETRYING, retrying);
        return this;
    }

    /**
     * Sets the step status suffix for completed status.
     *
     * @param completed suffix appended to the stap base name if it is in completed status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepCompleted(String completed) {
        this.stepStatusToName.put(StepStatus.COMPLETED, completed);
        return this;
    }

    /**
     * Sets the step status suffix for started status.
     *
     * @param started suffix appended to the stap base name if it is in started status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepStarted(String started) {
        this.stepStatusToName.put(StepStatus.STARTED, started);
        return this;
    }

    /**
     * Sets the step status suffix for timed-out status.
     *
     * @param timedOut suffix appended to the stap base name if it is in timed-out status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepTimedOut(String timedOut) {
        this.stepStatusToName.put(StepStatus.TIMED_OUT, timedOut);
        return this;
    }

    /**
     * Sets the step status suffix for failed status.
     *
     * @param failed suffix appended to the stap base name if it is in failed status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepFailed(String failed) {
        this.stepStatusToName.put(StepStatus.FAILED, failed);
        return this;
    }

    /**
     * Sets the step status suffix for cancelled status.
     *
     * @param cancelled suffix appended to the stap base name if it is in cancelled status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer stepCancelled(String cancelled) {
        this.stepStatusToName.put(StepStatus.CANCELLED, cancelled);
        return this;
    }

    /**
     * Sets the workflow status suffix for completed status.
     *
     * @param completed suffix appended to the workflow base name if it is in completed status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowCompleted(String completed) {
        this.workflowStatusToName.put(WorkflowStatus.COMPLETED, completed);
        return this;
    }

    /**
     * Sets the workflow status suffix for started status.
     *
     * @param started suffix appended to the workflow base name if it is in started status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowStarted(String started) {
        this.workflowStatusToName.put(WorkflowStatus.STARTED, started);
        return this;
    }

    /**
     * Sets the workflow status suffix for timed-out status.
     *
     * @param timedOut suffix appended to the workflow base name if it is in timed-out status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowTimedOut(String timedOut) {
        this.workflowStatusToName.put(WorkflowStatus.TIMED_OUT, timedOut);
        return this;
    }

    /**
     * Sets the workflow status suffix for failed status.
     *
     * @param failed suffix appended to the workflow base name if it is in failed status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowFailed(String failed) {
        this.workflowStatusToName.put(WorkflowStatus.FAILED, failed);
        return this;
    }

    /**
     * Sets the workflow status suffix for cancelled status.
     *
     * @param cancelled suffix appended to the workflow base name if it is in cancelled status.
     * @return current instance for fluent API.
     */
    public DefaultEventNameCustomizer workflowCancelled(String cancelled) {
        this.workflowStatusToName.put(WorkflowStatus.CANCELLED, cancelled);
        return this;
    }

    /**
     * Sets the function to customize step based on the payload.
     *
     * @param payloadCustomization function to customize step based on the payload.
     * @return current instance for fluent API.
     */
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

    /**
     * Data class containing information for payload customization.
     *
     * @param payload           current step payload.
     * @param status            current status.
     * @param namespaceTemplate namespace template.
     * @param localNameTemplate local name template.
     */
    public record PayloadCustomization(
            Map<String, Object> payload,
            String status,
            String namespaceTemplate,
            String localNameTemplate
    ) {

    }

    /**
     * Builder for {@link DefaultEventNameCustomizer}.
     *
     * @author Stefan Dragisic
     * @author Simon Zambrovski
     * @since 1.0.0
     */
    public static class Builder {

        /**
         * Constructs default event name customizer.
         *
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer defaults() {
            return new DefaultEventNameCustomizer();
        }

        /**
         * Constructs default event name customizer with base name.
         *
         * @param baseName base name of the step.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer baseName(@Nonnull String baseName) {
            return defaults().baseName(baseName);
        }

        /**
         * Constructs default event name customizer with workflow base name.
         *
         * @param workflowBaseName base name for the workflow.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer workflowBaseName(@Nonnull String workflowBaseName) {
            return defaults().workflowBaseName(workflowBaseName);
        }

        /**
         * Constructs default event name customizer with namespace for the events.
         *
         * @param namespace namespace for the events.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer namespace(@Nonnull String namespace) {
            return defaults().namespace(namespace);
        }

        /**
         * Constructs default event name customizer with step status suffix for retrying status.
         *
         * @param retrying suffix appended to the stap base name if it is in retrying status.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer stepRetrying(@Nonnull String retrying) {
            return defaults().stepRetrying(retrying);
        }

        /**
         * Constructs default event name customizer with step status suffix for completed status.
         *
         * @param completed suffix appended to the stap base name if it is in completed status.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer stepCompleted(@Nonnull String completed) {
            return defaults().stepCompleted(completed);
        }

        /**
         * Constructs default event name customizer with step status suffix for started status.
         *
         * @param started suffix appended to the stap base name if it is in started status.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer stepStarted(@Nonnull String started) {
            return defaults().stepStarted(started);
        }

        /**
         * Constructs default event name customizer with step status suffix for failed status.
         *
         * @param failed suffix appended to the stap base name if it is in failed status.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer stepFailed(@Nonnull String failed) {
            return defaults().stepFailed(failed);
        }

        /**
         * Constructs default event name customizer with step status suffix for timed-out status.
         *
         * @param timedOut suffix appended to the stap base name if it is in timed-out status.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer stepTimedOut(@Nonnull String timedOut) {
            return defaults().stepTimedOut(timedOut);
        }

        /**
         * Constructs default event name customizer with flag controlling if the step name is a suffix or is the entire
         * name of the event.
         *
         * @param appendToBaseName if true, the step name is considered as suffix.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer appendToBaseName(boolean appendToBaseName) {
            return defaults().appendToBaseName(appendToBaseName);
        }

        /**
         * Constructs default event name customizer with flag controlling if the simple name of the step is
         * capitalized.
         *
         * @param capitalizeSimpleName if true, the first name of the status is capitalized.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer capitalizeSimpleName(boolean capitalizeSimpleName) {
            return defaults().capitalizeSimpleName(capitalizeSimpleName);
        }

        /**
         * Constructs default event name customizer with a function to customize step based on the payload.
         *
         * @param payloadCustomization function to customize step based on the payload.
         * @return default event name customizer.
         */
        public static DefaultEventNameCustomizer payloadCustomization(
                Function<PayloadCustomization, QualifiedName> payloadCustomization) {
            return defaults().payloadCustomization(payloadCustomization);
        }

        /**
         * Merges two customizers into one.
         *
         * @param parent parent customizer.
         * @param child  child customizer.
         * @return one resulting customizer.
         */
        @Internal
        public static EventNameCustomizer merge(@Nonnull EventNameCustomizer parent,
                                                @Nonnull EventNameCustomizer child) {
            var defaultCustomizer = new DefaultEventNameCustomizer();
            return new EventNameCustomizer() {
                @Nonnull
                @Override
                public QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                                                  @Nonnull StepStatus stepStatus) {
                    var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
                    var parentName = parent.getEventName(stepName, parameters, stepStatus);
                    var childName = child.getEventName(stepName, parameters, stepStatus);

                    String resultingNamespace = defaultName.namespace();
                    if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = childName.namespace();
                    } else if (parentName.namespace() != null && !parentName.namespace()
                                                                            .equals(defaultName.namespace())) {
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

                @Nonnull
                @Override
                public QualifiedName getEventName(@Nonnull String stepName, @Nonnull Map<String, Object> parameters,
                                                  @Nonnull WorkflowStatus stepStatus) {
                    var defaultName = defaultCustomizer.getEventName(stepName, parameters, stepStatus);
                    var parentName = parent.getEventName(stepName, parameters, stepStatus);
                    var childName = child.getEventName(stepName, parameters, stepStatus);

                    String resultingNamespace = defaultName.namespace();
                    if (childName.namespace() != null && !childName.namespace().equals(defaultName.namespace())) {
                        resultingNamespace = childName.namespace();
                    } else if (parentName.namespace() != null && !parentName.namespace()
                                                                            .equals(defaultName.namespace())) {
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

                @Nonnull
                @Override
                public EventNameCustomizer forStepInheritance() {
                    return this;
                }
            };
        }
    }
}
