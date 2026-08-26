/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.workflow.dsl.base;

import io.axoniq.workflow.dsl.api.EventAssociationsUtils;
import io.axoniq.workflow.runtime.api.execution.context.CancelStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.CancelWorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.EventCondition;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.ExecuteStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.FailWorkflowDefinition;
import io.axoniq.workflow.runtime.api.execution.context.PayloadMapping;
import io.axoniq.workflow.runtime.api.execution.context.PayloadStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.PrimitiveMetadata;
import io.axoniq.workflow.runtime.api.execution.context.Timing;
import io.axoniq.workflow.runtime.api.execution.context.VersionStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WaitForStepDefinition;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy;
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult;
import io.axoniq.workflow.runtime.api.payload.PayloadModification;
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor;
import io.axoniq.workflow.runtime.association.Associations;
import io.axoniq.workflow.runtime.association.ValueRetriever;
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext;
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer;
import io.axoniq.workflow.runtime.api.execution.context.Version;
import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;

/**
 * Base Java DSL entry point for defining workflow steps.
 * <p>
 * This context exposes the core primitives used by workflow authors: executing external work, waiting for events,
 * mutating workflow payload, sleeping, failing, and cancelling. Non-blocking methods return a
 * {@link WorkflowStepResult} that can be awaited later, while methods prefixed with {@code await} block until the step
 * completes or fails.
 *
 * @author Simon Zambrovski
 * @author Stefan Dragisic
 * @author Allrad Buijze
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class BaseWorkflowContext extends AbstractDSLWorkflowContext {

    private final MessageTypeResolver messageTypeResolver;

    private Duration defaultTimeout = Duration.ofSeconds(5);
    private RetryPolicy defaultRetryPolicy = RetryPolicy.NONE;

    /**
     * Creates a workflow context for a single workflow instance.
     * <p>
     * Applications normally obtain this context from the runtime through a
     * {@link io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition} callback instead of constructing it
     * directly.
     *
     * @param workflowId            unique identifier of the workflow instance
     * @param payload               initial workflow payload
     * @param processingContext     processing context for the current message
     * @param workflowConfiguration runtime configuration for this workflow
     */
    public BaseWorkflowContext(
            @Nonnull String workflowId,
            @Nonnull Map<String, Object> payload,
            @Nonnull ProcessingContext processingContext,
            @Nonnull WorkflowConfiguration<?> workflowConfiguration
    ) {
        super(workflowId, payload, processingContext, workflowConfiguration);
        this.messageTypeResolver = processingContext().component(MessageTypeResolver.class);
    }

    /**
     * Creates a matcher that requires an event association value to equal the given value.
     *
     * @param value expected association value
     * @return matcher that can be used when building event conditions
     * @deprecated use {@link EventAssociationsUtils#equalsTo(Object)} instead.
     */
    @Deprecated(since = "0.2.0", forRemoval = true)
    public static Associations.Matcher equalsTo(Object value) {
        return EventAssociationsUtils.equalsTo(value);
    }

    /**
     * Creates a payload-property association source for the simple association DSL.
     *
     * @param propertyName payload property name
     * @return payload-property retriever
     * @deprecated use {@link EventAssociationsUtils#payloadProperty(String)} instead.
     */
    @Deprecated(since = "0.2.0", forRemoval = true)
    public static ValueRetriever payloadProperty(@Nonnull String propertyName) {
        return EventAssociationsUtils.payloadProperty(propertyName);
    }

    /**
     * Creates a metadata-property association source for the simple association DSL.
     *
     * @param propertyName metadata key
     * @return metadata-property retriever
     * @deprecated use {@link EventAssociationsUtils#metadataProperty(String)} instead.
     */
    @Deprecated(since = "0.2.0", forRemoval = true)
    public static ValueRetriever metadataProperty(@Nonnull String propertyName) {
        return EventAssociationsUtils.metadataProperty(propertyName);
    }

    /**
     * Applies a customizer to a step definition after checking both arguments for {@code null}. Further customization
     * is possible using the provided {@code customizer} unary operator.
     *
     * @param stepDefinition definition to customize
     * @param customizer     function that mutates or replaces the definition
     * @param <T>            concrete step definition type
     * @return customized step definition
     */
    public static <T> T apply(@Nonnull T stepDefinition, @Nonnull UnaryOperator<T> customizer) {
        return Objects.requireNonNull(customizer, "Customizer must not be null")
                      .apply(Objects.requireNonNull(stepDefinition, "Step definition must not be null"));
    }

    /**
     * Starts an execute step that invokes the given payload processor.
     * <p>
     * The step is scheduled immediately and returns a handle that can be awaited later. Use the {@code inputPayload} to
     * provide step-local input in addition to the current workflow payload. Further customization is possible using the
     * provided {@code customizer} unary operator.
     *
     * @param stepName     logical name of the step
     * @param inputPayload step-specific input values
     * @param processor    function that performs the step work
     * @param customizer   customizer for timeout, mapping, retries, or metadata
     * @return handle for the running step
     */
    @Nonnull
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> inputPayload,
            @Nonnull PayloadProcessor processor,
            @Nonnull UnaryOperator<ExecuteStepDefinition> customizer
    ) {
        return super.execute(apply(defaultExecuteStepDefinition(stepName, inputPayload, processor), customizer));
    }

    /**
     * Starts an execute step with the default step definition settings.
     *
     * @param stepName     logical name of the step
     * @param inputPayload step-specific input values
     * @param processor    function that performs the step work
     * @return handle for the running step
     */
    @Nonnull
    public WorkflowStepResult execute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> inputPayload,
            @Nonnull PayloadProcessor processor
    ) {
        return execute(stepName, inputPayload, processor, UnaryOperator.identity());
    }

    /**
     * Starts an execute step and blocks until it completes. Further customization is possible using the provided
     * {@code customizer} unary operator.
     *
     * @param stepName     logical name of the step
     * @param inputPayload step-specific input values
     * @param processor    function that performs the step work
     * @param customizer   customizer for timeout, mapping, retries, or metadata
     * @return payload produced by the completed step
     * @see #execute(String, Map, PayloadProcessor, UnaryOperator)
     */
    @Nonnull
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> inputPayload,
            @Nonnull PayloadProcessor processor,
            @Nonnull UnaryOperator<ExecuteStepDefinition> customizer
    ) {
        return super.awaitExecute(apply(defaultExecuteStepDefinition(stepName, inputPayload, processor),
                                        customizer));
    }

    /**
     * Starts an execute step with default settings and blocks until it completes.
     *
     * @param stepName     logical name of the step
     * @param inputPayload step-specific input values
     * @param processor    function that performs the step work
     * @return payload produced by the completed step
     * @see #execute(String, Map, PayloadProcessor)
     */
    @Nonnull
    public Map<String, Object> awaitExecute(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> inputPayload,
            @Nonnull PayloadProcessor processor
    ) {
        return awaitExecute(stepName, inputPayload, processor, UnaryOperator.identity());
    }


    /**
     * Starts a wait-for step that completes when a matching event is observed. Further customization is possible using
     * the provided {@code customizer} unary operator.
     *
     * @param stepName       logical name of the waiting step
     * @param eventCondition event selection criteria
     * @param customizer     customizer for timeout, mapping, or metadata
     * @return handle for the waiting step
     */
    @Nonnull
    public WorkflowStepResult waitForEvent(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer
    ) {
        return super.waitForEvent(apply(defaultWaitForStepDefinition(stepName, eventCondition), customizer));
    }

    /**
     * Starts a wait-for step with default settings.
     *
     * @param stepName       logical name of the waiting step
     * @param eventCondition event selection criteria
     * @return handle for the waiting step
     */
    @Nonnull
    public WorkflowStepResult waitForEvent(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition
    ) {
        return waitForEvent(stepName, eventCondition, UnaryOperator.identity());
    }

    /**
     * Starts a wait-for step and blocks until a matching event arrives. Further customization is possible using the
     * provided {@code customizer} unary operator.
     *
     * @param stepName       logical name of the waiting step
     * @param eventCondition event selection criteria
     * @param customizer     customizer for timeout, mapping, or metadata
     * @return payload extracted from the matching event
     * @see #waitForEvent(String, EventCondition, UnaryOperator)
     */
    @Nonnull
    public Map<String, Object> awaitEvent(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer
    ) {
        return super.awaitEvent(apply(defaultWaitForStepDefinition(stepName, eventCondition), customizer));
    }

    /**
     * Starts a wait-for step with default settings and blocks until a matching event arrives.
     *
     * @param stepName       logical name of the waiting step
     * @param eventCondition event selection criteria
     * @return payload extracted from the matching event
     * @see #waitForEvent(String, EventCondition)
     */
    @Nonnull
    public Map<String, Object> awaitEvent(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition
    ) {
        return awaitEvent(stepName, eventCondition, UnaryOperator.identity());
    }

    /**
     * Starts a sleep step. Further customization is possible using the provided {@code customizer} unary operator.
     * <p>
     * The actual delay is usually configured through the supplied customizer by setting a timeout on the step
     * definition.
     *
     * @param stepName   logical name of the sleep step
     * @param customizer customizer that typically sets the timeout
     * @return handle for the sleeping step
     */
    @Nonnull
    public WorkflowStepResult sleep(
            @Nonnull String stepName,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer
    ) {
        return super.waitForEvent(apply(defaultWaitForStepDefinition(stepName, EventConditions.never()), customizer));
    }

    /**
     * Starts a sleep step and blocks until the timeout expires. Further customization is possible using the provided
     * {@code customizer} unary operator.
     *
     * @param stepName   logical name of the sleep step
     * @param customizer customizer that typically sets the timeout
     * @see #sleep(String, UnaryOperator)
     */
    public void awaitSleep(
            @Nonnull String stepName,
            @Nonnull UnaryOperator<WaitForStepDefinition> customizer
    ) {
        var result = super.waitForEvent(apply(defaultWaitForStepDefinition(stepName, EventConditions.never()),
                                              customizer));
        // A cancelled sleep must surface, symmetric with awaitExecute/awaitEvent — otherwise a cancellation is
        // silently swallowed and the body sails past the sleep as if the delay had simply elapsed.
        if (result.canceled()) {
            throw new StepCancellationException("Step '" + stepName + "' was cancelled before completing");
        }
        // A timed-out sleep is the normal, expected completion of a sleep — return without throwing.
        if (result.failure() && result.error().isPresent()) {
            throw result.error().get();
        }
    }

    /**
     * Starts a step that updates the workflow payload. Further customization is possible using the provided
     * {@code customizer} unary operator.
     *
     * @param stepName     logical name of the payload update step
     * @param modification transformation to apply to the current payload
     * @param customizer   customizer for step metadata
     * @return handle for the payload update step
     */
    @Nonnull
    public WorkflowStepResult modifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification modification,
            @Nonnull UnaryOperator<PayloadStepDefinition> customizer
    ) {
        return super.modifyPayload(apply(defaultPayloadStepDefinition(stepName, modification), customizer));
    }

    /**
     * Starts a payload update step with default settings.
     *
     * @param stepName     logical name of the payload update step
     * @param modification transformation to apply to the current payload
     * @return handle for the payload update step
     */
    @Nonnull
    public WorkflowStepResult modifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification modification
    ) {
        return modifyPayload(stepName, modification, UnaryOperator.identity());
    }

    /**
     * Migrates this workflow to {@code newVersion} for {@code changeId} and returns whether the new branch is in
     * effect:
     * <ul>
     *   <li>Already recorded for this {@code changeId} → returns {@code true} iff the recorded
     *       version is {@code >=} {@code newVersion}.</li>
     *   <li>Not recorded but workflow already at {@code newVersion} → returns {@code true} without
     *       publishing a redundant step.</li>
     *   <li>Not recorded and {@code newVersion} strictly greater → publishes a migration step and
     *       returns {@code true}; if the replay-drift guard fires (in-flight workflow ran past this
     *       point under old code), returns {@code false}.</li>
     * </ul>
     * Downgrades (requested {@code <} current, no recorded step) raise
     * {@link IllegalArgumentException}. Call at most once per {@code changeId} per workflow body.
     * <p>
     * Use it to fork workflow logic safely:
     * <pre>{@code
     * if (ctx.migrateVersion("payment-redesign", "0.0.2")) {
     *     ctx.awaitExecute("processPayment", PaymentService::processV2);
     * } else {
     *     ctx.awaitExecute("chargePayment", PaymentService::chargeV1);
     * }
     * }</pre>
     *
     * @param changeId   developer-chosen identifier describing the change.
     * @param newVersion new workflow version to record (semver string, e.g. {@code "0.0.2"}).
     * @param customizer customizer for the version step definition (e.g. event name customizer).
     * @return {@code true} if the workflow is at (or past) {@code newVersion} for this {@code changeId}; {@code false}
     * if it stays on the legacy branch.
     */
    public boolean migrateVersion(@Nonnull String changeId,
                                  @Nonnull String newVersion,
                                  @Nonnull UnaryOperator<VersionStepDefinition> customizer) {
        if (changeId == null || changeId.isBlank()) {
            throw new IllegalArgumentException("changeId must not be blank");
        }
        Version.validate(newVersion);
        return super.migrateVersion(apply(defaultMigrateVersionStepDefinition(changeId, newVersion), customizer));
    }

    /**
     * Migrates this workflow to {@code newVersion} for {@code changeId} with default settings (default event-name
     * customizer).
     *
     * @param changeId   developer-chosen identifier describing the change.
     * @param newVersion new workflow version to record (semver string, e.g. {@code "0.0.2"}).
     * @return {@code true} if the workflow is at (or past) {@code newVersion} for this {@code changeId}.
     * @see #migrateVersion(String, String, UnaryOperator)
     */
    public boolean migrateVersion(@Nonnull String changeId, @Nonnull String newVersion) {
        return migrateVersion(changeId, newVersion, UnaryOperator.identity());
    }

    /**
     * Updates the workflow payload and blocks until the change has been applied. Further customization is possible
     * using the provided {@code customizer} unary operator.
     *
     * @param stepName     logical name of the payload update step
     * @param modification transformation to apply to the current payload
     * @param customizer   customizer for step metadata
     * @see #modifyPayload(String, PayloadModification, UnaryOperator)
     */
    public void awaitModifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification modification,
            @Nonnull UnaryOperator<PayloadStepDefinition> customizer
    ) {
        super.awaitModifyPayload(apply(defaultPayloadStepDefinition(stepName, modification), customizer));
    }

    /**
     * Updates the workflow payload with default settings and blocks until the change has been applied.
     *
     * @param stepName     logical name of the payload update step
     * @param modification transformation to apply to the current payload
     * @see #modifyPayload(String, PayloadModification)
     */
    public void awaitModifyPayload(
            @Nonnull String stepName,
            @Nonnull PayloadModification modification
    ) {
        awaitModifyPayload(stepName, modification, UnaryOperator.identity());
    }

    /**
     * Fails the workflow and publishes the configured failure event. Further customization is possible using the
     * provided {@code customizer} unary operator.
     *
     * @param cause      reason the workflow should fail
     * @param customizer customizer for failure metadata
     */
    public void fail(
            @Nonnull Throwable cause,
            @Nonnull UnaryOperator<FailWorkflowDefinition> customizer
    ) {
        super.fail(apply(defaultFailWorkflowDefinition().cause(cause), customizer));
    }

    /**
     * Fails the workflow using the default failure step definition.
     *
     * @param cause reason the workflow should fail
     */
    public void fail(
            @Nonnull Throwable cause
    ) {
        fail(cause, UnaryOperator.identity());
    }

    /**
     * Cancels the workflow and publishes the configured cancellation event. Further customization is possible using the
     * provided {@code customizer} unary operator.
     *
     * @param customizer customizer for cancellation metadata and cause
     */
    public void cancel(
            @Nonnull UnaryOperator<CancelWorkflowDefinition> customizer
    ) {
        super.cancel(apply(defaultCancelWorkflowDefinition(), customizer));
    }

    /**
     * Cancels the workflow using the default cancellation step definition.
     */
    public void cancel() {
        cancel(UnaryOperator.identity());
    }

    /**
     * Cancels a single running step without terminating the workflow. Further customization is possible using the
     * provided {@code customizer} unary operator.
     *
     * @param stepName   name of the step to cancel
     * @param customizer customizer for cancellation metadata and cause
     */
    public void cancelStep(
            @Nonnull String stepName,
            @Nonnull UnaryOperator<CancelStepDefinition> customizer
    ) {
        super.cancelStep(apply(defaultCancelStepDefinition(stepName), customizer));
    }

    /**
     * Cancels a single running step using the default step cancellation definition.
     *
     * @param stepName name of the step to cancel
     */
    public void cancelStep(@Nonnull String stepName) {
        cancelStep(stepName, UnaryOperator.identity());
    }


    /**
     * Creates the default execute step definition used by the convenience overloads in this class.
     *
     * @param stepName     logical name of the step
     * @param inputPayload step-specific input values
     * @param processor    function that performs the step work
     * @return default execute step definition
     */
    @Nonnull
    @Internal
    public ExecuteStepDefinition defaultExecuteStepDefinition(
            @Nonnull String stepName,
            @Nonnull Map<String, Object> inputPayload,
            @Nonnull PayloadProcessor processor
    ) {
        return new ExecuteStepDefinition(
                new PrimitiveMetadata(stepName, defaults()),
                inputPayload,
                processor,
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(defaultTimeout()),
                defaultRetryPolicy()
        );
    }

    /**
     * Creates the default wait-for step definition used by the convenience overloads in this class.
     *
     * @param stepName       logical name of the waiting step
     * @param eventCondition event selection criteria
     * @return default wait-for step definition
     */
    @Nonnull
    @Internal
    public WaitForStepDefinition defaultWaitForStepDefinition(
            @Nonnull String stepName,
            @Nonnull EventCondition eventCondition
    ) {
        return new WaitForStepDefinition(
                new PrimitiveMetadata(stepName, defaults()),
                eventCondition,
                new PayloadMapping(LocalOnlyPayloadReducer.INSTANCE, GlobalOnlyPayloadReducer.INSTANCE),
                new Timing(defaultTimeout())
        );
    }

    /**
     * Creates the default payload modification step definition used by the convenience overloads in this class.
     *
     * @param stepName     logical name of the payload update step
     * @param modification transformation to apply to the current payload
     * @return default payload step definition
     */
    @Nonnull
    @Internal
    public PayloadStepDefinition defaultPayloadStepDefinition(
            @Nonnull String stepName,
            @Nonnull PayloadModification modification
    ) {
        return new PayloadStepDefinition(
                new PrimitiveMetadata(stepName, defaults()),
                modification
        );
    }

    /**
     * Creates the default version step definition used by the {@link #migrateVersion(String, String, UnaryOperator)}
     * convenience overloads.
     *
     * @param changeId   developer-chosen identifier describing the change.
     * @param newVersion new workflow version to record (semver string).
     * @return default version step definition.
     */
    @Nonnull
    protected VersionStepDefinition defaultMigrateVersionStepDefinition(
            @Nonnull String changeId,
            @Nonnull String newVersion
    ) {
        return new VersionStepDefinition(
                new PrimitiveMetadata(changeId, defaults()),
                newVersion
        );
    }

    /**
     * Creates the default workflow failure definition.
     *
     * @return default fail step definition
     */
    @Nonnull
    @Internal
    public FailWorkflowDefinition defaultFailWorkflowDefinition() {
        return new FailWorkflowDefinition(
                new PrimitiveMetadata("__FailWorkflow", defaults()),
                null
        );
    }

    /**
     * Creates the default workflow cancellation definition.
     *
     * @return default cancel workflow definition
     */
    @Nonnull
    @Internal
    public CancelWorkflowDefinition defaultCancelWorkflowDefinition() {
        return new CancelWorkflowDefinition(
                new PrimitiveMetadata("__CancelWorkflow", defaults()),
                null
        );
    }

    /**
     * Creates the default single-step cancellation definition.
     *
     * @param stepName name of the step to cancel
     * @return default cancel step definition
     */
    @Nonnull
    @Internal
    public CancelStepDefinition defaultCancelStepDefinition(@Nonnull String stepName) {
        return new CancelStepDefinition(
                new PrimitiveMetadata(stepName, defaults()),
                null
        );
    }

    /**
     * Resolves the qualified message name for the given message type.
     *
     * @param messageType event or command class to resolve
     * @return qualified name for the provided class
     */
    @Nonnull
    protected QualifiedName resolve(@Nonnull Class<?> messageType) {
        return messageTypeResolver.resolveOrThrow(Objects.requireNonNull(messageType)).qualifiedName();
    }

    /**
     * Sets the timeout applied to newly created execute and wait-for step definitions unless a step-specific customizer
     * overrides it.
     *
     * @param defaultTimeout default timeout for newly created steps
     */
    public void setDefaultTimeout(@Nonnull Duration defaultTimeout) {
        this.defaultTimeout = Objects.requireNonNull(defaultTimeout, "defaultTimeout must not be null");
    }

    /**
     * Returns the timeout currently applied to newly created step definitions.
     *
     * @return default timeout for newly created steps
     */
    @Nonnull
    public Duration defaultTimeout() {
        return defaultTimeout;
    }

    /**
     * Sets the retry policy applied to newly created execute step definitions unless a customizer overrides it.
     *
     * @param defaultRetryPolicy default retry policy for execute steps
     */
    public void setDefaultRetryPolicy(@Nonnull RetryPolicy defaultRetryPolicy) {
        this.defaultRetryPolicy = Objects.requireNonNull(defaultRetryPolicy, "defaultRetryPolicy must not be null");
    }

    /**
     * Returns the retry policy currently applied to newly created execute step definitions.
     *
     * @return default retry policy for execute steps
     */
    @Nonnull
    public RetryPolicy defaultRetryPolicy() {
        return defaultRetryPolicy;
    }
}
