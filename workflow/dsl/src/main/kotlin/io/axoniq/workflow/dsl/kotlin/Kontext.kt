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
package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.dsl.api.Payload
import io.axoniq.workflow.runtime.api.execution.context.*
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException
import io.axoniq.workflow.runtime.api.execution.state.StepTimedOutException
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult
import io.axoniq.workflow.runtime.api.payload.PayloadModification
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor
import io.axoniq.workflow.runtime.api.payload.PayloadReducer
import io.axoniq.workflow.runtime.association.Associations
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults
import io.axoniq.workflow.runtime.execution.payload.GlobalOnlyPayloadReducer
import io.axoniq.workflow.runtime.execution.payload.LocalOnlyPayloadReducer
import org.axonframework.messaging.core.MessageTypeResolver
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.conversion.EventConverter
import java.util.function.Predicate
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Kotlin-friendly wrapper around [WorkflowKontext].
 * <p>
 * This type exposes the workflow primitives that Kotlin DSL authors use inside
 * a workflow definition block.
 *
 * @since 1.0.0
 * @author Simon Zambrovski
 */
class Kontext(
    private val workflowKontext: WorkflowKontext
) {

    var defaultTimeout: Duration = 5.seconds
    var defaultRetryPolicy: RetryPolicy = RetryPolicy.NONE

    /**
     * Current workflow payload as visible to the running workflow.
     */
    val payload: Map<String, Any?> get() = workflowKontext.workflowPayload()

    /**
     * Unique identifier of the current workflow instance.
     */
    val workflowId: String get() = workflowKontext.workflowId()

    /**
     * Current workflow definition version (semver string) — driven by `@Workflow(version=...)` at startup
     * and possibly bumped mid-flight via [version]. Every event the workflow emits carries this on its
     * `MessageType.version()`.
     */
    val workflowVersion: String get() = workflowKontext.workflowVersion()

    /**
     * Processing context for the current message.
     */
    val processingContext get() = workflowKontext.processingContext()

    /**
     * Resolves the Axon qualified message name for the given message type.
     *
     * @param messageType event or command class to resolve
     * @return qualified name for the provided class
     */
    private fun resolve(messageType: KClass<*>): QualifiedName =
        processingContext.component(MessageTypeResolver::class.java).resolveOrThrow(messageType.java).qualifiedName

    /**
     * Starts an execute step using the Kotlin convenience API.
     *
     * @param stepName logical step name
     * @param inputPayload step-local payload passed to the action
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param parameterPayloadReducer reducer controlling the action input
     * @param resultPayloadReducer reducer controlling the workflow payload update
     * @param retryPolicy retry policy applied to the action
     * @param action action to execute
     * @return handle for the running step
     */
    fun execute(
        stepName: String,
        inputPayload: Map<String, Any?> = emptyMap(),
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        parameterPayloadReducer: PayloadReducer = LocalOnlyPayloadReducer.INSTANCE,
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE,
        retryPolicy: RetryPolicy = defaultRetryPolicy,
        action: PayloadProcessor
    ): WorkflowStepResult =
        execute(
            workflowKontext.defaultExecuteStepDefinition(stepName, inputPayload, action)
                .timeout(timeout.toJavaDuration())
                .eventNameCustomizer(eventNameCustomizer)
                .parameterPayloadReducer(parameterPayloadReducer)
                .resultPayloadReducer(resultPayloadReducer)
                .retryPolicy(retryPolicy)
        )

    /**
     * Starts an execute step and blocks until it completes.
     *
     * @param stepName logical step name
     * @param inputPayload step-local payload passed to the action
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param parameterPayloadReducer reducer controlling the action input
     * @param resultPayloadReducer reducer controlling the workflow payload update
     * @param retryPolicy retry policy applied to the action
     * @param action action to execute
     * @return payload produced by the completed step
     */
    fun awaitExecute(
        stepName: String,
        inputPayload: Map<String, Any?> = emptyMap(),
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        parameterPayloadReducer: PayloadReducer = LocalOnlyPayloadReducer.INSTANCE,
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE,
        retryPolicy: RetryPolicy = defaultRetryPolicy,
        action: PayloadProcessor
    ): Map<String, Any?> =
        awaitExecute(
            workflowKontext.defaultExecuteStepDefinition(stepName, inputPayload, action)
                .timeout(timeout.toJavaDuration())
                .eventNameCustomizer(eventNameCustomizer)
                .parameterPayloadReducer(parameterPayloadReducer)
                .resultPayloadReducer(resultPayloadReducer)
                .retryPolicy(retryPolicy)
        )

    /**
     * Runs a typed synchronous action as a workflow step.
     *
     * The action result is stored under the internal key
     * `"__" + stepName + "Result"` in the step payload and then unwrapped
     * back to `T`.
     *
     * @param stepName logical step name
     * @param resultType expected return type
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param retryPolicy retry policy applied to the action
     * @param action action to execute
     * @return typed result produced by the action
     */
    fun <T : Any> awaitExecute(
        stepName: String,
        resultType: KClass<T>,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        retryPolicy: RetryPolicy = defaultRetryPolicy,
        action: () -> T
    ): T =
        syntheticStepResultKey(stepName).let { resultKey ->
            resultType.java.cast(
                awaitExecute(
                    stepName = stepName,
                    timeout = timeout,
                    eventNameCustomizer = eventNameCustomizer,
                    retryPolicy = retryPolicy
                ) { _, _ -> mapOf(resultKey to action()) }[resultKey]
            )
        }

    inline fun <reified T : Any> awaitExecute(
        stepName: String,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        retryPolicy: RetryPolicy = defaultRetryPolicy,
        noinline action: () -> T
    ): T = awaitExecute(stepName, T::class, timeout, eventNameCustomizer, retryPolicy, action)

    private fun syntheticStepResultKey(stepName: String): String = "__${stepName}Result"

    /**
     * Starts a wait-for step using the Kotlin convenience API.
     *
     * @param stepName logical step name
     * @param eventCondition event condition to satisfy
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param resultPayloadReducer reducer controlling the workflow payload update
     * @return handle for the waiting step
     */
    fun waitForEvent(
        stepName: String,
        eventCondition: EventCondition,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE
    ): WorkflowStepResult =
        waitForEvent(
            workflowKontext.defaultWaitForStepDefinition(stepName, eventCondition)
                .timeout(timeout.toJavaDuration())
                .eventNameCustomizer(eventNameCustomizer)
                .resultPayloadReducer(resultPayloadReducer)
        )

    /**
     * Starts a wait-for step and blocks until it completes.
     *
     * @param stepName logical step name
     * @param eventCondition event condition to satisfy
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param resultPayloadReducer reducer controlling the workflow payload update
     * @return payload extracted from the matching event
     */
    fun awaitEvent(
        stepName: String,
        eventCondition: EventCondition,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE
    ): Map<String, Any?> =
        awaitEvent(
            workflowKontext.defaultWaitForStepDefinition(stepName, eventCondition)
                .timeout(timeout.toJavaDuration())
                .eventNameCustomizer(eventNameCustomizer)
                .resultPayloadReducer(resultPayloadReducer)
        )

    /**
     * Waits for an event of the given type and returns the converted event
     * payload.
     *
     * @param stepName logical step name
     * @param eventType expected event payload type
     * @param conditions optional association constraints for event matching
     * @param timeout step timeout, defaults to [defaultTimeout]
     * @param eventNameCustomizer customizer for published event names
     * @param resultPayloadReducer reducer controlling the workflow payload update
     * @return converted event payload
     */
    fun <T : Any> awaitEvent(
        stepName: String,
        eventType: KClass<T>,
        conditions: Associations? = null,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE
    ): T {
        val result = waitForEvent(
            stepName,
            eventCondition(eventType, conditions),
            timeout,
            eventNameCustomizer,
            resultPayloadReducer
        )
        result.await()
        if (result.success()) {
            return result.resultAs(eventType.java, processingContext.component(EventConverter::class.java))
                .orElseThrow { IllegalStateException("No event payload for step '$stepName'") }
        }
        if (result.timeout()) {
            throw StepTimedOutException(
                "Step '$stepName' timed out waiting for event ${eventType.java.name}"
            )
        }
        if (result.canceled()) {
            throw StepCancellationException(
                "Step '$stepName' was cancelled while waiting for event ${eventType.java.name}"
            )
        }
        if (result.failure() && result.error().isPresent) {
            throw result.error().get()
        }
        throw IllegalStateException("No event payload for step '$stepName'")
    }

    inline fun <reified T : Any> awaitEvent(
        stepName: String,
        conditions: Associations? = null,
        timeout: Duration = defaultTimeout,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        resultPayloadReducer: PayloadReducer = GlobalOnlyPayloadReducer.INSTANCE
    ): T = awaitEvent(stepName, T::class, conditions, timeout, eventNameCustomizer, resultPayloadReducer)

    /**
     * Starts a durable sleep step and blocks until the timeout expires.
     *
     * @param stepName logical step name
     * @param timeout sleep duration
     * @param eventNameCustomizer customizer for published event names
     */
    fun sleep(
        stepName: String,
        timeout: Duration,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ) {
        val result = waitForEvent(stepName, EventConditions.never(), timeout, eventNameCustomizer)
        // A cancelled sleep must surface, symmetric with awaitExecute/awaitEvent — otherwise a cancellation is
        // silently swallowed and the body sails past the sleep as if the delay had simply elapsed.
        if (result.canceled()) {
            throw StepCancellationException("Step '$stepName' was cancelled before completing")
        }
        // A timed-out sleep is the normal, expected completion of a sleep — return without throwing.
        if (result.failure() && result.error().isPresent) {
            throw result.error().get()
        }
    }

    /**
     * Starts a payload update step using the Kotlin convenience API.
     *
     * @param stepName logical step name
     * @param modification payload modification to apply
     * @param eventNameCustomizer customizer for published event names
     * @return handle for the payload update step
     */
    fun modifyPayload(
        stepName: String,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        modification: PayloadModification
    ): WorkflowStepResult =
        modifyPayload(
            workflowKontext.defaultPayloadStepDefinition(stepName, modification)
                .eventNameCustomizer(eventNameCustomizer)
        )

    /**
     * Applies a payload update and blocks until it has been committed.
     *
     * @param stepName logical step name
     * @param eventNameCustomizer customizer for published event names
     * @param modification payload modification to apply
     */
    fun awaitModifyPayload(
        stepName: String,
        eventNameCustomizer: EventNameCustomizer = defaults(),
        modification: PayloadModification
    ) {
        awaitModifyPayload(
            workflowKontext.defaultPayloadStepDefinition(stepName, modification)
                .eventNameCustomizer(eventNameCustomizer)
        )
    }

    /**
     * Replaces the workflow payload with the fields extracted from the given
     * value.
     *
     * @param stepName logical step name
     * @param value object whose properties become part of the workflow payload
     */
    fun setPayload(stepName: String, value: Any) {
        awaitModifyPayload(stepName) { workflowPayload ->
            Payload.payload(workflowPayload)
                .with(Payload.payload(workflowKontext, value))
                .values
        }
    }

    /**
     * Starts an execute step using a fully configured step definition.
     *
     * @param stepDefinition execute step definition to schedule
     * @return handle for the running step
     */
    fun execute(stepDefinition: ExecuteStepDefinition): WorkflowStepResult =
        workflowKontext.execute(stepDefinition)

    /**
     * Starts an execute step and blocks until it completes.
     *
     * @param stepDefinition execute step definition to schedule
     * @return payload produced by the completed step
     */
    fun awaitExecute(stepDefinition: ExecuteStepDefinition): Map<String, Any?> =
        workflowKontext.awaitExecute(stepDefinition)

    /**
     * Starts a wait-for step using a fully configured step definition.
     *
     * @param stepDefinition wait-for step definition to schedule
     * @return handle for the waiting step
     */
    fun waitForEvent(stepDefinition: WaitForStepDefinition): WorkflowStepResult =
        workflowKontext.waitForEvent(stepDefinition)

    /**
     * Starts a wait-for step and blocks until a matching event is observed.
     *
     * @param stepDefinition wait-for step definition to schedule
     * @return payload extracted from the matching event
     */
    fun awaitEvent(stepDefinition: WaitForStepDefinition): Map<String, Any?> =
        workflowKontext.awaitEvent(stepDefinition)

    /**
     * Starts a payload update step using a fully configured step definition.
     *
     * @param stepDefinition payload update definition to schedule
     * @return handle for the payload update step
     */
    fun modifyPayload(stepDefinition: PayloadStepDefinition): WorkflowStepResult =
        workflowKontext.modifyPayload(stepDefinition)

    /**
     * Applies a payload update and blocks until it has been committed.
     *
     * @param stepDefinition payload update definition to schedule
     */
    fun awaitModifyPayload(stepDefinition: PayloadStepDefinition) =
        workflowKontext.awaitModifyPayload(stepDefinition)

    /**
     * Migrates this workflow to [newVersion] for [changeId] and returns whether the new branch is
     * in effect:
     * - Already recorded for [changeId] → returns `true` iff the recorded version is `>=` [newVersion].
     * - Not recorded but workflow already at [newVersion] → returns `true` without publishing.
     * - Not recorded and [newVersion] strictly greater → publishes a migration step and returns `true`;
     *   if the replay-drift guard fires, returns `false`.
     *
     * Downgrades raise [IllegalArgumentException]. Call at most once per [changeId] per workflow body.
     *
     * Use it to fork workflow logic safely:
     * ```
     * if (ctx.migrateVersion("payment-redesign", "0.0.2")) {
     *     ctx.awaitExecute("processPayment") { PaymentService.processV2(it) }
     * } else {
     *     ctx.awaitExecute("chargePayment") { PaymentService.chargeV1(it) }
     * }
     * ```
     *
     * @param changeId            developer-chosen identifier describing the change.
     * @param newVersion          new workflow version to record (semver string, e.g. `"0.0.2"`).
     * @param eventNameCustomizer customizer for the wire-level event name of the migration step.
     * @return `true` iff the workflow is at (or past) [newVersion] for [changeId].
     */
    fun migrateVersion(
        changeId: String,
        newVersion: String,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ): Boolean = workflowKontext.migrateVersion(changeId, newVersion) { it.eventNameCustomizer(eventNameCustomizer) }

    /**
     * Advanced overload: migrates from a fully configured step definition.
     * Mirrors [modifyPayload] / [execute] / [waitFor] StepDefinition-accepting forms.
     *
     * @param stepDefinition fully configured version step definition.
     * @return `true` iff the workflow is at (or past) the definition's version.
     */
    fun migrateVersion(stepDefinition: VersionStepDefinition): Boolean =
        workflowKontext.migrateVersion(
            stepDefinition.primitiveMetadata().stepName(),
            stepDefinition.newVersion()
        ) { stepDefinition }

    /**
     * Creates a combined result that succeeds when all given step results match
     * the predicate.
     *
     * @param predicate the predicate to check.
     * @param results the results to combine.
     * @return combined result.
     */
    fun allMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.allMatch(predicate, *results)

    /**
     * Creates a combined result that succeeds when none of the given step
     * results match the predicate.
     *
     * @param predicate the predicate to check.
     * @param results the results to combine.
     * @return combined result.
     */
    fun noneMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.noneMatch(predicate, *results)

    /**
     * Creates a combined result that succeeds when any of the given step
     * results match the predicate.
     *
     * @param predicate the predicate to check.
     * @param results the results to combine.
     * @return combined result.
     */
    fun anyMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.anyMatch(predicate, *results)

    /**
     * Terminates the entire workflow with an error, publishing a failure event and
     * cancelling all running steps.
     *
     * @param cause the exception that caused the failure
     * @param eventNameCustomizer customizer for the published failure event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException always, after the failure event is published
     */
    fun fail(cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.fail(cause) { it.eventNameCustomizer(eventNameCustomizer) }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancel { it.eventNameCustomizer(eventNameCustomizer) }

    /**
     * Cancels the entire workflow gracefully with a human-readable reason, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param reason descriptive reason for the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(reason: String, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancel {
            it.eventNameCustomizer(eventNameCustomizer).cause(WorkflowCancelledException(reason))
        }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param cause the exception that triggered the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancel { it.eventNameCustomizer(eventNameCustomizer).cause(cause) }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException].
     *
     * @param stepName the name of the step to cancel
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancelStep(stepName) { it.eventNameCustomizer(eventNameCustomizer) }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with the given cause, wrapped in a
     * [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException] if it isn't one already.
     *
     * @param stepName the name of the step to cancel
     * @param cause the exception that caused the step cancellation
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancelStep(stepName) { it.eventNameCustomizer(eventNameCustomizer).cause(cause) }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException]
     * carrying the given reason.
     *
     * @param stepName the name of the step to cancel
     * @param reason descriptive reason for the step cancellation
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, reason: String, eventNameCustomizer: EventNameCustomizer = defaults()) =
        workflowKontext.cancelStep(stepName) {
            it.eventNameCustomizer(eventNameCustomizer).cause(StepCancellationException(reason))
        }

    /**
     * Waits for the result produced by the block and rethrows step failures as
     * exceptions.
     *
     * @param result block returning a [WorkflowStepResult]
     */
    fun block(result: Kontext.() -> WorkflowStepResult) {
        val r = result()
        // Symmetric with awaitExecute/awaitEvent/awaitModifyPayload: surface cancellation and timeout too, not only
        // failure — otherwise a cancelled or timed-out step is silently swallowed and the body continues.
        if (r.canceled()) {
            throw StepCancellationException("Step '${r.stepName}' was cancelled before completing")
        }
        if (r.timeout()) {
            throw StepTimedOutException("Step '${r.stepName}' timed out before completing")
        }
        if (r.failure()) {
            if (r.error().isPresent) {
                throw r.error().get()
            } else {
                throw RuntimeException("Unknown step failure: ${r.stepName}")
            }
        }
    }

    private fun eventCondition(
        eventType: KClass<*>,
        conditions: Associations?
    ): EventCondition {
        val qualifiedName = resolve(eventType)
        return if (conditions == null) {
            EventConditions.fromQualifiedName(qualifiedName)
        } else {
            EventConditions.fromQualifiedName(qualifiedName, conditions)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun asObjectMap(values: Map<String, Any?>): Map<String, Any> =
        values as Map<String, Any>
}
