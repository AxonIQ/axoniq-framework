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
package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.execution.context.*
import io.axoniq.workflow.runtime.api.execution.context.retry.RetryPolicy
import io.axoniq.workflow.runtime.api.execution.state.CombinatorWorkflowStepResult
import io.axoniq.workflow.runtime.api.execution.state.StepCancellationException
import io.axoniq.workflow.runtime.api.execution.state.WorkflowStepResult
import io.axoniq.workflow.runtime.api.payload.PayloadProcessor
import io.axoniq.workflow.runtime.api.payload.PayloadReducer
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults
import org.axonframework.conversion.Converter
import org.axonframework.messaging.core.MessageTypeResolver
import org.axonframework.messaging.core.QualifiedName
import java.util.function.Predicate
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

/**
 * Kotlin Workflow Context extension object.
 * @since 1.0.0
 * @author Simon Zambrovski
 */
class Kontext(
    private val workflowKontext: WorkflowKontext
) {

    val payload: Map<String, Any?> get() = workflowKontext.workflowPayload()
    val workflowId: String get() = workflowKontext.workflowId()

    class MapPropertyExtractingExecuteCommand<T>(
        val resultPropertyName: String,
        command: PrimitiveCommands.WorkflowStepResultExecuteCommand
    ) :
        PrimitiveCommands.DelegatingExecuteCommand<T>(command) {
        override fun result(result: WorkflowStepResult): T {
            if (result.success() && result.result<Any>().isPresent) {
                val map = result.result<Map<String, Any?>>().get()
                @Suppress("UNCHECKED_CAST")
                return map[resultPropertyName] as T
            } else {
                throw result.error().get()
            }
        }
    }

    class TypeConvertingWaitForCommand<T : Any>(
        command: PrimitiveCommands.WorkflowStepResultWaitForCommand,
        val type: Class<T>,
        val converter: Converter
    ) :
        PrimitiveCommands.DelegatingWaitForCommand<T>(command) {
        override fun result(result: WorkflowStepResult): T {
            if (result.success() && result.result<Any>().isPresent) {
                val map = result.result<Map<String, Any?>>().get()
                return converter.convert(map, type)!!
            } else {
                throw result.error().get()
            }
        }
    }

    fun <T> awaitExecute(
        stepName: String,
        action: (payload: Map<String, Any?>) -> T,
        local: Map<String, Any?> = mapOf(),
        parameterMapping: PayloadReducer = PayloadReducer.LOCAL_ONLY,
        resultMapping: PayloadReducer = PayloadReducer.GLOBAL_ONLY,
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ): T {
        val stepSpecificName = "__$stepName"
        @Suppress("UNCHECKED_CAST")
        return workflowKontext.execute(
            MapPropertyExtractingExecuteCommand<T>(
                stepSpecificName,
                PrimitiveCommands.WorkflowStepResultExecuteCommand(
                    stepName,
                    local as Map<String, Any>?,
                    { pc, payload ->
                        mapOf(stepSpecificName to action.invoke(payload))
                    },
                    parameterMapping,
                    resultMapping,
                    timeout.toJavaDuration(),
                    eventNameCustomizer,
                    RetryPolicy.NONE
                )
            )
        )
    }

    fun <T : Any> awaitEvent(
        stepName: String,
        type: KClass<T>,
        resultMapping: PayloadReducer = PayloadReducer.GLOBAL_ONLY,
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ): T {
        return workflowKontext.waitFor(
            TypeConvertingWaitForCommand(
                PrimitiveCommands.WorkflowStepResultWaitForCommand(
                    stepName,
                    EventConditions.fromQualifiedName(
                        workflowKontext.processingContext().component(MessageTypeResolver::class.java)
                            .resolve(type.java)
                            .orElseThrow().qualifiedName
                    ),
                    resultMapping,
                    timeout.toJavaDuration(),
                    eventNameCustomizer
                ),
                type.java,
                workflowKontext.processingContext().component(Converter::class.java)
            )
        )
    }

    fun allMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.allMatch(predicate, *results)

    fun noneMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.noneMatch(predicate, *results)

    fun anyMatch(predicate: Predicate<WorkflowStepResult>, vararg results: WorkflowStepResult)
            : CombinatorWorkflowStepResult = workflowKontext.anyMatch(predicate, *results)

    fun execute(
        stepName: String,
        action: PayloadProcessor,
        local: Map<String, Any?> = mapOf(),
        parameterMapping: PayloadReducer = PayloadReducer.LOCAL_ONLY,
        resultMapping: PayloadReducer = PayloadReducer.GLOBAL_ONLY,
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ): WorkflowStepResult = workflowKontext.execute(
        PrimitiveCommands.WorkflowStepResultExecuteCommand(
            stepName,
            local as Map<String, Any>?,
            action,
            parameterMapping,
            resultMapping,
            timeout.toJavaDuration(),
            eventNameCustomizer,
            RetryPolicy.NONE
        )
    )

    fun waitFor(
        stepName: String,
        qualifiedName: QualifiedName,
        resultMapping: PayloadReducer = PayloadReducer.GLOBAL_ONLY,
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ): WorkflowStepResult = workflowKontext.waitFor(
        PrimitiveCommands.WorkflowStepResultWaitForCommand(
            stepName,
            EventConditions.fromQualifiedName(qualifiedName),
            resultMapping,
            timeout.toJavaDuration(),
            eventNameCustomizer
        )
    )


    fun block(
        stepName: String,
        timeout: Duration,
        eventNameCustomizer: EventNameCustomizer = defaults()
    ) {
        val result = waitFor(
            stepName = stepName,
            qualifiedName = QualifiedName(Void::class.java),
            timeout = timeout,
            eventNameCustomizer = eventNameCustomizer
        )
        if (result.failure() && result.error().isPresent) {
            throw result.error().get()
        }
    }

    /**
     * Terminates the entire workflow with an error, publishing a failure event and
     * cancelling all running steps.
     *
     * @param cause the exception that caused the failure
     * @param eventNameCustomizer customizer for the published failure event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowFailedException always, after the failure event is published
     */
    fun fail(cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(TerminatePrimitive.TerminateCommand.fail(cause, eventNameCustomizer))
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(TerminatePrimitive.TerminateCommand.cancel(eventNameCustomizer))
    }

    /**
     * Cancels the entire workflow gracefully with a human-readable reason, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param reason descriptive reason for the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws io.axoniq.workflow.runtime.api.execution.context.WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(reason: String, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(
            TerminatePrimitive.TerminateCommand.cancel(
                WorkflowCancelledException(
                    reason
                ), eventNameCustomizer
            )
        )
    }

    /**
     * Cancels the entire workflow gracefully, publishing a cancellation event and
     * cancelling all running steps.
     *
     * @param cause the exception that triggered the cancellation
     * @param eventNameCustomizer customizer for the published cancellation event name
     * @throws WorkflowCancelledException always, after the cancellation event is published
     */
    fun cancel(cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(TerminatePrimitive.TerminateCommand.cancel(cause, eventNameCustomizer))
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException].
     *
     * @param stepName the name of the step to cancel
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(
            TerminatePrimitive.TerminateCommand.cancelledStep(
                stepName,
                null,
                eventNameCustomizer
            )
        )
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with the given cause, wrapped in a
     * [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException] if it isn't one already.
     *
     * @param stepName the name of the step to cancel
     * @param cause the exception that caused the step cancellation
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, cause: Throwable, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(
            TerminatePrimitive.TerminateCommand.cancelledStep(
                stepName,
                cause,
                eventNameCustomizer
            )
        )
    }

    /**
     * Cancels a single running step by name without terminating the workflow.
     * The step's future is completed exceptionally with a [io.axoniq.workflow.runtime.api.execution.state.StepCancellationException]
     * carrying the given reason.
     *
     * @param stepName the name of the step to cancel
     * @param reason descriptive reason for the step cancellation
     * @param eventNameCustomizer customizer for the published event name
     */
    fun cancelStep(stepName: String, reason: String, eventNameCustomizer: EventNameCustomizer = defaults()) {
        workflowKontext.terminate(
            TerminatePrimitive.TerminateCommand.cancelledStep(
                stepName,
                StepCancellationException(reason),
                eventNameCustomizer
            )
        )
    }

    // just to create blocking call
    fun block(result: Kontext.() -> WorkflowStepResult) {
        val r = result()
        if (r.failure()) {
            if (r.error().isPresent) {
                throw r.error().get()
            } else {
                throw RuntimeException("Unknown step failure: ${r.stepName}")
            }
        }
    }

}
