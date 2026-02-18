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
package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.*
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName
import org.axonframework.conversion.Converter
import org.axonframework.messaging.core.MessageTypeResolver
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.EventMessage
import java.util.function.Predicate
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class Kontext(
    private val workflowKontext: WorkflowKontext
) {

    val payload: Map<String, Any?> get() = workflowKontext.payload
    val workflowId: String get() = workflowKontext.workflowId

    class MapPropertyExtractingExecuteCommand<T>(
        val resultPropertyName: String,
        command: PrimitiveCommands.WorkflowStepResultExecuteCommand
    ) :
        PrimitiveCommands.DelegatingExecuteCommand<T>(command) {
        override fun result(result: WorkflowStepResult): T {
            if (result.isSuccess && result.result<Any>().isPresent) {
                @Suppress("UNCHECKED_CAST")
                return (result.result<Map<String, Any?>>().get())[resultPropertyName] as T
            } else {
                throw result.error().get()
            }
        }
    }

    class TypeConvertingWaitForCommand<T>(
        command: PrimitiveCommands.WorkflowStepResultWaitForCommand,
        val type: Class<T>,
        val converter: Converter
    ) :
        PrimitiveCommands.DelegatingWaitForCommand<T>(command) {
        override fun result(result: WorkflowStepResult): T {
            if (result.isSuccess && result.result<Any>().isPresent) {
                @Suppress("UNCHECKED_CAST")
                return converter.convert(result.result<Map<String, Any?>>().get(), type) as T
            } else {
                throw result.error().get()
            }
        }
    }

    fun <T> awaitExecute(
        stepName: String,
        action: (payload: Map<String, Any?>) -> T,
        local: Map<String, Any?> = mapOf(),
        parameterMapping: PayloadReducer = PayloadReducer.local(),
        resultMapping: PayloadReducer = PayloadReducer.all(),
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = eventName()
    ): T {
        val stepSpecificName = "__$stepName"
        return workflowKontext.execute(
            MapPropertyExtractingExecuteCommand<T>(
                stepSpecificName,
                PrimitiveCommands.WorkflowStepResultExecuteCommand(
                    stepName,
                    local,
                    { pc, payload ->
                        mapOf(stepSpecificName to action.invoke(payload))
                    },
                    parameterMapping,
                    resultMapping,
                    timeout.toJavaDuration(),
                    eventNameCustomizer
                )
            )
        )
    }

    fun <T : Any> awaitEvent(
        stepName: String,
        type: KClass<T>,
        predicate: Predicate<EventMessage> = Predicate { true },
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = eventName()
    ): T {
        return workflowKontext.waitFor(
            TypeConvertingWaitForCommand(
                PrimitiveCommands.WorkflowStepResultWaitForCommand(
                    stepName,
                    workflowKontext.processingContext().component(MessageTypeResolver::class.java).resolve(type.java)
                        .orElseThrow().qualifiedName,
                    predicate,
                    timeout.toJavaDuration(),
                    eventNameCustomizer
                ),
                type.java,
                workflowKontext.processingContext().component(Converter::class.java)
            )
        )
    }


    fun execute(
        stepName: String,
        action: PayloadProcessor,
        local: Map<String, Any?> = mapOf(),
        parameterMapping: PayloadReducer = PayloadReducer.local(),
        resultMapping: PayloadReducer = PayloadReducer.all(),
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = eventName()
    ): WorkflowStepResult = workflowKontext.execute(
        PrimitiveCommands.WorkflowStepResultExecuteCommand(
            stepName,
            local,
            action,
            parameterMapping,
            resultMapping,
            timeout.toJavaDuration(),
            eventNameCustomizer
        )
    )

    fun waitFor(
        stepName: String,
        qualifiedName: QualifiedName,
        predicate: Predicate<EventMessage> = Predicate { true },
        timeout: Duration = 5.seconds,
        eventNameCustomizer: EventNameCustomizer = eventName()
    ): WorkflowStepResult = workflowKontext.waitFor(
        PrimitiveCommands.WorkflowStepResultWaitForCommand(
            stepName,
            qualifiedName,
            predicate,
            timeout.toJavaDuration(),
            eventNameCustomizer
        )
    )


    fun block(
        stepName: String,
        timeout: Duration,
        eventNameCustomizer: EventNameCustomizer = eventName()
    ) {
        val result = waitFor(
            stepName,
            QualifiedName(Void::class.java),
            { false },
            timeout,
            eventNameCustomizer
        )
        if (result.isFailure && result.error().isPresent) {
            throw result.error().get()
        }
    }

    // just to create blocking call
    fun block(result: Kontext.() -> WorkflowStepResult) {
        val r = result()
        if (r.isFailure) {
            if (r.error().isPresent) {
                throw r.error().get()
            } else {
                throw RuntimeException("Unknown step failure: ${r.stepName}")
            }
        }
    }

}
