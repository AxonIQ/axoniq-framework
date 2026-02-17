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

import io.axoniq.workflow.runtime.api.EventNameCustomizer
import io.axoniq.workflow.runtime.api.WorkflowDefinition
import io.axoniq.workflow.runtime.api.WorkflowServices
import io.axoniq.workflow.runtime.engine.impl.WorkflowInstance
import org.axonframework.messaging.core.unitofwork.ProcessingContext


class WorkflowKontext(
    workflowId: String,
    initialPayload: Map<String, Any?>,
    processingContext: ProcessingContext,
    parentCustomizer: EventNameCustomizer,
    workflowServices: WorkflowServices
) : WorkflowInstance(
    workflowId,
    initialPayload,
    processingContext,
    parentCustomizer,
    workflowServices
) {

    fun runWorkflow(block: Kontext.() -> Unit) {
        Kontext(this).apply(block)
    }

    companion object {
        @JvmStatic
        fun from(block: Kontext.() -> Unit): WorkflowDefinition<WorkflowKontext> {
            return WorkflowDefinition { it.runWorkflow(block) }
        }
    }
}