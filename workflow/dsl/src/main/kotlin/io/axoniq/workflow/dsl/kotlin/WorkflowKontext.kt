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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration
import io.axoniq.workflow.runtime.api.execution.context.WorkflowDefinition
import io.axoniq.workflow.runtime.execution.AbstractDSLWorkflowContext
import org.axonframework.messaging.core.unitofwork.ProcessingContext

/**
 * Kotlin Workflow Context used for workflow definition via DSL.
 *
 * @param workflowId unique identifier of the workflow instance.
 * @param initialPayload initial payload of the workflow.
 * @param processingContext processing context of the message.
 * @param workflowConfiguration workflow configuration.
 * @since 1.0.0
 * @author Simon Zambrovski
 */
class WorkflowKontext(
    workflowId: String,
    initialPayload: Map<String, Any?>,
    processingContext: ProcessingContext,
    workflowConfiguration: WorkflowConfiguration<*>,
) : AbstractDSLWorkflowContext(
    workflowId,
    initialPayload,
    processingContext,
    workflowConfiguration
) {

    /**
     * Executes the block with the workflow context.
     *
     * @param block block to be executed.
     */
    fun runWorkflow(block: Kontext.() -> Unit) {
        Kontext(this).apply(block)
    }

    companion object {
        /**
         * Creates a workflow definition from the block.
         *
         * @param block block to be executed.
         * @return workflow definition.
         */
        @JvmStatic
        fun from(block: Kontext.() -> Unit): WorkflowDefinition<WorkflowKontext> {
            return WorkflowDefinition { it.runWorkflow(block) }
        }
    }
}