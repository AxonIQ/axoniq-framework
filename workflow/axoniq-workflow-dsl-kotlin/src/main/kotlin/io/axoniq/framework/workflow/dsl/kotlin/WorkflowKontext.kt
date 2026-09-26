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
package io.axoniq.framework.workflow.dsl.kotlin

import io.axoniq.framework.workflow.dsl.base.BaseWorkflowContext
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowConfiguration
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowDefinition
import org.axonframework.messaging.core.unitofwork.ProcessingContext

/**
 * Kotlin DSL workflow context for a single workflow instance.
 *
 * @param workflowId unique identifier of the workflow instance
 * @param initialPayload initial payload of the workflow
 * @param processingContext processing context of the current message
 * @param workflowConfiguration runtime configuration for this workflow
 * @since 5.4.0
 * @author Simon Zambrovski
 */
class WorkflowKontext(
    workflowId: String,
    initialPayload: Map<String, Any?>,
    processingContext: ProcessingContext,
    workflowConfiguration: WorkflowConfiguration<*>,
) : BaseWorkflowContext(
    workflowId,
    initialPayload,
    processingContext,
    workflowConfiguration
) {
    /**
     * Runs the workflow definition block against this context instance.
     *
     * @param block Kotlin DSL block that defines the workflow steps
     */
    fun runWorkflow(block: Kontext.() -> Unit) {
        Kontext(this).apply(block)
    }

    companion object {
        /**
         * Creates a reusable workflow definition from a Kotlin DSL block.
         *
         * @param block Kotlin DSL block that defines the workflow steps
         * @return workflow definition that can be registered with the runtime
         */
        @JvmStatic
        fun from(block: Kontext.() -> Unit): WorkflowDefinition<WorkflowKontext> {
            return WorkflowDefinition { it.runWorkflow(block) }
        }
    }
}
