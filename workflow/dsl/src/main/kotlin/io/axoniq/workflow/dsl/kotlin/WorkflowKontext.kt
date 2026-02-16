package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.EventNameCustomizer
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
}