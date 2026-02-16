package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.EventNameCustomizer
import io.axoniq.workflow.runtime.api.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.WorkflowServices
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName
import org.axonframework.messaging.core.unitofwork.ProcessingContext

class WorkflowKontextFactory(component: EventNameCustomizer) : WorkflowContextFactory<WorkflowKontext> {

    override fun createContext(
        initialPayload: Map<String, Any?>,
        workflowId: String,
        processingContext: ProcessingContext,
        workFlowServices: WorkflowServices
    ): WorkflowKontext = WorkflowKontext(
        initialPayload = initialPayload,
        workflowId = workflowId,
        processingContext = processingContext,
        parentCustomizer = eventName(), // FIXME
        workflowServices = workFlowServices,
    )
}