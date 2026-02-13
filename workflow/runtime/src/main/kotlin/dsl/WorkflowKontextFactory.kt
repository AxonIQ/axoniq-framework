package io.axoniq.workflow.dsl

import io.axoniq.workflow.runtime.api.workflow.AssociationProvider
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.workflow.WorkflowServices
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName
import org.axonframework.messaging.core.unitofwork.ProcessingContext

class WorkflowKontextFactory(
  private val associationProvider: AssociationProvider
) : WorkflowContextFactory<WorkflowKontext> {

  override fun createContext(
    initialPayload: Map<String, Any?>,
    processingContext: ProcessingContext,
    workFlowServices: WorkflowServices
  ): WorkflowKontext = WorkflowKontext(
    initialPayload = initialPayload,
    workflowId = associationProvider.associationKey(initialPayload)
      .orElseThrow { IllegalArgumentException("Could not extract workflow id") },
    processingContext = processingContext,
    parentCustomizer = eventName(), // FIXME
    workflowServices = workFlowServices,
  )
}