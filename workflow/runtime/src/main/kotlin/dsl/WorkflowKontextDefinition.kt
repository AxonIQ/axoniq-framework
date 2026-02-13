package io.axoniq.workflow.dsl

import io.axoniq.workflow.runtime.api.workflow.AssociationProvider
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.execution.WorkflowState
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory
import java.util.*

interface WorkflowKontextDefinition : WorkflowDefinition<WorkflowKontext>, WorkflowConfiguration<WorkflowKontext> {

  fun workflowId(payload: Map<String, Any?>): String

  override fun workflowDefinition(): WorkflowDefinition<WorkflowKontext> = this

  override fun workflowContextFactory(): WorkflowContextFactory<WorkflowKontext> =
    WorkflowKontextFactory(associationProvider())

  override fun workflowStateFactory() = WorkflowStateFactory { ctx -> ctx as WorkflowState }

  override fun associationProvider() = AssociationProvider { payload -> Optional.ofNullable(workflowId(payload)) }

  override fun execute(context: WorkflowKontext) {
    context.runWorkflow {
      onExecute()
    }
  }

  fun Kontext.onExecute()

}