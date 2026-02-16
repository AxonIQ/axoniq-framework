package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.AssociationProvider
import io.axoniq.workflow.runtime.api.WorkflowConfiguration
import io.axoniq.workflow.runtime.api.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.execution.WorkflowState
import io.axoniq.workflow.runtime.engine.execution.WorkflowStateFactory
import java.util.*

interface WorkflowKontextDefinition : WorkflowDefinition<WorkflowKontext>, WorkflowConfiguration<WorkflowKontext> {

    fun workflowId(payload: Map<String, Any?>): String

    override fun workflowDefinition(): WorkflowDefinition<WorkflowKontext> = this

    override fun workflowContextFactory(): WorkflowContextFactory<WorkflowKontext> = WorkflowKontextFactory()

    override fun workflowStateFactory() = WorkflowStateFactory { ctx -> ctx as WorkflowState }

    override fun associationProvider() = AssociationProvider { payload -> Optional.ofNullable(workflowId(payload)) }

    override fun accept(context: WorkflowKontext) {
        context.runWorkflow {
            onExecute()
        }
    }

    fun Kontext.onExecute()

}