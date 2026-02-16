package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.WorkflowDefinition

interface WorkflowKontextDefinition : WorkflowDefinition<WorkflowKontext> {

    override fun accept(context: WorkflowKontext) {
        context.runWorkflow {
            onExecute()
        }
    }

    fun Kontext.onExecute()
}