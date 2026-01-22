package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.StateManager

abstract class KDefinition(
  val workflowIdRetriever: (Map<String, Any>) -> String,
  val execute: (Kontext) -> Unit
) : WorkflowDefinition<Kontext> {

  override fun createContext(stateManager: StateManager, trigger: Map<String, Any>): Kontext {
    return Kontext(workflowId = workflowIdRetriever.invoke(trigger), stateManager = stateManager, payload = trigger)
  }

  override fun workflowId(trigger: MutableMap<String, Any>): String {
    return workflowIdRetriever.invoke(trigger)
  }

  override fun execute(context: Kontext) {
    execute.invoke(context)
  }

}