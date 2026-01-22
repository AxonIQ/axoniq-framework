package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.workflow.CorrelationProvider
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.StateManager
import io.axoniq.workflow.runtime.engine.WorkflowConfiguration
import java.util.*

abstract class KDefinition(
  val workflowIdRetriever: (Map<String, Any>) -> String,
  val execute: (Kontext) -> Unit
) : WorkflowDefinition<Kontext>, WorkflowContextFactory<Kontext>, CorrelationProvider, WorkflowConfiguration<Kontext> {

  override fun createContext(payload: Map<String, Any>, stateManager: StateManager): Kontext {
    return Kontext(workflowId = workflowIdRetriever.invoke(payload), stateManager = stateManager, payload = payload)
  }

  override fun correlationKey(payload: Map<String, Any>): Optional<String> {
    return Optional.of(workflowIdRetriever.invoke(payload))
  }

  override fun execute(context: Kontext) {
    execute.invoke(context)
  }

  override fun correlatorProvider(): CorrelationProvider = this

  override fun workflowContextFactory(): WorkflowContextFactory<Kontext> = this

  override fun workflowDefinition(): WorkflowDefinition<Kontext> = this
}