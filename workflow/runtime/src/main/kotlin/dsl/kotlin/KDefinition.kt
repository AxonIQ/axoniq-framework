package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.workflow.AssociationProvider
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.StateManager
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration
import java.util.*

abstract class KDefinition(
  val associate: (Map<String, Any>) -> String,
  val execute: (Kontext) -> Unit
) : WorkflowDefinition<Kontext>, WorkflowContextFactory<Kontext>, AssociationProvider, WorkflowConfiguration<Kontext> {

  override fun createContext(payload: Map<String, Any>, stateManager: StateManager): Kontext {
    return Kontext(workflowId = associate.invoke(payload), stateManager = stateManager, payload = payload)
  }

  override fun associationKey(payload: Map<String, Any>): Optional<String> {
    return Optional.of(associate.invoke(payload))
  }

  override fun execute(context: Kontext) {
    execute.invoke(context)
  }

  override fun associationProvider(): AssociationProvider = this

  override fun workflowContextFactory(): WorkflowContextFactory<Kontext> = this

  override fun workflowDefinition(): WorkflowDefinition<Kontext> = this
}