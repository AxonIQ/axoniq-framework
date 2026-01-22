package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.runtime.api.workflow.AssociationProvider
import io.axoniq.workflow.runtime.api.workflow.StateManager
import io.axoniq.workflow.runtime.api.workflow.WorkflowContextFactory
import io.axoniq.workflow.runtime.api.workflow.WorkflowDefinition
import io.axoniq.workflow.runtime.engine.SimpleStateManager
import io.axoniq.workflow.runtime.api.workflow.WorkflowConfiguration
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext
import io.axoniq.workflow.runtime.api.workflow.WorkflowState
import io.axoniq.workflow.runtime.api.workflow.WorkflowStateFactory
import org.axonframework.messaging.eventhandling.gateway.EventAppender
import java.time.Clock
import java.util.*

abstract class KDefinition(
  val associate: (Map<String, Any>) -> String,
  val execute: (Kontext) -> Unit
) : WorkflowDefinition<Kontext>, WorkflowContextFactory<Kontext>, WorkflowStateFactory, AssociationProvider, WorkflowConfiguration<Kontext> {

  override fun createContext(payload: Map<String, Any>, stateManager: StateManager, eventAppender: EventAppender): Kontext {
    return Kontext(
      workflowId = associate.invoke(payload),
      payload = payload,
      stateManager = stateManager,
      eventAppender = eventAppender,
      clock = Clock.systemDefaultZone()
    )
  }

  override fun associationKey(payload: Map<String, Any>): Optional<String> {
    return Optional.of(associate.invoke(payload))
  }

  override fun execute(context: Kontext) {
    execute.invoke(context)
  }

  override fun create(context: WorkflowContext): WorkflowState {
    if (context is Kontext) {
      return context
    } else {
      throw UnsupportedOperationException("$context is not an instance of Kontext.")
    }
  }

  override fun associationProvider(): AssociationProvider = this

  override fun workflowContextFactory(): WorkflowContextFactory<Kontext> = this

  override fun workflowDefinition(): WorkflowDefinition<Kontext> = this

  override fun workflowStateFactory(): WorkflowStateFactory = this

}