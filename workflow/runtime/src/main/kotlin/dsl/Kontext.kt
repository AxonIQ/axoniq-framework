package io.axoniq.workflow.dsl

import io.axoniq.workflow.runtime.api.primitives.*
import io.axoniq.workflow.runtime.api.workflow.PayloadProcessor
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.eventName
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.EventMessage
import java.time.Duration
import java.util.function.Predicate
import kotlin.reflect.KClass
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class Kontext(
  private val workflowKontext: WorkflowKontext
) {

  val payload: Map<String, Any?> get() = workflowKontext.payload
  val workflowId: String get() = workflowKontext.workflowId

  fun <T> awaitExecute(
    stepName: String,
    action: (payload: Map<String, Any?>) -> T,
    local: Map<String, Any?> = mapOf(),
    parameterMapping: PayloadReducer = PayloadReducer.local(),
    resultMapping: PayloadReducer = PayloadReducer.all(),
    timeout: Duration = 5.seconds.toJavaDuration(),
    eventNameCustomizer: EventNameCustomizer = eventName()
  ): T {
    val stepSpecificName = "__$stepName"
    return workflowKontext.execute(
      ConvertingExecuteCommand<T>(
        stepSpecificName,
        WorkflowStepResultExecuteCommand(
          stepName,
          local,
          { pc, payload ->
            mapOf(stepSpecificName to action.invoke(payload))
          },
          parameterMapping,
          resultMapping,
          timeout,
          eventNameCustomizer
        )
      )
    )
  }

  class ConvertingExecuteCommand<T>(val resultPropertyName: String, command: WorkflowStepResultExecuteCommand) :
    DelegatingExecuteCommand<T>(command) {
    override fun result(result: WorkflowStepResult): T {
      if (result.isSuccess && result.payload<Any>().isPresent) {
        @Suppress("UNCHECKED_CAST")
        return (result.payload<Map<String, Any?>>().get())[resultPropertyName] as T
      } else {
        throw result.error().get()
      }
    }
  }


  fun execute(
    stepName: String,
    action: PayloadProcessor,
    local: Map<String, Any?> = mapOf(),
    parameterMapping: PayloadReducer = PayloadReducer.local(),
    resultMapping: PayloadReducer = PayloadReducer.all(),
    timeout: Duration = 5.seconds.toJavaDuration(),
    eventNameCustomizer: EventNameCustomizer = eventName()
  ): WorkflowStepResult = workflowKontext.execute(
    stepName,
    local,
    action,
    parameterMapping,
    resultMapping,
    timeout,
    eventNameCustomizer
  )

  fun waitFor(
    stepName: String,
    qualifiedName: QualifiedName,
    predicate: Predicate<EventMessage?> = Predicate { true },
    timeout: Duration = 5.seconds.toJavaDuration(),
    eventNameCustomizer: EventNameCustomizer = eventName()
  ): WorkflowStepResult = workflowKontext.waitFor(
    stepName,
    qualifiedName,
    predicate,
    timeout,
    eventNameCustomizer
  )

  fun waitFor(
    stepName: String,
    eventType: KClass<*>,
    predicate: Predicate<EventMessage?> = Predicate { true },
    timeout: Duration = 5.seconds.toJavaDuration(),
    eventNameCustomizer: EventNameCustomizer = eventName()
  ) = waitFor(
    stepName,
    QualifiedName(eventType.java),
    predicate,
    timeout,
    eventNameCustomizer
  )

  fun wait(
    stepName: String,
    timeout: Duration,
    eventNameCustomizer: EventNameCustomizer = eventName()
  ) {
    val result = waitFor(
      stepName,
      QualifiedName(Void::class.java),
      { false },
      timeout,
      eventNameCustomizer
    )
    if (result.isFailure && result.error().isPresent) {
      throw result.error().get()
    }
  }

  fun <T> await(result: Kontext.() -> WorkflowStepResult): T {
    val result = result()
    if (result.isSuccess && result.payload<Any>().isPresent) {
      return result.payload<T>().get()
    } else {
      throw result.error().get()
    }
  }

}
