package io.axoniq.workflow.dsl.kotlin

import io.axoniq.workflow.dsl.simple.Payload
import io.axoniq.workflow.dsl.simple.Payload.payload
import io.axoniq.workflow.runtime.api.primitives.PayloadReducer
import io.axoniq.workflow.runtime.api.workflow.WorkflowContext
import io.axoniq.workflow.runtime.context.DefaultEventNameCustomizer.Builder.defaults
import io.axoniq.workflow.runtime.context.WorkflowExecutionImpl
import io.axoniq.workflow.runtime.engine.StateManager
import java.time.Duration
import java.util.concurrent.CompletionException

class Kontext(
  workflowId: String,
  stateManager: StateManager,
  payload: Map<String, Any>
) : WorkflowExecutionImpl(workflowId, stateManager, payload), WorkflowContext

fun Kontext.execute(stepName: String, payload: Payload, action: (Payload) -> Payload): Payload =
  execute(
    stepName,
    payload.values,
    { m -> action.invoke(payload(m)).values },
    PayloadReducer.local(),
    PayloadReducer.all(),
    Duration.ofSeconds(5),
    defaults()
  ).thenApply { payload(it) }.join()

inline fun <reified T : Any> Kontext.waitForEvent(
  stepName: String,
  noinline predicate: (T) -> Boolean,
  duration: Duration
): T {
  return waitFor(
    stepName,
    T::class.java,
    predicate,
    duration,
    { t -> typeToPayloadConverter().apply(t) },
    defaults()
  ).join()
}

fun Kontext.wait(
  stepName: String,
  duration: Duration
) {
  try {
    waitFor(
      stepName,
      Unit::class.java,
      { false },
      duration,
      { t -> typeToPayloadConverter().apply(t) },
      defaults()
    ).join()
  } catch (_: CompletionException) {
    // no error on timeout
  }
}
