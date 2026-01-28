package io.axoniq.workflow.runtime.kotlin

import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.EventMessage
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.function.Predicate

class Playground {

  @Test
  fun test() {

  }

  data class Subscription(
    val qn: QualifiedName,
    val predicate: Predicate<Any>
  )

  class EventReceiver {

    private val registrations: MutableList<Subscription> = mutableListOf()

    fun onEvent(eventMessage: EventMessage) {
      if (registrations.any { it.qn.equals(eventMessage.type().qualifiedName) && it.predicate.test(eventMessage.payload()) }) {
        // complete the future.
        
      }
    }

    fun register(qn: QualifiedName, predicate: Predicate<Any>): CompletableFuture<Any> {
      val subscription = Subscription(qn, predicate)
      TODO()
    }
  }
}