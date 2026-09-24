/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */
package io.axoniq.framework.workflow.springboot.kotlin

import io.axoniq.framework.workflow.dsl.api.StepStatus
import io.axoniq.framework.workflow.dsl.kotlin.Kontext
import io.axoniq.framework.workflow.annotation.Workflow
import io.axoniq.framework.workflow.runtime.util.MetadataUtils.METADATA_KEY_STEP_NAME
import io.axoniq.framework.workflow.runtime.util.MetadataUtils.METADATA_KEY_TYPE
import io.axoniq.framework.workflow.runtime.util.MetadataUtils.METADATA_KEY_WORKFLOW_ID
import io.axoniq.framework.workflow.runtime.util.MetadataUtils.isPublishStep
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.axonframework.eventsourcing.eventstore.EventStorageEngine
import org.axonframework.eventsourcing.eventstore.SourcingCondition
import org.axonframework.eventsourcing.eventstore.TerminalEventMessage
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine
import org.axonframework.messaging.core.MessageType
import org.axonframework.messaging.core.Metadata
import org.axonframework.messaging.core.QualifiedName
import org.axonframework.messaging.eventhandling.EventMessage
import org.axonframework.messaging.eventhandling.EventSink
import org.axonframework.messaging.eventhandling.GenericEventMessage
import org.axonframework.messaging.eventhandling.annotation.Event
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.TokenStore
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.inmemory.InMemoryTokenStore
import org.axonframework.messaging.eventstreaming.EventCriteria
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.EnableMBeanExport
import org.springframework.jmx.support.RegistrationPolicy
import org.springframework.test.context.ContextConfiguration
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Every Kotlin `publish` / `awaitPublish` overload of [Kontext] appends exactly one event: the business event itself,
 * recorded as the publisher's completed step.
 *
 * @author Stefan Dragisic
 */
@SpringBootTest(
    classes = [KotlinPublishWorkflowIT.TestConfig::class],
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["axon.axonserver.enabled=false"]
)
class KotlinPublishWorkflowIT {

    @Autowired
    private lateinit var eventSink: EventSink

    @Autowired
    private lateinit var eventStorageEngine: EventStorageEngine

    @Test
    fun `each publish overload appends the event once as the completed step`() {
        // given
        val trigger = GenericEventMessage(
            MessageType(QualifiedName("io.acme.kt.OrderPlaced")),
            mapOf("orderId" to "order-1")
        )

        // when
        eventSink.publish(null, trigger)

        // then: one event per call, in call order, each recorded as a completed publish step
        await().atMost(Duration.ofSeconds(10)).untilAsserted {
            assertThat(publishedEvents())
                .extracting<String> { it.type().qualifiedName().toString() }
                .containsExactly(
                    "io.acme.kt.OrderAnnounced",
                    "io.acme.kt.OrderApproved",
                    "io.acme.kt.OrderShipped",
                    "io.acme.kt.OrderReleased"
                )
        }
        val published = publishedEvents()
        assertThat(published)
            .extracting<String> { it.metadata()[METADATA_KEY_STEP_NAME] }
            .containsExactly("announce", "notifyApproved", "notifyShipped", "release")
        val workflowId = published.first().metadata()[METADATA_KEY_WORKFLOW_ID]
        assertThat(workflowId).isNotNull()
        assertThat(published).allSatisfy {
            assertThat(it.metadata())
                .containsEntry(METADATA_KEY_WORKFLOW_ID, workflowId)
                .containsEntry(METADATA_KEY_TYPE, StepStatus.COMPLETED.name)
        }
        // the payload overload resolves the type through the MessageTypeResolver and keeps user metadata
        assertThat(published[0].payloadAs(OrderAnnounced::class.java)).isEqualTo(OrderAnnounced("order-1"))
        assertThat(published[0].metadata()).containsEntry("channel", "email")
        // the EventMessage overload publishes the given message type as-is
        assertThat(published[2].payloadAs(OrderShipped::class.java)).isEqualTo(OrderShipped("order-1"))
    }

    private fun publishedEvents(): List<EventMessage> {
        val events = mutableListOf<EventMessage>()
        val stream = eventStorageEngine.source(SourcingCondition.conditionFor(EventCriteria.havingAnyTag()))
        try {
            stream.reduce(Unit) { _, entry ->
                val event = entry.message()
                if (event !is TerminalEventMessage && isPublishStep(event.metadata())) {
                    events.add(event)
                }
            }.orTimeout(5, TimeUnit.SECONDS).join()
        } finally {
            stream.close()
        }
        return events
    }

    @ContextConfiguration
    @EnableAutoConfiguration
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    class TestConfig {

        @Bean
        fun publishingWorkflow() = PublishingWorkflow()

        @Bean
        fun tokenStore(): TokenStore = InMemoryTokenStore()

        @Bean
        fun eventStorageEngine(): EventStorageEngine = InMemoryEventStorageEngine()
    }

    class PublishingWorkflow {

        @Workflow(
            workflowName = "KotlinPublishing",
            startOnEventName = "io.acme.kt.OrderPlaced",
            idProperty = "orderId"
        )
        fun Kontext.onExecute() {
            val orderId = payload["orderId"] as String
            // publish(stepName, payload, metadata)
            val announced = publish("announce", OrderAnnounced(orderId), Metadata.with("channel", "email"))
            // awaitPublish(stepName, payload)
            awaitPublish("notifyApproved", OrderApproved(orderId, "alice"))
            // publish(stepName, event)
            val shipped = publish("notifyShipped", eventMessage("io.acme.kt.OrderShipped", OrderShipped(orderId)))
            // awaitPublish(stepName, event)
            awaitPublish("release", eventMessage("io.acme.kt.OrderReleased", OrderReleased(orderId)))
            announced.await()
            shipped.await()
        }

        private fun eventMessage(name: String, payload: Any): EventMessage =
            GenericEventMessage(MessageType(QualifiedName(name)), payload)
    }

    @Event(namespace = "io.acme.kt", name = "OrderAnnounced")
    data class OrderAnnounced(val orderId: String)

    @Event(namespace = "io.acme.kt", name = "OrderApproved")
    data class OrderApproved(val orderId: String, val approvedBy: String)

    data class OrderShipped(val orderId: String)

    data class OrderReleased(val orderId: String)
}
