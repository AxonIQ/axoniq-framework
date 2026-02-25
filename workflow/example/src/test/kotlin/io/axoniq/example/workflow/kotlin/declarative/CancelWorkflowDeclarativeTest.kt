/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.example.workflow.kotlin.declarative

import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.api.AssociationProvider
import io.axoniq.workflow.runtime.api.EventCondition
import io.axoniq.workflow.runtime.api.EventNameCustomizerProvider
import io.axoniq.workflow.runtime.engine.configuration.PrettyPrintingRecordingEventStore
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace
import io.axoniq.workflow.runtime.api.WorkflowExecution
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Predicate

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWorkflowDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinitions(): Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<WorkflowKontext>> {
        val workflow = CancelWorkflow()
        return Consumer { d ->
            d.declarative("Cancel workflow in Kotlin")
                .on(EventCondition.fromType(RegistrationReceivedEvent::class.java))
                .workflowDefinition { WorkflowKontext.from(workflow::execute) }
                .eventNameCustomizer { EventNameCustomizerProvider { namespace("io.axoniq.dsl.cancel").workflowBaseName("Workflow") } }
                .workflowIdProvider {
                    AssociationProvider { trigger: MutableMap<String, Any?> ->
                        Optional.of(
                            "cancel-" + trigger["id"].toString()
                        )
                    }
                }
                .notCustomized()
        }
    }

    @Test
    fun `workflow is cancelled`() {

        delayedPublisher.addSchedules(
            listOf(
                DelayedPublisher.Schedule.ofMillis(
                    500,
                    RegistrationReceivedEvent("user-789", "cancel@test.com")
                )
            )
        )

        delayedPublisher.start()

        Awaitility.await().untilAsserted(ThrowingRunnable {
            assertThat(workflowEngine.workflowInstances()).isNotEmpty()
        })

        workflowEngine.runWorkflows()

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            assertThat(workflowEngine.workflowInstances())
                .allMatch(Predicate { h: WorkflowExecution? -> h!!.status.isTerminal })
        })

        // Wait 2 seconds before asserting to let async cleanup settle
        Thread.sleep(2_000)

        for (context in workflowEngine.workflowInstances().stream()
            .map(WorkflowExecution::workflowContext).toList()) {
            assertThat(context.getStatus().isTerminal).isTrue()
            assertThat(context.getStatus()).isEqualTo(WorkflowStatus.CANCELLED)
            assertThat(context.getStepHistory()).containsExactlyInAnyOrder("stepA", "stepB", "stepC")
        }

        // Verify all expected events were published
        val eventStore = PrettyPrintingRecordingEventStore.lastInstance()
        val eventTypes = eventStore.publishedEvents.map { it.type().qualifiedName().toString() }

        eventTypes.forEach { logger.info("  - $it") }
        assertThat(eventTypes).contains(
            "io.axoniq.dsl.cancel.WorkflowStarted",
            "io.axoniq.workflow.StepAStarted",
            "io.axoniq.workflow.StepBStarted",
            "io.axoniq.workflow.StepCStarted",
            "io.axoniq.workflow.StepACancelled",
            "io.axoniq.workflow.StepBCancelled",
            "io.axoniq.workflow.StepCCancelled",
            "io.axoniq.dsl.cancel.WorkflowCancelled"
        )
    }
}
