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
package io.axoniq.example.workflow.kotlin.workflow


import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule
import io.axoniq.workflow.runtime.engine.execution.EventConditions
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace
import io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import java.util.function.UnaryOperator

/**
 * @author Stefan Dragisic
 * @since 1.0.0
 */
class CancelWorkflowDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinitions(): UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WorkflowKontext>> {
        val workflow = CancelWorkflow()
        return UnaryOperator { d ->
            d.declarative({ c -> WorkflowKontext.from(workflow::execute) })
                .workflowName("Cancel workflow in Kotlin")
                .on(EventConditions.fromType(RegistrationReceivedEvent::class.java))
                .customized { c, wc ->
                    wc.eventNameCustomizer(namespace("io.axoniq.dsl.cancel").workflowBaseName("Workflow"))
                        .workflowIdProvider(
                            fromPayloadAttribute(
                                c, "id",
                                UnaryOperator { id: String? -> "cancel-$id" })
                        )
                }
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
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty()
        })

        workflowEngine.runWorkflows()

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            assertThat(workflowEngine.workflowExecutions())
                .allMatch(Predicate { h: WorkflowExecution? -> h!!.state().workflowStatus().isTerminal })
        })

        // Wait 2 seconds before asserting to let async cleanup settle
        Thread.sleep(2_000)

        for (state in workflowEngine.workflowExecutions().stream()
            .map(WorkflowExecution::state).toList()) {
            assertThat(state.workflowStatus().isTerminal).isTrue()
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.CANCELLED)
            assertThat(state.workflowStepNames()).containsExactlyInAnyOrder("stepA", "stepB", "stepC")
        }
    }
}
