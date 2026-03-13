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

import io.axoniq.example.workflow.kotlin.fixture.MagicHappenedEvent
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
import org.assertj.core.api.Assertions
import org.awaitility.Awaitility.await
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import java.util.function.UnaryOperator

class UserSignupDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinitions(): UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WorkflowKontext>> {
        return UnaryOperator { d ->
            d.declarative({ c -> WorkflowKontext.from(UserSignupWorkflow()::execute) })
                .workflowName("User signup workflow in Kotlin")
                .on(EventConditions.fromType(RegistrationReceivedEvent::class.java))
                .customized { c, wc ->
                    wc.eventNameCustomizer(namespace("io.axoniq.dsl.wf"))
                        .workflowIdProvider(
                            fromPayloadAttribute(
                                c, "id",
                                UnaryOperator { id: String? -> "signup-$id" })
                        )
                }
        }
    }


    @Test
    fun `register user`() {

        delayedPublisher.addSchedules(
            listOf<DelayedPublisher.Schedule>(
                DelayedPublisher.Schedule.ofMillis(
                    500,
                    RegistrationReceivedEvent("user-456", "kermit@muppets.biz")
                ),
                DelayedPublisher.Schedule.ofMillis(
                    6500,
                    MagicHappenedEvent("Merlin")
                )
            )
        )


        // Arm the publisher to start the delayed execution
        delayedPublisher.start()


        // all started
        await().untilAsserted(ThrowingRunnable {
            Assertions.assertThat(workflowEngine.workflowExecutions()).isNotEmpty()
        })

        // simulate all-replayed and start workflows
        workflowEngine.runWorkflows()


        // run to the end
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            Assertions.assertThat(workflowEngine.workflowExecutions())
                .allMatch(Predicate { h: WorkflowExecution -> h.state().workflowStatus().isTerminal })
        })


        // Verify that both workflows executed all steps
        for (context in workflowEngine.workflowExecutions().stream()
            .map(WorkflowExecution::state).toList()) {
            Assertions.assertThat(context.workflowStatus().isTerminal).isTrue()
            Assertions.assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED)
            Assertions.assertThat(context.workflowStepNames()).containsExactly(
                "createUser",
                "activateUser",
                "waitASecond",
                "sendWelcomeEmail",
                "waitForMagicToHappen"
            )
        }


    }

}