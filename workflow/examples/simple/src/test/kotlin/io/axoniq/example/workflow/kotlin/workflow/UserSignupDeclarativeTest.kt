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
package io.axoniq.example.workflow.kotlin.workflow

import io.axoniq.example.workflow.kotlin.fixture.MagicHappenedEvent
import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.history.api.WorkflowHistory
import io.axoniq.workflow.runtime.api.execution.context.EventConditions
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus
import io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace
import io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Predicate

/**
 * User signup test using the declarative workflow API kotlin DSL.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
class UserSignupDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinition(): Function<DetectionPhase<WorkflowKontext>, FinalizedPhase<WorkflowKontext>> {
        return { d ->
            d.declarative { WorkflowKontext.from(UserSignupWorkflow()::execute) }
                .workflowName("User signup workflow in Kotlin")
                .on(EventConditions.fromType(RegistrationReceivedEvent::class.java))
                .customized { c, wc ->
                    wc.eventNameCustomizer(namespace("io.axoniq.dsl.wf"))
                        .workflowIdProvider(
                            fromPayloadAttribute(
                                c, "id"
                            ) { id: String? -> "signup-$id" }
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
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty()
        })

        // run to the end
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty()
            assertThat(workflowHistoryRepository.findAll())
                .allMatch(Predicate { h -> h.state().workflowStatus().isTerminal })
        })

        // Verify that both workflows executed all steps
        for (context in workflowHistoryRepository.findAll().stream()
            .map(WorkflowHistory::state).toList()) {
            assertThat(context.workflowStatus().isTerminal).isTrue()
            assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED)
            assertThat(context.workflowStepNames()).containsExactly(
                "createUser",
                "activateUser",
                "waitASecond",
                "sendWelcomeEmail",
                "waitForMagicToHappen"
            )
        }


    }

}