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

import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.api.execution.context.EventConditions
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.function.Function
import java.util.function.Predicate
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class SleepKotlinDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinition(): Function<DetectionPhase<WorkflowKontext>, FinalizedPhase<WorkflowKontext>> {
        return Function { d ->
            d.declarative {
                WorkflowKontext.from {
                    val cooldown = waitForEvent("cooldown", EventConditions.never(), timeout = 500.milliseconds)
                    val work = execute("work", timeout = 5.seconds) { _, _ -> mapOf("done" to true) }

                    anyMatch({ it.isCompleted }, cooldown, work).await()
                }
            }
                .workflowName("Sleep Kotlin Workflow")
                .on(EventConditions.fromType(RegistrationReceivedEvent::class.java))
                .notCustomized()
        }
    }

    @Test
    fun `complete workflow with sleep`() {
        delayedPublisher.addSchedules(
            listOf(
                DelayedPublisher.Schedule.ofMillis(
                    100,
                    RegistrationReceivedEvent("user-sleep-kt", "kt@test.com")
                )
            )
        )

        delayedPublisher.start()

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty()
            assertThat(workflowHistoryRepository.findAll())
                .allMatch(Predicate { h -> h.state().workflowStatus().isTerminal })
        })

        val history = workflowHistoryRepository.findAll().first()
        assertThat(history.state().workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED)
        assertThat(history.state().workflowStepNames()).contains("cooldown", "work")
    }
}
