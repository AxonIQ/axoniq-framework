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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.example.workflow.kotlin.workflow

import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.configuration.WorkflowModule
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.api.execution.context.EventConditions
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import java.util.function.UnaryOperator

class KotlinRetryWorkflowDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {
    private val workflow = KotlinRetryWorkflow()

    override fun getDeclaredDefinitions(): UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WorkflowKontext>> {
        return UnaryOperator { d ->
            d.declarative({ c -> WorkflowKontext.from(workflow::execute) })
                .workflowName("Retry workflow in Kotlin")
                .on(EventConditions.fromType(RegistrationReceivedEvent::class.java))
                .customized { c, w -> w }
        }
    }

    @Test
    fun `workflow retries and succeeds`() {
        delayedPublisher.addSchedules(
            listOf(
                DelayedPublisher.Schedule.ofMillis(
                    100,
                    RegistrationReceivedEvent("user-retry", "retry@test.com")
                )
            )
        )

        delayedPublisher.start()

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            assertThat(workflowHistoryRepository.findAll())
                .isNotEmpty
                .allMatch(Predicate { h -> h!!.state().workflowStatus().isTerminal })
        })

        val executions = workflowHistoryRepository.findAll()
        assertThat(executions).hasSize(1)
        val state = executions.first().state()
        assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED)
        assertThat(workflow.retryAttempts.get()).isEqualTo(3)
        assertThat(workflow.asyncRetryAttempts.get()).isEqualTo(2)
    }
}
