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

import io.axoniq.example.workflow.kotlin.fixture.MagicHappenedEvent
import io.axoniq.example.workflow.kotlin.fixture.RegistrationReceivedEvent
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.api.AssociationProvider
import io.axoniq.workflow.runtime.api.EventCondition
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import org.assertj.core.api.Assertions
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Predicate

class UserSignupDeclarativeTest : AbstractDeclarativeTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { WorkflowKontextFactory() }
) {

    override fun getDeclaredDefinitions(): Consumer<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<WorkflowKontext>> {
        val workflow = UserSignupWorkflow()
        return Consumer { d ->
            d.declarative("User signup workflow in Kotlin")
                .on(EventCondition.fromType(RegistrationReceivedEvent::class.java))
                .workflowDefinition { WorkflowKontext.from(workflow::execute) }
                .workflowIdProvider {
                    AssociationProvider { trigger: MutableMap<String, Any?> ->
                        Optional.of(
                            "signup-" + trigger["id"].toString()
                        )
                    }
                }
                .customized { _, c -> c.eventNameCustomizer(namespace("io.axoniq.dsl.wf")) }
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
        Awaitility.await().untilAsserted(ThrowingRunnable {
            Assertions.assertThat(workflowEngine.workflowInstances()).isNotEmpty()
        })

        // simulate all-replayed and start workflows
        workflowEngine.runWorkflows()


        // run to the end
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(ThrowingRunnable {
            Assertions.assertThat(workflowEngine.workflowInstances().values)
                .allMatch(Predicate { h: WorkflowEngine.ExecutionHandle? -> h!!.status.isTerminal })
        })


        // Verify that both workflows executed all steps
        for (context in workflowEngine.workflowInstances().values.stream()
            .map(WorkflowEngine.ExecutionHandle::workflowContext).toList()) {
            Assertions.assertThat(context.getStatus().isTerminal).isTrue()
            Assertions.assertThat(context.getStatus()).isEqualTo(WorkflowStatus.COMPLETED)
            Assertions.assertThat(context.getStepHistory()).containsExactlyInAnyOrder(
                "createUser",
                "activateUser",
                "sendWelcomeEmail",
                "waitASecond",
                "waitForMagicToHappen"
            )
        }


    }

}