package io.axoniq.workflow.runtime.kotlin.usersignup

import io.axoniq.example.workflow.kotlin.MagicHappenedEvent
import io.axoniq.example.workflow.kotlin.RegistrationReceivedEvent
import io.axoniq.example.workflow.kotlin.usersignup.UserSignupWorkflow
import io.axoniq.workflow.dsl.kotlin.WorkflowKontext
import io.axoniq.workflow.dsl.kotlin.WorkflowKontextFactory
import io.axoniq.workflow.runtime.api.AssociationProvider
import io.axoniq.workflow.runtime.api.EventCondition
import io.axoniq.workflow.runtime.api.EventNameCustomizer
import io.axoniq.workflow.runtime.api.EventNameCustomizerProvider
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer
import io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine
import io.axoniq.workflow.runtime.test.AbstractTestBase
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher
import io.github.oshai.kotlinlogging.KotlinLogging
import org.assertj.core.api.Assertions
import org.awaitility.Awaitility
import org.awaitility.core.ThrowingRunnable
import org.axonframework.common.configuration.Configuration
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Predicate

val logger = KotlinLogging.logger {}

class UserSignupTest : AbstractTestBase<WorkflowKontext>(
    WorkflowKontext::class.java,
    { c: Configuration ->
        WorkflowKontextFactory(
            c.getComponent(EventNameCustomizer::class.java) { DefaultEventNameCustomizer.Builder.eventName() }
        )
    }
) {

    override fun getDefinitions(): Consumer<WorkflowModule.WorkflowDefinitionPhase.DefinitionPhase<WorkflowKontext>> {
        val workflow = UserSignupWorkflow()
        return Consumer { d ->
            d.declarative("User signup workflow in Kotlin")
                .on(EventCondition.fromType(RegistrationReceivedEvent::class.java))
                .workflowDefinition { workflow }
                .eventNameCustomizer { EventNameCustomizerProvider { namespace("io.axoniq.dsl.wf") } }
                .workflowIdProvider {
                    AssociationProvider { trigger: MutableMap<String, Any?> ->
                        Optional.of(
                            "signup-" + trigger["id"].toString()
                        )
                    }
                }
                .notCustomized()
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