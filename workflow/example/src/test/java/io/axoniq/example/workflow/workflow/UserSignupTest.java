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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.runtime.api.WorkflowContext;
import io.axoniq.workflow.runtime.engine.association.Associations;
import io.axoniq.workflow.runtime.engine.configuration.WorkflowModule;
import io.axoniq.workflow.runtime.engine.execution.EventConditions;
import io.axoniq.workflow.runtime.engine.execution.WorkflowExecution;
import io.axoniq.workflow.runtime.engine.execution.WorkflowStatus;
import io.axoniq.workflow.runtime.engine.impl.WorkflowEngine;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static io.axoniq.workflow.runtime.engine.impl.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.engine.impl.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Simple workflow based on {@link SimpleWorkflowContext}.
 */
class UserSignupTest {

    /**
     * Test for declaration.
     */
    @Nested
    class DeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

        public DeclarativeTest() {
            super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
        }

        @Override
        protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
            return d -> d
                    .declarative(c -> new UserSignupWorkflow()::execute)
                    .workflowName("MyWorkflow")
                    .on(EventConditions
                                .fromType(
                                        RegistrationReceivedEvent.class,
                                        Associations.associate("status", "=", "vip")
                                )
                    )
                    .customized((c, w) -> w
                            .eventNameCustomizer(namespace("io.axoniq.dsl.wf.workflow"))
                            .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "signup-" + id))
                    );
        }

        @Test
        void shouldExecuteAllStepsOnFirstRun() {
            UserSignupTest.this.shouldExecuteAllStepsOnFirstRun(delayedPublisher, workflowEngine);
        }
    }

    /**
     * Test for autodetection.
     */
    @Nested
    class AutodetectedTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

        public AutodetectedTest() {
            super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
        }

        @Override
        protected UnaryOperator<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>> getDeclaredDefinitions() {
            return (d) -> d.autodetected(
                    c -> new UserSignupWorkflow(),
                    SimpleWorkflowContext.class
            );
        }

        @Test
        void shouldExecuteAllStepsOnFirstRun() {
            UserSignupTest.this.shouldExecuteAllStepsOnFirstRun(delayedPublisher, workflowEngine);
        }
    }


    void shouldExecuteAllStepsOnFirstRun(
            DelayedPublisher delayedPublisher, WorkflowEngine workflowEngine
    ) {

        delayedPublisher.addSchedules(List.of(
                ofMillis(
                        500,
                        new RegistrationReceivedEvent("1", "kermit@muppets.biz", "regular") // don't start
                ),
                ofMillis(
                        500,
                        new RegistrationReceivedEvent("2", "piggy@muppets.biz", "vip") // start
                ),
                ofMillis(
                        6100,
                        new MagicHappenedEvent("Saruman") // don't correlate
                ),
                ofMillis(
                        400,
                        new MagicHappenedEvent("Merlin") // correlate
                )
        ));

        // Arm the publisher to start the delayed execution
        delayedPublisher.start();

        // all started
        await().untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).isNotEmpty();
        });

        // simulate all-replayed and start workflows
        workflowEngine.runWorkflows();

        // run to the end
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowEngine.workflowInstances()).allMatch(h -> h.workflowStatus().isTerminal());
        });

        // Verify that both workflows executed all steps
        for (WorkflowContext context : workflowEngine.workflowInstances().stream()
                                                     .map(WorkflowExecution::workflowContext).toList()) {
            assertThat(context.workflowStatus().isTerminal()).isTrue();
            assertThat(context.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(context.workflowStepNames()).containsExactlyInAnyOrder(
                    "createUser",
                    "activateUser",
                    "sendWelcomeEmail",
                    "waitASecond",
                    "waitForMagicToHappen"
            );
            assertThat(context.workflowPayload().containsKey("magic"));
            assertThat(context.workflowPayload().containsKey("__createUser"));
        }
    }
}
