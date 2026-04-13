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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.MagicHappenedEvent;
import io.axoniq.example.workflow.fixture.RegistrationReceivedEvent;
import io.axoniq.workflow.configuration.WorkflowModule;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowStatusChangeListener;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowEngine;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import io.axoniq.workflow.runtime.test.utils.DelayedPublisher;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.dsl.api.AssociationsUtils.associate;
import static io.axoniq.workflow.dsl.simple.SimpleWorkflowContext.equalsTo;
import static io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever.payloadProperty;
import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Simple workflow based on {@link SimpleWorkflowContext}.
 *
 * @author Stefan Dragisic
 * @author Simon Zambrovski
 * @since 1.0.0
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
        protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
            return d -> d
                    .declarative(c -> new UserSignupWorkflow()::execute)
                    .workflowName("MyWorkflow")
                    .on(EventConditions
                                .fromType(
                                        RegistrationReceivedEvent.class,
                                        associate(payloadProperty("status"), equalsTo("vip"))
                                )
                    )
                    .customized((c, w) -> w
                            .eventNameCustomizer(namespace("io.axoniq.dsl.wf.workflow"))
                            .workflowIdProvider(fromPayloadAttribute(c, "id", id -> "signup-" + id))
                            .registerWorkflowStatusChangeListener(WorkflowStatus.COMPLETED,
                                                                  new WorkflowStatusChangeListener() {
                                                                      @Override
                                                                      public <C extends WorkflowContext> void onWorkflowStatus(
                                                                              @Nonnull WorkflowStatus state,
                                                                              @Nonnull C context) {
                                                                          new UserSignupWorkflow().onFinish(state,
                                                                                                            (SimpleWorkflowContext) context);
                                                                      }
                                                                  }
                            )
                    );
        }

        @Test
        void shouldExecuteAllStepsOnFirstRun() {
            UserSignupTest.this.shouldExecuteAllStepsOnFirstRun(delayedPublisher,
                                                                workflowEngine,
                                                                workflowHistoryRepository);
        }
    }

    @Nested
    class AutodetectedTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

        public AutodetectedTest() {
            super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
        }

        @Override
        protected Function<WorkflowModule.WorkflowDefinitionPhase.DetectionPhase<SimpleWorkflowContext>, WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
            return d -> d
                    .autodetected(c -> new UserSignupWorkflow());
        }

        @Test
        void shouldExecuteAllStepsOnFirstRun() {
            UserSignupTest.this.shouldExecuteAllStepsOnFirstRun(delayedPublisher,
                                                                workflowEngine,
                                                                workflowHistoryRepository);
        }
    }


    void shouldExecuteAllStepsOnFirstRun(
            DelayedPublisher delayedPublisher,
            WorkflowEngine workflowEngine,
            WorkflowHistoryRepository workflowHistoryRepository
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
            assertThat(workflowEngine.workflowExecutions()).isNotEmpty();
        });

        // simulate all-replayed and start workflows

        assertThat(workflowEngine.workflowExecutions()).hasSize(1);

        // run to the end
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(h -> h.state().workflowStatus().isTerminal());
        });

        assertThat(workflowEngine.workflowExecutions()).isEmpty(); // no running workflows anymore

        assertThat(workflowHistoryRepository.findAll()).hasSize(1); // history is still present

        // Verify that both workflows executed all steps
        for (WorkflowHistory workflowHistory : workflowHistoryRepository.findAll()) {
            var state = workflowHistory.state();
            assertThat(state.workflowStatus().isTerminal()).isTrue();
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            assertThat(state.workflowStepNames()).containsExactly(
                    "createUser",
                    "activateUser",
                    "sendWelcomeEmail",
                    "waitASecond",
                    "waitForMagicToHappen",
                    "modifyPayload"
            );

            var payload = workflowHistory.state().payload();
            assertThat(payload.containsKey("magic"));
            assertThat(payload.containsKey("__createUser"));
        }
    }
}
