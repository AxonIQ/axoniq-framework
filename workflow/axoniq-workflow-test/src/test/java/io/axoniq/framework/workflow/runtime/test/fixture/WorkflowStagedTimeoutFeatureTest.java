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
package io.axoniq.framework.workflow.runtime.test.fixture;

import io.axoniq.framework.workflow.configuration.WorkflowModule;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.dsl.annotation.Workflow;
import io.axoniq.framework.workflow.dsl.api.EventConditions;
import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Test for BDD fixture time advancement.
 *
 * @author Simon Zambrovski
 */
class WorkflowStagedTimeoutFeatureTest {

    private static final Duration CONFIRMATION_TIMEOUT = Duration.ofMinutes(5);

    private WorkflowTestFixture<?, ?> fixture;

    @BeforeEach
    void setUp() {
        var module = WorkflowModule.defaults("TimeoutHandling", SimpleWorkflowContext.class)
                                   .workflowContextFactory(c -> new SimpleWorkflowContextFactory())
                                   .definition(d -> d.autodetected(c -> new TimeoutHandlingWorkflow()));
        fixture = WorkflowTestFixture.of(module, UnaryOperator.identity());
    }

    @AfterEach
    void tearDown() {
        fixture.then().stop();
    }

    @Test
    void passTimeTriggersTimeoutAndWorkflowHandlesIt() {
        fixture.given()
               .noExecution();

        fixture.when()
               .publishEvent(new PaymentRequested("payment-1"))
               .executionExists();

        fixture.then()
               .waitingIn("waitForConfirmation");

        fixture.when()
               .timePasses(CONFIRMATION_TIMEOUT.plusSeconds(1));

        fixture.then()
               .step("waitForConfirmation", StepStatus.TIMED_OUT)
               .waitingIn("handleTimeout");

        fixture.when()
               .executeReturning("handleTimeout", Map.of("handledBy", "timeout-handler"));

        fixture.then()
               .workflowFinished(WorkflowStatus.COMPLETED)
               .step("handleTimeout", StepStatus.COMPLETED)
               .stepsPassed("recordTimeout")
               .payloadContains(Map.of(
                       "paymentId", "payment-1",
                       "status", "timed-out",
                       "timeoutHandled", true
               ));
    }

    /**
     * Event that starts the timeout-handling test workflow.
     *
     * @param paymentId payment identifier
     */
    @Event(namespace = "io.axoniq.framework.workflow.test", name = "PaymentRequested")
    record PaymentRequested(String paymentId) {

    }

    /**
     * Test workflow that reacts to a timed-out wait step.
     */
    public static class TimeoutHandlingWorkflow {

        /**
         * Executes the timeout-handling workflow.
         *
         * @param ctx workflow context
         */
        @Workflow(
                workflowName = "TimeoutHandlingWorkflow",
                workflowNamespace = "io.axoniq.framework.workflow.test.timeout",
                idProperty = "paymentId",
                startOnEventClass = PaymentRequested.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            var confirmation = ctx.waitForEvent(
                    "waitForConfirmation",
                    EventConditions.never(),
                    step -> step.timeout(CONFIRMATION_TIMEOUT)
            );

            confirmation.await();

            if (confirmation.timeout()) {
                ctx.execute(
                        "handleTimeout",
                        Map.of("reason", "confirmation-timeout"),
                        (processingContext, payload) -> Map.of("handledBy", "workflow"),
                        step -> step.timeout(Duration.ofMinutes(10))
                ).await();
                ctx.awaitModifyPayload("recordTimeout", payload -> {
                    var updated = new HashMap<>(payload);
                    updated.put("status", "timed-out");
                    updated.put("timeoutHandled", true);
                    return updated;
                });
            }
        }
    }
}
