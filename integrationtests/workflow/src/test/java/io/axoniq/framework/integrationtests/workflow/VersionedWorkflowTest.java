/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.framework.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.framework.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.framework.workflow.annotation.Workflow;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

import static io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link VersionedWorkflow} — verifies that a fresh workflow bumps to version {@code "0.0.2"} via
 * the marker and takes the new branch.
 *
 * @author Stefan Dragisic
 */
class VersionedWorkflowTest extends AbstractWorkflowIntegrationTestBase<SimpleWorkflowContext> {

    public VersionedWorkflowTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        return d -> d
                .autodetected(c -> new VersionedWorkflow());
    }

    @Test
    void freshWorkflowRecordsV2MarkerAndTakesNewBranch() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-v2-1", "customer-1"))
        ));
        delayedPublisher.start();

        testDriver.historyMatches(h -> h.state().workflowStatus() == WorkflowStatus.COMPLETED);

        var state = testDriver.testingState().state();
        assertThat(state.hasVersionMigrationStep("payment-redesign")).isTrue();
        assertThat(state.versionFor("payment-redesign")).isEqualTo("0.0.2");
        assertThat(state.workflowDefinitionId().version()).isEqualTo("0.0.2");
        testDriver.testingState().hasSteps("reserveStock", "processPayment");
        testDriver.testingState().noStep("chargePayment");
    }

    public static class VersionedWorkflow {

        private static final Logger logger = LoggerFactory.getLogger(VersionedWorkflow.class);

        @Workflow(
                workflowName = "Workflow",
                workflowNamespace = "io.axoniq.dsl.versioned",
                idProperty = "orderId",
                startOnEventClass = OrderPlacedEvent.class
        )
        public void execute(SimpleWorkflowContext ctx) {
            ctx.awaitExecute("reserveStock", Boolean.class, () -> {
                logger.info("Reserving stock for {}", ctx.workflowPayload());
                return true;
            });

            if (ctx.migrateVersion("payment-redesign", "0.0.2")) {
                ctx.awaitExecute("processPayment", Boolean.class, () -> {
                    logger.info("Processing payment (v0.0.2 branch)");
                    return true;
                });
            } else {
                ctx.awaitExecute("chargePayment", Boolean.class, () -> {
                    logger.info("Charging payment (legacy v0.0.1 branch)");
                    return true;
                });
            }
        }
    }

    @Event(namespace = "my.custom", name = "OrderPlaced")
    public record OrderPlacedEvent(String orderId, String customerId) {

    }
}
