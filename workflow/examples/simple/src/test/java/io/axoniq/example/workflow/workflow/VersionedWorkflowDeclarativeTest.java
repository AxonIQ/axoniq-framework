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
package io.axoniq.example.workflow.workflow;

import io.axoniq.example.workflow.fixture.OrderPlacedEvent;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.DetectionPhase;
import io.axoniq.workflow.configuration.WorkflowModule.WorkflowDefinitionPhase.FinalizedPhase;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContext;
import io.axoniq.workflow.dsl.simple.SimpleWorkflowContextFactory;
import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.runtime.api.execution.context.EventConditions;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.test.AbstractDeclarativeTestBase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.namespace;
import static io.axoniq.workflow.runtime.execution.PayloadPropertyWorkflowIdProvider.fromPayloadAttribute;
import static io.axoniq.workflow.runtime.test.utils.DelayedPublisher.Schedule.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for {@link VersionedWorkflow} — verifies that a fresh workflow bumps to version
 * {@code "0.0.2"} via the marker and takes the new branch.
 *
 * @author Stefan Dragisic
 */
class VersionedWorkflowDeclarativeTest extends AbstractDeclarativeTestBase<SimpleWorkflowContext> {

    public VersionedWorkflowDeclarativeTest() {
        super(SimpleWorkflowContext.class, c -> new SimpleWorkflowContextFactory());
    }

    @Override
    protected Function<DetectionPhase<SimpleWorkflowContext>, FinalizedPhase<SimpleWorkflowContext>> getDeclaredDefinition() {
        var workflow = new VersionedWorkflow();
        return d -> d
                .declarative(c -> workflow::execute)
                .workflowName("Versioned workflow")
                .on(EventConditions.fromType(OrderPlacedEvent.class))
                .customized((c, w) -> w
                        .eventNameCustomizer(namespace("io.axoniq.dsl.versioned").workflowBaseName("Workflow"))
                        .workflowIdProvider(fromPayloadAttribute(c, "orderId", id -> "order-" + id))
                );
    }

    @Test
    void freshWorkflow_recordsV2Marker_andTakesNewBranch() {
        delayedPublisher.addSchedules(List.of(
                ofMillis(500, new OrderPlacedEvent("order-v2-1", "customer-1"))
        ));
        delayedPublisher.start();

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(workflowHistoryRepository.findAll()).isNotEmpty();
            assertThat(workflowHistoryRepository.findAll())
                    .allMatch(e -> e.state().workflowStatus().isTerminal());
        });

        // Exactly one workflow was started — engine deduplicates by workflow ID derived from orderId.
        assertThat(workflowHistoryRepository.findAll()).hasSize(1);

        for (WorkflowState state : workflowHistoryRepository.findAll().stream()
                                                            .map(WorkflowHistory::state).toList()) {
            assertThat(state.workflowStatus()).isEqualTo(WorkflowStatus.COMPLETED);
            // The marker is projected into state.
            assertThat(state.hasVersionMigrationStep("payment-redesign")).isTrue();
            assertThat(state.currentWorkflowVersion("payment-redesign")).isEqualTo("0.0.2");
            // Migration step also bumps the workflow's current version.
            assertThat(state.workflowDefinitionVersion()).isEqualTo("0.0.2");
            // Workflow took the v0.0.2 branch — processPayment is present, chargePayment is not.
            assertThat(state.workflowStepNames()).contains("reserveStock", "processPayment");
            assertThat(state.workflowStepNames()).doesNotContain("chargePayment");
        }
    }
}
