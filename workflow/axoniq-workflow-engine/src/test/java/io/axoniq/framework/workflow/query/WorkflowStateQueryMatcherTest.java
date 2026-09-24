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
package io.axoniq.framework.workflow.query;

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStep;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import org.axonframework.messaging.core.VersionedType;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WorkflowStateQueryMatcherTest {

    private static WorkflowState matchingState() {
        WorkflowState state = mock(WorkflowState.class);
        when(state.workflowId()).thenReturn("order-42");
        when(state.workflowDefinitionId()).thenReturn(VersionedType.of("PaymentWorkflow", "1.0"));
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.containsStep("reserve-funds")).thenReturn(true);
        when(state.getStep("reserve-funds")).thenReturn(step(StepStatus.COMPLETED));
        when(state.payload()).thenReturn(Map.of("orderId", "order-42"));
        when(state.versionFor("payment-retry")).thenReturn("2.0");
        when(state.hasVersionMigrationStep("payment-retry")).thenReturn(true);
        return state;
    }

    private static WorkflowStep step(StepStatus status) {
        return new WorkflowStep("reserve-funds", status, null, null, Instant.EPOCH, null);
    }

    @Test
    void matchesEverySupportedCriterion() {
        WorkflowState state = matchingState();

        var query = WorkflowStateQuery.all()
                                      .workflowId("order-42")
                                      .workflowDefinitionId(VersionedType.of("PaymentWorkflow", "1.0"))
                                      .workflowStatus(WorkflowStatus.STARTED)
                                      .step("reserve-funds")
                                      .stepStatus("reserve-funds", StepStatus.COMPLETED)
                                      .payloadValue("orderId", "order-42")
                                      .version("payment-retry", "2.0")
                                      .versionMigration("payment-retry");

        assertThat(WorkflowStateQueryMatcher.matches(query, state)).isTrue();
    }

    @Test
    void matchesAnUnrestrictedQuery() {
        assertThat(WorkflowStateQueryMatcher.matches(WorkflowStateQuery.all(), matchingState())).isTrue();
    }

    @Test
    void doesNotMatchAWorkflowId() {
        WorkflowState state = matchingState();
        when(state.workflowId()).thenReturn("order-43");

        assertThat(WorkflowStateQueryMatcher.matches(WorkflowStateQuery.byWorkflowId("order-42"), state))
                .isFalse();
    }

    @Test
    void doesNotMatchAWorkflowDefinitionId() {
        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byWorkflowDefinitionId(VersionedType.of("PaymentWorkflow", "2.0")),
                matchingState()
        )).isFalse();
    }

    @Test
    void doesNotMatchAWorkflowDefinitionName() {
        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byWorkflowDefinitionId(VersionedType.of("ShippingWorkflow", "1.0")),
                matchingState()
        )).isFalse();
    }

    @Test
    void doesNotMatchAWorkflowStatus() {
        WorkflowState state = matchingState();
        when(state.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);

        assertThat(WorkflowStateQueryMatcher.matches(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED),
                                                     state))
                .isFalse();
    }

    @Test
    void doesNotMatchWhenTheRequiredStepIsAbsent() {
        WorkflowState state = matchingState();
        when(state.containsStep("reserve-funds")).thenReturn(false);

        assertThat(WorkflowStateQueryMatcher.matches(WorkflowStateQuery.byStep("reserve-funds"), state)).isFalse();
    }

    @Test
    void doesNotMatchWhenTheRequiredStepStatusHasNoStep() {
        WorkflowState state = matchingState();
        when(state.getStep("reserve-funds")).thenReturn(null);

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byStepStatus("reserve-funds", StepStatus.COMPLETED), state
        )).isFalse();
    }

    @Test
    void doesNotMatchWhenTheRequiredStepStatusDiffers() {
        WorkflowState state = matchingState();
        when(state.getStep("reserve-funds")).thenReturn(step(StepStatus.STARTED));

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byStepStatus("reserve-funds", StepStatus.COMPLETED), state
        )).isFalse();
    }

    @Test
    void doesNotMatchWhenThePayloadKeyIsAbsent() {
        WorkflowState state = matchingState();
        when(state.payload()).thenReturn(Map.of());

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byPayloadValue("orderId", "order-42"), state
        )).isFalse();
    }

    @Test
    void doesNotMatchWhenThePayloadValueDiffers() {
        WorkflowState state = matchingState();
        when(state.payload()).thenReturn(Map.of("orderId", "order-43"));

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byPayloadValue("orderId", "order-42"), state
        )).isFalse();
    }

    @Test
    void doesNotMatchAnEffectiveVersion() {
        WorkflowState state = matchingState();
        when(state.versionFor("payment-retry")).thenReturn("1.0");

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byVersion("payment-retry", "2.0"), state
        )).isFalse();
    }

    @Test
    void doesNotMatchWhenTheVersionMigrationIsAbsent() {
        WorkflowState state = matchingState();
        when(state.hasVersionMigrationStep("payment-retry")).thenReturn(false);

        assertThat(WorkflowStateQueryMatcher.matches(
                WorkflowStateQuery.byVersionMigration("payment-retry"), state
        )).isFalse();
    }
}
