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

import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.query.utils.WorkflowStateQueryMatcher;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.framework.workflow.runtime.api.execution.state.WorkflowStep;
import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.messaging.core.VersionedType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkflowStateQueryMatcherTest {

    @Test
    void matchesEverySupportedCriterion() {
        WorkflowState state = mock(WorkflowState.class);
        var definitionId = VersionedType.of("PaymentWorkflow", "1.0");
        when(state.workflowId()).thenReturn("order-42");
        when(state.workflowDefinitionId()).thenReturn(definitionId);
        when(state.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(state.containsStep("reserve-funds")).thenReturn(true);
        when(state.getStep("reserve-funds")).thenReturn(
                new WorkflowStep("reserve-funds", StepStatus.COMPLETED, null, null, Instant.EPOCH, null)
        );
        when(state.payload()).thenReturn(Map.of("orderId", "order-42"));
        when(state.versionFor("payment-retry")).thenReturn("2.0");
        when(state.hasVersionMigrationStep("payment-retry")).thenReturn(true);

        var query = WorkflowStateQuery.all()
                                      .workflowId("order-42")
                                      .workflowDefinitionId(definitionId)
                                      .workflowStatus(WorkflowStatus.STARTED)
                                      .step("reserve-funds")
                                      .stepStatus("reserve-funds", StepStatus.COMPLETED)
                                      .payloadValue("orderId", "order-42")
                                      .version("payment-retry", "2.0")
                                      .versionMigration("payment-retry");

        assertThat(WorkflowStateQueryMatcher.matches(query, state)).isTrue();
    }

    @Test
    void doesNotMatchAStateThatViolatesACriterion() {
        WorkflowState state = mock(WorkflowState.class);
        when(state.workflowId()).thenReturn("order-43");

        assertThat(WorkflowStateQueryMatcher.matches(WorkflowStateQuery.all().workflowId("order-42"), state))
                .isFalse();
    }
}
