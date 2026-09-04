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
package io.axoniq.framework.workflow.query.api;

import io.axoniq.framework.workflow.runtime.api.execution.status.StepStatus;
import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import org.axonframework.messaging.core.MessageType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WorkflowStateQueryTest {

    @Test
    void exposesAllExplicitStateValueCriteria() {
        var query = WorkflowStateQuery.all()
                                         .workflowId("order-42")
                                         .workflowDefinitionId(new MessageType("PaymentWorkflow", "2.0"))
                                         .workflowStatus(WorkflowStatus.STARTED)
                                         .step("reserve-funds")
                                         .stepStatus("reserve-funds", StepStatus.COMPLETED)
                                         .payloadValue("orderId", "order-42")
                                         .version("payment-retry", "2.0")
                                         .versionMigration("payment-retry");

        assertThat(query.criteria()).containsExactly(
                new WorkflowStateQuery.WorkflowIdCriterion("order-42"),
                new WorkflowStateQuery.WorkflowDefinitionIdCriterion(new MessageType("PaymentWorkflow", "2.0")),
                new WorkflowStateQuery.WorkflowStatusCriterion(WorkflowStatus.STARTED),
                new WorkflowStateQuery.StepCriterion("reserve-funds"),
                new WorkflowStateQuery.StepStatusCriterion("reserve-funds", StepStatus.COMPLETED),
                new WorkflowStateQuery.PayloadValueCriterion("orderId", "order-42"),
                new WorkflowStateQuery.VersionCriterion("payment-retry", "2.0"),
                new WorkflowStateQuery.VersionMigrationCriterion("payment-retry")
        );
    }

    @Test
    void preservesTheOriginalQueryWhenAddingARestriction() {
        var unrestricted = WorkflowStateQuery.all();
        var restricted = unrestricted.workflowId("other-workflow");

        assertThat(unrestricted.criteria()).isEmpty();
        assertThat(restricted.criteria()).containsExactly(
                new WorkflowStateQuery.WorkflowIdCriterion("other-workflow")
        );
    }
}
