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

import io.axoniq.framework.workflow.dsl.api.StepStatus;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import org.axonframework.messaging.core.VersionedType;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowStateQueryTest {

    @Test
    void exposesAllExplicitStateValueCriteria() {
        var workflowDefinitionId = VersionedType.of("PaymentWorkflow", "2.0");
        var query = WorkflowStateQuery.all()
                                      .workflowId("order-42")
                                      .workflowDefinitionId(workflowDefinitionId)
                                      .workflowStatus(WorkflowStatus.STARTED)
                                      .step("reserve-funds")
                                      .stepStatus("reserve-funds", StepStatus.COMPLETED)
                                      .payloadValue("orderId", "order-42")
                                      .version("payment-retry", "2.0")
                                      .versionMigration("payment-retry");

        var criteria = query.criteria();
        assertThat(criteria).hasSize(8);
        assertThat((WorkflowStateQuery.WorkflowIdCriterion) criteria.get(0)).extracting(
                WorkflowStateQuery.WorkflowIdCriterion::workflowId
        ).isEqualTo("order-42");
        assertThat((WorkflowStateQuery.WorkflowDefinitionIdCriterion) criteria.get(1)).extracting(
                WorkflowStateQuery.WorkflowDefinitionIdCriterion::workflowDefinitionId
        ).isEqualTo(workflowDefinitionId);
        assertThat((WorkflowStateQuery.WorkflowStatusCriterion) criteria.get(2)).extracting(
                WorkflowStateQuery.WorkflowStatusCriterion::workflowStatus
        ).isEqualTo(WorkflowStatus.STARTED);
        assertThat((WorkflowStateQuery.StepCriterion) criteria.get(3)).extracting(
                WorkflowStateQuery.StepCriterion::stepName
        ).isEqualTo("reserve-funds");
        assertThat((WorkflowStateQuery.StepStatusCriterion) criteria.get(4)).extracting(
                WorkflowStateQuery.StepStatusCriterion::stepName,
                WorkflowStateQuery.StepStatusCriterion::stepStatus
        ).containsExactly("reserve-funds", StepStatus.COMPLETED);
        assertThat((WorkflowStateQuery.PayloadValueCriterion) criteria.get(5)).extracting(
                WorkflowStateQuery.PayloadValueCriterion::key,
                WorkflowStateQuery.PayloadValueCriterion::value
        ).containsExactly("orderId", "order-42");
        assertThat((WorkflowStateQuery.VersionCriterion) criteria.get(6)).extracting(
                WorkflowStateQuery.VersionCriterion::changeId,
                WorkflowStateQuery.VersionCriterion::version
        ).containsExactly("payment-retry", "2.0");
        assertThat((WorkflowStateQuery.VersionMigrationCriterion) criteria.get(7)).extracting(
                WorkflowStateQuery.VersionMigrationCriterion::changeId
        ).isEqualTo("payment-retry");
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

    @Test
    void createsAQueryForEveryInitialCriterion() {
        var definitionId = VersionedType.of("PaymentWorkflow", "2.0");

        assertThat(WorkflowStateQuery.byWorkflowId("order-42").criteria())
                .containsExactly(new WorkflowStateQuery.WorkflowIdCriterion("order-42"));
        assertThat(WorkflowStateQuery.byWorkflowDefinitionId(definitionId).criteria())
                .containsExactly(new WorkflowStateQuery.WorkflowDefinitionIdCriterion(definitionId));
        assertThat(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED).criteria())
                .containsExactly(new WorkflowStateQuery.WorkflowStatusCriterion(WorkflowStatus.STARTED));
        assertThat(WorkflowStateQuery.byStep("reserve-funds").criteria())
                .containsExactly(new WorkflowStateQuery.StepCriterion("reserve-funds"));
        assertThat(WorkflowStateQuery.byStepStatus("reserve-funds", StepStatus.COMPLETED).criteria())
                .containsExactly(new WorkflowStateQuery.StepStatusCriterion("reserve-funds", StepStatus.COMPLETED));
        assertThat(WorkflowStateQuery.byPayloadValue("orderId", null).criteria())
                .containsExactly(new WorkflowStateQuery.PayloadValueCriterion("orderId", null));
        assertThat(WorkflowStateQuery.byVersion("payment-retry", "2.0").criteria())
                .containsExactly(new WorkflowStateQuery.VersionCriterion("payment-retry", "2.0"));
        assertThat(WorkflowStateQuery.byVersionMigration("payment-retry").criteria())
                .containsExactly(new WorkflowStateQuery.VersionMigrationCriterion("payment-retry"));
    }

    @Test
    void rejectsNullOrEmptyStructuralStringCriteria() {
        assertThatThrownBy(() -> new WorkflowStateQuery.WorkflowIdCriterion(null)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.WorkflowIdCriterion("")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.StepCriterion("")).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.StepStatusCriterion("", StepStatus.STARTED))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.PayloadValueCriterion("", "value"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.VersionCriterion("", "1.0"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.VersionCriterion("change", ""))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.VersionMigrationCriterion(""))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.WorkflowDefinitionIdCriterion(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.WorkflowStatusCriterion(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new WorkflowStateQuery.StepStatusCriterion("reserve-funds", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void acceptsANullPayloadValue() {
        assertThat(new WorkflowStateQuery.PayloadValueCriterion("result", null).value()).isNull();
    }
}
