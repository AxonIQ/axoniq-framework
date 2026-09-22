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
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE;
import static io.axoniq.framework.workflow.runtime.execution.WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RunningWorkflowsTest {

    @Test
    void workflowLifecycleEventsSelectLifecycleTaggedEvents() {
        assertThat(EventSourcedRunningWorkflows.criteriaBuilder()).isEqualTo(
                EventCriteria.havingTags(Tag.of(
                        TAG_WORKFLOW_EVENT_TYPE,
                        TAG_VALUE_EVENT_TYPE_LIFECYCLE
                ))
        );
    }

    @Test
    void startedStatusAddsWorkflowIdAndUpdatesContains() {
        var state = new EventSourcedRunningWorkflows();

        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.STARTED));

        assertThat(state.workflowIds()).containsExactly("wf-123");
        assertThat(state.contains("wf-123")).isTrue();
        assertThat(state.contains("wf-456")).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = WorkflowStatus.class, names = {"COMPLETED", "FAILED", "CANCELLED", "TIMED_OUT"})
    void terminalStatusesRemoveWorkflowIds(WorkflowStatus status) {
        var state = new EventSourcedRunningWorkflows();

        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.STARTED));
        state.evolve(MetadataUtils.create("wf-123", status));

        assertThat(state.workflowIds()).isEmpty();
        assertThat(state.contains("wf-123")).isFalse();
    }

    @Test
    void noneStatusDoesNotChangeRunningWorkflowIds() {
        var state = new EventSourcedRunningWorkflows();

        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.STARTED));
        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.NONE));

        assertThat(state.workflowIds()).containsExactly("wf-123");
        assertThat(state.contains("wf-123")).isTrue();
    }

    @Test
    void metadataWithoutWorkflowStatusDoesNotChangeRunningWorkflowIds() {
        var state = new EventSourcedRunningWorkflows();

        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.STARTED));
        state.evolve(MetadataUtils.create("wf-123"));

        assertThat(state.workflowIds()).containsExactly("wf-123");
    }

    @Test
    void workflowIdsReturnsImmutableCopy() {
        var state = new EventSourcedRunningWorkflows();

        state.evolve(MetadataUtils.create("wf-123", WorkflowStatus.STARTED));

        assertThatThrownBy(() -> state.workflowIds().add("wf-456"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(state.workflowIds()).containsExactly("wf-123");
    }
}
