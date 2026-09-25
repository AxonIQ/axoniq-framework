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

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.infra.ComponentDescriptor;
import org.junit.jupiter.api.*;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class InMemoryWorkflowExecutionRepositoryTest {

    private static WorkflowExecution execution(String workflowId, WorkflowStatus workflowStatus) {
        var state = mock(WorkflowState.class);
        when(state.workflowId()).thenReturn(workflowId);
        when(state.workflowStatus()).thenReturn(workflowStatus);

        var execution = mock(WorkflowExecution.class);
        when(execution.state()).thenReturn(state);
        return execution;
    }

    @Test
    void savesAnExecutionOnceAndFindsItByIdentifierAndPredicate() {
        var repository = new InMemoryWorkflowExecutionRepository();
        var matching = execution("order-42", WorkflowStatus.STARTED);
        repository.save("order-42", () -> matching);
        var duplicateFactoryCalled = new AtomicBoolean();

        var savedAgain = repository.save("order-42", () -> {
            duplicateFactoryCalled.set(true);
            return execution("order-42", WorkflowStatus.COMPLETED);
        });

        assertThat(savedAgain).isSameAs(matching);
        assertThat(duplicateFactoryCalled).isFalse();
        assertThat(repository.findById("order-42")).containsSame(matching);
        assertThat(repository.findById("unknown")).isEmpty();
        assertThat(repository.findAll(execution -> execution == matching)).containsExactly(matching);
        assertThat(repository.findAll()).containsExactly(matching);
        assertThatThrownBy(() -> repository.findById(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> repository.save("order-43", null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void findsOnlyExecutionsWhoseStateMatchesTheQuery() {
        var repository = new InMemoryWorkflowExecutionRepository();
        var matching = execution("order-42", WorkflowStatus.STARTED);
        var nonMatching = execution("order-43", WorkflowStatus.COMPLETED);
        repository.save("order-42", () -> matching);
        repository.save("order-43", () -> nonMatching);

        var result = repository.findAll(WorkflowStateQuery.byWorkflowStatus(WorkflowStatus.STARTED));

        assertThat(result).isCompleted();
        assertThat(result.join()).containsExactly(matching);
    }

    @Test
    void removesExecutionsAndDescribesTheCurrentContents() {
        var repository = new InMemoryWorkflowExecutionRepository();
        var first = execution("order-42", WorkflowStatus.STARTED);
        var second = execution("order-43", WorkflowStatus.COMPLETED);
        repository.save("order-42", () -> first);
        repository.save("order-43", () -> second);
        var descriptor = mock(ComponentDescriptor.class);

        repository.describeTo(descriptor);
        repository.removeAll(execution -> execution == second);

        verify(descriptor).describeProperty("size", 2);
        verify(descriptor).describeProperty(eq("workflowIds"), any(Collection.class));
        assertThat(repository.remove("order-42")).isSameAs(first);
        assertThat(repository.remove("unknown")).isNull();
        repository.clear();
        assertThat(repository.findAll()).isEmpty();
    }
}
