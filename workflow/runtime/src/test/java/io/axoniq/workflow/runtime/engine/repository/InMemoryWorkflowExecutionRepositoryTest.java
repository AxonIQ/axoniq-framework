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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.repository;

import io.axoniq.workflow.runtime.api.WorkflowExecution;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;

class InMemoryWorkflowExecutionRepositoryTest {

    private InMemoryWorkflowExecutionRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowExecutionRepository();
    }

    @Test
    void findByIdReturnsEmptyWhenNotFound() {
        assertThat(repository.findById("non-existent")).isEmpty();
    }

    @Test
    void findByIdReturnsStoredHandle() {
        var handle = createHandle("wf-1");
        repository.save(() -> handle);

        assertThat(repository.findById("wf-1")).isPresent().containsSame(handle);
    }

    @Test
    void findByIdRejectsNullWorkflowId() {
        assertThatNullPointerException()
                .isThrownBy(() -> repository.findById(null))
                .withMessageContaining("workflowId");
    }

    @Test
    void storeIfAbsentCreatesNewHandle() {
        var handle = createHandle("wf-1");
        var result = repository.save(() -> handle);

        assertThat(result).isSameAs(handle);
        assertThat(repository.findById("wf-1")).containsSame(handle);
    }

    @Test
    void storeIfAbsentReturnsExistingHandle() {
        var existing = createHandle("wf-1");
        var replacement = createHandle("wf-1");
        repository.save(() -> existing);

        var result = repository.save(() -> replacement);

        assertThat(result).isSameAs(existing);
    }

    @Test
    void storeIfAbsentRejectsNullFactory() {
        assertThatNullPointerException()
                .isThrownBy(() -> repository.save(null))
                .withMessageContaining("factory");
    }

    @Test
    void findAllReturnsAllHandles() {
        var handle1 = createHandle("wf-1");
        var handle2 = createHandle("wf-2");
        repository.save(() -> handle1);
        repository.save(() -> handle2);

        var all = repository.findAll();

        assertThat(all).containsExactlyInAnyOrder(handle1, handle2);
    }

    @Test
    void findAllReturnsEmptyWhenNoHandles() {
        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void findAllReturnsUnmodifiableCollection() {
        repository.save(() -> createHandle("wf-1"));

        var all = repository.findAll();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> all.add(createHandle("wf-x")));
    }

    @Test
    void clearRemovesAllHandles() {
        repository.save(() -> createHandle("wf-1"));
        repository.save(() -> createHandle("wf-2"));

        repository.clear();

        assertThat(repository.findAll()).isEmpty();
        assertThat(repository.findById("wf-1")).isEmpty();
        assertThat(repository.findById("wf-2")).isEmpty();
    }

    private static WorkflowExecution createHandle(String workflowId) {
        return new WorkflowExecution(workflowId, mock(), mock(), mock());
    }
}
