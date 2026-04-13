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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.history;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.workflow.runtime.execution.EventSourcedWorkflowState;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryWorkflowHistoryRepositoryTest {

    private InMemoryWorkflowHistoryRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowHistoryRepository();
    }

    @Test
    void shouldSaveAndFindById() {
        String workflowId = "wf-1";
        WorkflowHistory history = new WorkflowHistory(workflowId, new EventSourcedWorkflowState());

        repository.save(history);

        Optional<WorkflowHistory> found = repository.findById(workflowId);
        assertThat(found).isPresent();
        assertThat(found.get()).isEqualTo(history);
    }

    @Test
    void shouldReturnEmptyWhenNotFound() {
        Optional<WorkflowHistory> found = repository.findById("non-existent");
        assertThat(found).isEmpty();
    }

    @Test
    void shouldFindAll() {
        WorkflowHistory history1 = new WorkflowHistory("wf-1", new EventSourcedWorkflowState());
        WorkflowHistory history2 = new WorkflowHistory("wf-2", new EventSourcedWorkflowState());

        repository.save(history1);
        repository.save(history2);

        List<WorkflowHistory> all = repository.findAll();
        assertThat(all).hasSize(2);
        assertThat(all).containsExactlyInAnyOrder(history1, history2);
    }

    @Test
    void shouldClearRepository() {
        repository.save(new WorkflowHistory("wf-1", new EventSourcedWorkflowState()));
        assertThat(repository.findAll()).isNotEmpty();

        repository.clear();

        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void shouldUpdateExistingEntry() {
        String workflowId = "wf-1";
        WorkflowHistory history1 = new WorkflowHistory(workflowId, new EventSourcedWorkflowState());
        repository.save(history1);

        WorkflowHistory history2 = new WorkflowHistory(workflowId, new EventSourcedWorkflowState());
        repository.save(history2);

        Optional<WorkflowHistory> found = repository.findById(workflowId);
        assertThat(found).isPresent();
        assertThat(found.get()).isEqualTo(history2);
        assertThat(repository.findAll()).hasSize(1);
    }
}
