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
package io.axoniq.framework.workflow.history;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.query.api.WorkflowStateQuery;
import io.axoniq.framework.workflow.runtime.execution.EventSourcedWorkflowState;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryWorkflowHistoryRepositoryTest {

    private static final MessageType DEFINITION_ID = new MessageType(new QualifiedName("HistoryWorkflow"), "0.0.1");

    private InMemoryWorkflowHistoryRepository repository;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowHistoryRepository();
    }

    @Test
    void shouldSaveAndFindById() {
        String workflowId = "wf-1";
        WorkflowHistory history = new WorkflowHistory(workflowId,
                                                      new EventSourcedWorkflowState(workflowId, DEFINITION_ID));

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
        WorkflowHistory history1 = new WorkflowHistory("wf-1", new EventSourcedWorkflowState("wf-1", DEFINITION_ID));
        WorkflowHistory history2 = new WorkflowHistory("wf-2", new EventSourcedWorkflowState("wf-2", DEFINITION_ID));

        repository.save(history1);
        repository.save(history2);

        List<WorkflowHistory> all = repository.findAll();
        assertThat(all).hasSize(2);
        assertThat(all).containsExactlyInAnyOrder(history1, history2);
    }

    @Test
    void shouldFindOnlyEntriesMatchingAStateQuery() {
        WorkflowHistory payment = new WorkflowHistory("wf-1",
                                                       new EventSourcedWorkflowState("wf-1", DEFINITION_ID));
        WorkflowHistory shipping = new WorkflowHistory(
                "wf-2", new EventSourcedWorkflowState("wf-2", new MessageType("ShippingWorkflow", "0.0.1"))
        );
        repository.save(payment);
        repository.save(shipping);

        assertThat(repository.findAll(WorkflowStateQuery.all().workflowDefinitionId(DEFINITION_ID)))
                .containsExactly(payment);
    }

    @Test
    void shouldClearRepository() {
        repository.save(new WorkflowHistory("wf-1", new EventSourcedWorkflowState("wf-1", DEFINITION_ID)));
        assertThat(repository.findAll()).isNotEmpty();

        repository.clear();

        assertThat(repository.findAll()).isEmpty();
    }

    @Test
    void shouldUpdateExistingEntry() {
        String workflowId = "wf-1";
        WorkflowHistory history1 = new WorkflowHistory(workflowId,
                                                       new EventSourcedWorkflowState(workflowId, DEFINITION_ID));
        repository.save(history1);

        WorkflowHistory history2 = new WorkflowHistory(workflowId,
                                                       new EventSourcedWorkflowState(workflowId, DEFINITION_ID));
        repository.save(history2);

        Optional<WorkflowHistory> found = repository.findById(workflowId);
        assertThat(found).isPresent();
        assertThat(found.get()).isEqualTo(history2);
        assertThat(repository.findAll()).hasSize(1);
    }
}
