/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.history;

import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.inmemory.InMemoryWorkflowHistoryRepository;
import io.axoniq.framework.workflow.history.inmemory.WorkflowHistoryProjector;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class WorkflowHistoryProjectorTest {

    private static final VersionedType DEFINITION_ID =
            VersionedType.of(new QualifiedName("test-workflow"), "1.0.0");

    private InMemoryWorkflowHistoryRepository repository;
    private WorkflowHistoryProjector projector;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowHistoryRepository();
        projector = new WorkflowHistoryProjector(repository);
    }

    @Test
    void shouldCreateNewHistoryEntryOnEvent() {
        String workflowId = "wf-1";
        EventMessage event = new GenericEventMessage(
                new MessageType("test"),
                new Object(),
                MetadataUtils.withWorkflowDefinitionId(MetadataUtils.create(workflowId), DEFINITION_ID)
        );
        ProcessingContext context = mock(ProcessingContext.class);

        projector.handle(event, context);

        assertThat(repository.findById(workflowId).join()).isPresent();
        assertThat(repository.findById(workflowId).join().get().workflowId()).isEqualTo(workflowId);
    }

    @Test
    void shouldUpdateExistingHistoryEntryOnEvent() {
        String workflowId = "wf-1";
        ProcessingContext context = mock(ProcessingContext.class);

        // First event
        EventMessage event1 = new GenericEventMessage(
                new MessageType("test1"),
                new Object(),
                MetadataUtils.withWorkflowDefinitionId(MetadataUtils.create(workflowId), DEFINITION_ID)
        );
        projector.handle(event1, context);

        WorkflowHistory firstHistory = repository.findById(workflowId).join().get();

        // Second event
        EventMessage event2 = new GenericEventMessage(
                new MessageType("test2"),
                new Object(),
                MetadataUtils.withWorkflowDefinitionId(MetadataUtils.create(workflowId), DEFINITION_ID)
        );
        projector.handle(event2, context);

        WorkflowHistory secondHistory = repository.findById(workflowId).join().get();

        assertThat(secondHistory).isNotSameAs(firstHistory);
        assertThat(secondHistory.workflowId()).isEqualTo(workflowId);
    }

    @Test
    void shouldIgnoreEventWithoutWorkflowId() {
        EventMessage event = new GenericEventMessage(new MessageType("test"),
                                                     new Object(),
                                                     Metadata.with("other", "value"));
        ProcessingContext context = mock(ProcessingContext.class);

        projector.handle(event, context);

        assertThat(repository.findAll().join()).isEmpty();
    }
}
