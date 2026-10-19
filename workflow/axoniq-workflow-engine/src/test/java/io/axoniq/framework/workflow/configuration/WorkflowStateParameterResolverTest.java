/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.dsl.api.WorkflowState;
import io.axoniq.framework.workflow.history.api.WorkflowHistory;
import io.axoniq.framework.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.execution.WorkflowExecutionRepository;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.junit.jupiter.api.*;
import org.mockito.*;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Tests validating the {@link WorkflowStateParameterResolver}.
 *
 * @author Simon Zambrovski
 */
class WorkflowStateParameterResolverTest {

    private final MessageType messageType = new MessageType(new QualifiedName("ns", "name"), "1.0");
    private ProcessingContext context;
    private WorkflowExecutionRepository executionRepository;
    private WorkflowHistoryRepository historyRepository;
    private MockedStatic<Message> messageMockedStatic;

    private WorkflowStateParameterResolver testSubject;

    @BeforeEach
    void setUp() {
        context = mock(ProcessingContext.class);
        testSubject = new WorkflowStateParameterResolver();
        executionRepository = mock(WorkflowExecutionRepository.class);
        historyRepository = mock(WorkflowHistoryRepository.class);

        when(context.component(WorkflowExecutionRepository.class)).thenReturn(executionRepository);
        when(context.component(WorkflowHistoryRepository.class)).thenReturn(historyRepository);

        messageMockedStatic = mockStatic(Message.class);
    }

    @AfterEach
    void tearDown() {
        messageMockedStatic.close();
    }

    private void setMessageInContext(Message message) {
        messageMockedStatic.when(() -> Message.fromContext(context)).thenReturn(message);
    }

    @Test
    void matchesNoWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", Metadata.with("foo", "bar"));
        setMessageInContext(message);

        assertThat(testSubject.matches(context)).isFalse();
    }

    @Test
    void matchesWithWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create("workflow-1"));
        setMessageInContext(message);

        assertThat(testSubject.matches(context)).isTrue();
    }

    @Test
    void resolveNoWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", Metadata.with("foo", "bar"));
        setMessageInContext(message);

        CompletableFuture<WorkflowState> result = testSubject.resolveParameterValue(context);

        assertThat(result).isCompletedExceptionally();
        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unable to inject workflow state, since no workflow id was found in the message.");
    }

    @Test
    void resolveInExecutionRepository() throws Exception {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        WorkflowExecution execution = mock(WorkflowExecution.class);
        WorkflowState state = mock(WorkflowState.class);
        when(execution.state()).thenReturn(state);
        when(executionRepository.findById(workflowId)).thenReturn(Optional.of(execution));

        CompletableFuture<WorkflowState> result = testSubject.resolveParameterValue(context);

        assertThat(result).isCompleted().isNotCompletedExceptionally();
        assertThat(result.get()).isSameAs(state);
        verify(context).component(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verifyNoInteractions(historyRepository);
    }

    @Test
    void resolveInHistoryRepository() throws Exception {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        WorkflowHistory history = mock(WorkflowHistory.class);
        WorkflowState state = mock(WorkflowState.class);
        when(history.state()).thenReturn(state);

        when(executionRepository.findById(workflowId)).thenReturn(Optional.empty());
        when(historyRepository.findById(workflowId)).thenReturn(CompletableFuture.completedFuture(Optional.of(history)));

        CompletableFuture<WorkflowState> result = testSubject.resolveParameterValue(context);

        assertThat(result).isCompleted().isNotCompletedExceptionally();
        assertThat(result.get()).isSameAs(state);
        verify(context).component(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verify(context).component(WorkflowHistoryRepository.class);
        verify(historyRepository).findById(workflowId);
    }

    @Test
    void resolveNotFound() {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        when(executionRepository.findById(workflowId)).thenReturn(Optional.empty());
        when(historyRepository.findById(workflowId)).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

        assertThat(testSubject.resolveParameterValue(context)).isCompletedExceptionally();

        verify(context).component(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verify(context).component(WorkflowHistoryRepository.class);
        verify(historyRepository).findById(workflowId);
    }
}
