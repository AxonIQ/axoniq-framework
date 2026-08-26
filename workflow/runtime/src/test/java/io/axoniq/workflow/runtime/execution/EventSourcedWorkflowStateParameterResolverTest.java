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
package io.axoniq.workflow.runtime.execution;

import io.axoniq.workflow.history.api.WorkflowHistory;
import io.axoniq.workflow.history.api.WorkflowHistoryRepository;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.Configuration;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for {@link WorkflowStateParameterResolver}.
 */
class EventSourcedWorkflowStateParameterResolverTest {

    private WorkflowStateParameterResolver resolver;
    private ProcessingContext context;
    private Configuration configuration;
    private WorkflowExecutionRepository executionRepository;
    private WorkflowHistoryRepository historyRepository;
    private final MessageType messageType = new MessageType(new QualifiedName("ns", "name"), "1.0");
    private MockedStatic<Message> messageMockedStatic;

    @BeforeEach
    void setUp() {
        context = mock(ProcessingContext.class);
        configuration = mock(Configuration.class);
        resolver = new WorkflowStateParameterResolver(configuration);
        executionRepository = mock(WorkflowExecutionRepository.class);
        historyRepository = mock(WorkflowHistoryRepository.class);

        when(configuration.getComponent(WorkflowExecutionRepository.class)).thenReturn(executionRepository);
        when(configuration.getComponent(WorkflowHistoryRepository.class)).thenReturn(historyRepository);

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
    void testMatchesNoWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", Metadata.with("foo", "bar"));
        setMessageInContext(message);

        assertFalse(resolver.matches(context));
    }

    @Test
    void testMatchesWithWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create("workflow-1"));
        setMessageInContext(message);

        assertTrue(resolver.matches(context));
    }

    @Test
    void testResolveNoWorkflowId() {
        GenericMessage message = new GenericMessage(messageType, "payload", Metadata.with("foo", "bar"));
        setMessageInContext(message);

        CompletableFuture<WorkflowState> result = resolver.resolveParameterValue(context);

        assertTrue(result.isCompletedExceptionally());
        ExecutionException exception = assertThrows(ExecutionException.class, result::get);
        assertTrue(exception.getCause() instanceof IllegalStateException);
        assertEquals("Unable to inject workflow state, since no workflow id was found in the message.",
                     exception.getCause().getMessage());
    }

    @Test
    void testResolveInExecutionRepository() throws Exception {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        WorkflowExecution execution = mock(WorkflowExecution.class);
        WorkflowState state = mock(WorkflowState.class);
        when(execution.state()).thenReturn(state);
        when(executionRepository.findById(workflowId)).thenReturn(Optional.of(execution));

        CompletableFuture<WorkflowState> result = resolver.resolveParameterValue(context);

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertEquals(state, result.get());
        verify(configuration).getComponent(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verifyNoInteractions(historyRepository);
    }

    @Test
    void testResolveInHistoryRepository() throws Exception {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        WorkflowHistory history = mock(WorkflowHistory.class);
        WorkflowState state = mock(WorkflowState.class);
        when(history.state()).thenReturn(state);

        when(executionRepository.findById(workflowId)).thenReturn(Optional.empty());
        when(historyRepository.findById(workflowId)).thenReturn(Optional.of(history));

        CompletableFuture<WorkflowState> result = resolver.resolveParameterValue(context);

        assertTrue(result.isDone());
        assertFalse(result.isCompletedExceptionally());
        assertEquals(state, result.get());
        verify(configuration).getComponent(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verify(configuration).getComponent(WorkflowHistoryRepository.class);
        verify(historyRepository).findById(workflowId);
    }

    @Test
    void testResolveNotFound() {
        String workflowId = "workflow-1";
        GenericMessage message = new GenericMessage(messageType, "payload", MetadataUtils.create(workflowId));
        setMessageInContext(message);

        when(executionRepository.findById(workflowId)).thenReturn(Optional.empty());
        when(historyRepository.findById(workflowId)).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, () -> resolver.resolveParameterValue(context));

        verify(configuration).getComponent(WorkflowExecutionRepository.class);
        verify(executionRepository).findById(workflowId);
        verify(configuration).getComponent(WorkflowHistoryRepository.class);
        verify(historyRepository).findById(workflowId);
    }
}
