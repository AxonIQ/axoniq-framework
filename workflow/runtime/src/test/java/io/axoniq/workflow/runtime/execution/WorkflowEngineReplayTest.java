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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowConfiguration;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContext;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowIdProvider;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.replay.ReplayStatus;
import org.axonframework.messaging.eventhandling.replay.ReplayStatusChanged;
import org.junit.jupiter.api.*;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static io.axoniq.workflow.runtime.execution.DefaultEventNameCustomizer.Builder.defaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test for {@link WorkflowEngine} replay behavior.
 */
class WorkflowEngineReplayTest {

    private WorkflowEngine workflowEngine;
    private WorkflowExecutionRepository workflowExecutionRepository;
    private WorkflowConfigurationRegistry<?> workflowConfigurationRegistry;

    @BeforeEach
    void setUp() {
        workflowExecutionRepository = spy(new InMemoryWorkflowExecutionRepository());
        workflowConfigurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowEngine = new WorkflowEngine(
                workflowConfigurationRegistry,
                workflowExecutionRepository
        );
    }

    @Test
    void replayEventsAreDeliveredToExecution() {
        String workflowId = "workflowId";
        WorkflowExecution execution = mock(WorkflowExecution.class);
        workflowExecutionRepository.save(workflowId, () -> execution);

        EventMessage eventMessage = mock(EventMessage.class);
        Metadata metaData = Metadata.with("workflowId", workflowId);
        when(eventMessage.metadata()).thenReturn(metaData);

        ProcessingContext processingContext = mock(ProcessingContext.class);

        workflowEngine.handle(eventMessage, processingContext);

        verify(execution).onEvent(eventMessage, processingContext);
    }

    @Test
    void replayFinishedCleanupAndExecute() {

        WorkflowExecution terminalExecution = mock(WorkflowExecution.class);
        WorkflowState terminalState = mock(WorkflowState.class);
        when(terminalExecution.workflowId()).thenReturn("terminalId");
        when(terminalExecution.state()).thenReturn(terminalState);
        when(terminalState.workflowStatus()).thenReturn(WorkflowStatus.COMPLETED);
        WorkflowContext terminalContext = mock(WorkflowContext.class);
        when(terminalExecution.workflowContext()).thenReturn(terminalContext);
        ProcessingContext terminalPC = mock(ProcessingContext.class);
        when(terminalContext.processingContext()).thenReturn(terminalPC);
        when(terminalPC.whenComplete(any())).thenReturn(terminalPC);

        WorkflowExecution runningExecution = mock(WorkflowExecution.class);
        WorkflowState runningState = mock(WorkflowState.class);
        WorkflowContext runningContext = mock(WorkflowContext.class);
        when(runningExecution.workflowId()).thenReturn("runningId");
        when(runningExecution.state()).thenReturn(runningState);
        when(runningExecution.workflowContext()).thenReturn(runningContext);
        when(runningState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        ProcessingContext runningPC = mock(ProcessingContext.class);
        when(runningContext.processingContext()).thenReturn(runningPC);
        when(runningPC.whenComplete(any())).thenAnswer(invocation -> {
            Consumer<ProcessingContext> consumer = invocation.getArgument(0);
            consumer.accept(runningPC);
            return runningPC;
        });

        workflowExecutionRepository.save("terminalId", () -> terminalExecution);
        workflowExecutionRepository.save("runningId", () -> runningExecution);

        ReplayStatusChanged replayStatusChanged = mock(ReplayStatusChanged.class);
        ReplayStatus status = mock(ReplayStatus.class);
        when(replayStatusChanged.status()).thenReturn(status);
        when(status.isReplay()).thenReturn(false);

        ProcessingContext context = mock(ProcessingContext.class);

        workflowEngine.handle(replayStatusChanged, context);

        // Verify terminal execution removed
        verify(workflowExecutionRepository).remove("terminalId");

        verify(runningExecution, times(1)).execute(any());
        verify(terminalExecution, never()).execute(any()); //
    }

    @Test
    void replayStatusChangesIsExecutedFlag() {
        ProcessingContext pc = mock(ProcessingContext.class);
        WorkflowConfiguration<?> config = mock(WorkflowConfiguration.class);
        when(config.workflowName()).thenReturn("test-workflow");
        when(config.eventNameCustomizer()).thenReturn(defaults());

        when(pc.component(UnitOfWorkFactory.class)).thenReturn(mock(UnitOfWorkFactory.class));
        when(pc.component(Clock.class)).thenReturn(Clock.systemUTC());
        when(pc.component(eq(ExecutorService.class), any())).thenReturn(mock(ExecutorService.class));
        when(pc.component(EventSink.class)).thenReturn(mock(EventSink.class));

        WorkflowContext workflowContext = mock(WorkflowContext.class);
        when(workflowContext.processingContext()).thenReturn(pc);

        SimpleWorkflowExecution execution = new SimpleWorkflowExecution(
                "id",
                Map.of(),
                pc,
                config,
                workflowContext
        );

        // Initially NOT executable (replay mode)
        assertThat(execution.isExecutable()).isFalse();

        EventMessage event = mock(EventMessage.class);
        when(event.metadata()).thenReturn(Metadata.with("none", "none"));

        execution.onEvent(event, pc);
        assertThat(execution.isExecutable()).isFalse();

        // Trigger switch to executable (end of replay)
        execution.execute(i -> {
        });
        assertThat(execution.isExecutable()).isTrue();
    }

    @Test
    void duplicateWorkflowIdStartRequestIsIgnored() {
        String workflowId = "dup-id";

        // Pre-register a running workflow under "dup-id"
        WorkflowExecution existing = mock(WorkflowExecution.class);
        workflowExecutionRepository.save(workflowId, () -> existing);
        clearInvocations(workflowExecutionRepository);

        // A configuration that matches the incoming event and resolves to the same id
        WorkflowConfiguration<?> configuration = mock(WorkflowConfiguration.class);
        WorkflowIdProvider idProvider = event -> workflowId;
        when(configuration.workflowIdProvider()).thenReturn(idProvider);

        QualifiedName eventName = new QualifiedName("OrderPlaced");
        when(workflowConfigurationRegistry.getWorkflowsConfigurations(eventName))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration(e -> true, configuration)));

        EventMessage eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(eventName));

        ProcessingContext processingContext = mock(ProcessingContext.class);

        workflowEngine.handle(eventMessage, processingContext);

        // No new workflow is created for the duplicate id
        verify(workflowExecutionRepository, never()).save(eq(workflowId), any());
        verify(configuration, never()).workflowExecutionFactory();
        verify(configuration, never()).workflowContextFactory();

        // The pre-existing workflow still receives the event through the routing loop
        verify(existing).onEvent(eventMessage, processingContext);
    }
}