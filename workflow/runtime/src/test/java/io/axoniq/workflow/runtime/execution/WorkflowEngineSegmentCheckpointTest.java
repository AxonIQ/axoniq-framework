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
import io.axoniq.workflow.runtime.api.execution.context.WorkflowContextFactory;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecutionFactory;
import io.axoniq.workflow.runtime.api.execution.state.WorkflowState;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.execution.WorkflowConfigurationRegistry.PredicatedWorkflowConfiguration;
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.FOUR_SEGMENTS;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that checkpoint holdback is scoped to the segment being advanced: a busy workflow only holds
 * back the token of the segment that owns it.
 * <p>
 * The busy instance is started through the engine's own handling path rather than pushed into the repository, because
 * that path is where the engine indexes an instance as a source of checkpoint work. Asking the index instead of
 * scanning the repository is what keeps the holdback decision cheap; scoping that question to the segment being
 * advanced is what keeps one busy instance from stalling every other segment's token.
 */
class WorkflowEngineSegmentCheckpointTest {

    private static final TrackingToken TOKEN = new GlobalSequenceTrackingToken(42);
    private static final String BUSY_WORKFLOW_ID = "busy-workflow";
    private static final QualifiedName START_EVENT = new QualifiedName("StartBusyWorkflow");

    private WorkflowEngine workflowEngine;
    private WorkflowEngineReplaySupport replaySupport;
    private WorkflowEngineCheckpointingSupport checkpointingSupport;

    @BeforeEach
    void setUp() {
        var configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                new InMemoryWorkflowExecutionRepository(),
                mock(WorkflowStore.class),
                mock(UnitOfWorkFactory.class)
        );
        replaySupport = new WorkflowEngineReplaySupport(workflowEngine);
        checkpointingSupport = new WorkflowEngineCheckpointingSupport(workflowEngine);
        workflowEngine.setEngineSupportComponents(replaySupport, checkpointingSupport);
        registerStartConfiguration(configurationRegistry);
        // Delivered on the segment that owns it, exactly as the routing sequences a unique start candidate.
        workflowEngine.handle(startEvent(), processingContext(owningSegment(BUSY_WORKFLOW_ID)));
    }

    @Test
    void busyWorkflowHoldsBackOnlyTheTokenOfTheSegmentOwningIt() {
        var owner = owningSegment(BUSY_WORKFLOW_ID);

        for (var segment : FOUR_SEGMENTS) {
            var advanced = checkpointingSupport.onCheckpointAdvanced(segment, TOKEN);
            if (segment.equals(owner)) {
                assertThat(advanced)
                        .as("segment %s owns '%s' and must wait for it to drain", segment, BUSY_WORKFLOW_ID)
                        .isNotCompleted();
            } else {
                assertThat(advanced)
                        .as("segment %s owns no busy workflow and must advance", segment)
                        .isCompletedWithValue(TOKEN);
            }
        }
    }

    /**
     * Registers a definition starting {@link #BUSY_WORKFLOW_ID}. The started execution reports itself as a source of
     * checkpoint work the moment the engine installs its listener, and keeps reporting unsafe when asked again.
     */
    @SuppressWarnings("unchecked")
    private static void registerStartConfiguration(WorkflowConfigurationRegistry<?> configurationRegistry) {
        var busyExecution = mock(WorkflowExecution.class);
        when(busyExecution.workflowId()).thenReturn(BUSY_WORKFLOW_ID);
        when(busyExecution.hasUnsafeCheckpointWork()).thenReturn(true);
        // Live-mode activation sweeps the repository for terminal executions, so the mock needs a status.
        var busyState = mock(WorkflowState.class);
        when(busyState.workflowStatus()).thenReturn(WorkflowStatus.STARTED);
        when(busyExecution.state()).thenReturn(busyState);
        when(busyExecution.isRunning()).thenReturn(true);
        doAnswer(invocation -> {
            invocation.<WorkflowExecution.CheckpointWorkStateListener>getArgument(0).onMarkedUnsafe();
            return null;
        }).when(busyExecution).registerCheckpointWorkStateListener(any());

        WorkflowConfiguration<WorkflowContext> configuration = mock(WorkflowConfiguration.class);
        WorkflowContextFactory<WorkflowContext> contextFactory = mock(WorkflowContextFactory.class);
        WorkflowExecutionFactory executionFactory = mock(WorkflowExecutionFactory.class);
        var workflowContext = mock(WorkflowContext.class);
        when(configuration.workflowIdProvider()).thenReturn(event -> BUSY_WORKFLOW_ID);
        when(configuration.workflowVersion()).thenReturn("1.0.0");
        when(configuration.workflowContextFactory()).thenReturn(contextFactory);
        when(configuration.workflowExecutionFactory()).thenReturn(executionFactory);
        when(contextFactory.createContext(anyMap(), eq(BUSY_WORKFLOW_ID), any(), eq(configuration)))
                .thenReturn(workflowContext);
        when(executionFactory.create(workflowContext)).thenReturn(busyExecution);
        when(configurationRegistry.getHighestVersionConfigurations(new MessageType(START_EVENT)))
                .thenReturn(List.of(new PredicatedWorkflowConfiguration((e, pc) -> true, configuration)));
    }

    private static EventMessage startEvent() {
        var eventMessage = mock(EventMessage.class);
        when(eventMessage.metadata()).thenReturn(Metadata.emptyInstance());
        when(eventMessage.type()).thenReturn(new MessageType(START_EVENT));
        when(eventMessage.payloadAs(any(TypeReference.class))).thenReturn(Map.of("id", BUSY_WORKFLOW_ID));
        return eventMessage;
    }

    private ProcessingContext processingContext(Segment segment) {
        var context = new StubProcessingContext();
        context.putResource(Segment.RESOURCE_KEY, segment);
        return context;
    }
}
