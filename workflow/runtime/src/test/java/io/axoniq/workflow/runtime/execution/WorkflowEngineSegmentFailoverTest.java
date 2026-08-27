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

import io.axoniq.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.workflow.runtime.api.execution.status.WorkflowStatus;
import io.axoniq.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.idOnAnotherSegmentThan;
import static io.axoniq.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that workflow executions follow their segment: a node materializes only the instances of the segments it
 * claims, and drops them again when it releases them.
 */
class WorkflowEngineSegmentFailoverTest {

    private static final String OWNED_ID = "sharded-0";

    private WorkflowExecutionRepository repository;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;

    @BeforeEach
    void setUp() {
        repository = new InMemoryWorkflowExecutionRepository();
        workflowStore = mock(WorkflowStore.class);
        workflowEngine = new WorkflowEngine(
                mock(WorkflowConfigurationRegistry.class),
                repository,
                mock(WorkflowCancellationService.class),
                workflowStore,
                mock(UnitOfWorkFactory.class)
        );
        workflowEngine.setEngineSupportComponents(mock(WorkflowEngineReplaySupport.class),
                                                  mock(WorkflowEngineCheckpointingSupport.class));
    }

    @Test
    void claimingASegmentSourcesOnlyTheInstancesThatSegmentOwns() {
        var foreignId = idOnAnotherSegmentThan(OWNED_ID);
        var running = new EventSourcedRunningWorkflows();
        running.evolve(MetadataUtils.create(OWNED_ID, WorkflowStatus.STARTED));
        running.evolve(MetadataUtils.create(foreignId, WorkflowStatus.STARTED));
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(CompletableFuture.completedFuture(running));
        when(workflowStore.loadWorkflow(eq(OWNED_ID), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("sourced")));

        var owner = owningSegment(OWNED_ID);
        // The claim survives the failed sourcing of a single instance, which WorkflowEngineRestoreFaultIsolationTest
        // pins. What this case asserts is which instances were sourced at all.
        workflowEngine.restoreWorkflowsFor(owner, null, new StubProcessingContext(), new StubProcessingContext())
                      .join();

        verify(workflowStore).loadWorkflow(eq(OWNED_ID), any());
        verify(workflowStore, never()).loadWorkflow(eq(foreignId), any());
    }

    @Test
    void releasingASegmentStopsAndDropsOnlyTheInstancesItOwns() {
        var owner = owningSegment(OWNED_ID);
        var foreignId = idOnAnotherSegmentThan(OWNED_ID);
        var owned = execution(OWNED_ID);
        var foreign = execution(foreignId);
        repository.save(OWNED_ID, () -> owned);
        repository.save(foreignId, () -> foreign);

        workflowEngine.releaseWorkflowsFor(owner);

        verify(owned).stopForShutdown();
        verify(foreign, never()).stopForShutdown();
        assertThat(repository.findAll()).containsExactly(foreign);
    }

    private static WorkflowExecution execution(String workflowId) {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        return execution;
    }
}
