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

import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Neither segment callback may hold a segment forever when the work it waits on never completes.
 * <p>
 * A claim can stall on a remote workflow-store load, so it must time out rather than hold a segment forever. A release
 * only requests the released executions to stop and reports that request immediately.
 */
class WorkflowEngineSegmentCallbackTest {

    private static final Segment SEGMENT = FOUR_SEGMENTS.getFirst();
    private static final String OWNED_ID = anyIdOn(SEGMENT);
    private static final String FOREIGN_ID = idOnAnotherSegmentThan(OWNED_ID);

    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(200);

    private static WorkflowEngine engine(WorkflowExecutionRepository repository, WorkflowStore workflowStore) {
        return engine(repository, workflowStore, mock(WorkflowCancellationService.class));
    }

    private static WorkflowEngine engine(WorkflowExecutionRepository repository,
                                         WorkflowStore workflowStore,
                                         WorkflowCancellationService cancellationService) {
        var engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                        repository,
                                        cancellationService,
                                        workflowStore,
                                        restoreUnitOfWorkFactory());
        engine.setCheckpointingSupport(new WorkflowEngineCheckpointingSupport(engine));
        engine.restoreTimeout = SHORT_TIMEOUT;
        return engine;
    }

    private static WorkflowExecution execution(String workflowId) {
        var execution = mock(WorkflowExecution.class);
        when(execution.workflowId()).thenReturn(workflowId);
        return execution;
    }

    /**
     * Returns a real unit of work factory: the engine sources every restored instance in a unit of work of its own, so
     * a mock would hand it none.
     */
    private static UnitOfWorkFactory restoreUnitOfWorkFactory() {
        return new SimpleUnitOfWorkFactory(new ApplicationContext() {
            @Override
            public <C> C component(Class<C> type, String name) {
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    @Test
    void aClaimWhoseStateLoadNeverCompletesFailsInsteadOfHoldingTheSegment() {
        var workflowStore = mock(WorkflowStore.class);
        // A load that never completes, which is what a hanging store looks like from here.
        when(workflowStore.loadRunningWorkflows(any())).thenReturn(new CompletableFuture<>());
        var engine = engine(new InMemoryWorkflowExecutionRepository(), workflowStore);

        var claim = engine.restoreWorkflowsFor(SEGMENT,
                                               null,
                                               new StubProcessingContext(),
                                               new StubProcessingContext());

        assertThatThrownBy(claim::join)
                .as("""
                            The claim must complete exceptionally once the timeout elapses. Completing it normally reports a \
                            segment as claimed while none of its instances is running; never completing it holds the segment \
                            on this node with no diagnostics.""")
                .hasCauseInstanceOf(TimeoutException.class);
    }

    @Test
    void aReleaseStopsAndDropsOnlyTheExecutionsOfItsOwnSegment() {
        var owned = execution(OWNED_ID);
        var foreign = execution(FOREIGN_ID);
        var repository = new InMemoryWorkflowExecutionRepository();
        repository.save(OWNED_ID, () -> owned);
        repository.save(FOREIGN_ID, () -> foreign);
        var cancellationService = mock(WorkflowCancellationService.class);

        var release = engine(repository, mock(WorkflowStore.class), cancellationService).releaseWorkflowsFor(SEGMENT);

        assertThat(release).isCompleted();
        verify(owned).stopForShutdown();
        verify(foreign, never()).stopForShutdown();
        verifyNoInteractions(cancellationService);
        assertThat(repository.findAll()).containsExactly(foreign);
    }

    @Test
    void aReleaseOfASegmentWithoutInstancesCompletesRightAway() {
        var engine = engine(new InMemoryWorkflowExecutionRepository(), mock(WorkflowStore.class));

        assertThat(engine.releaseWorkflowsFor(SEGMENT))
                .as("nothing to drain, so nothing to wait for")
                .isCompleted();
    }

    @Test
    void theProductionTimeoutIsUsedWhenNoneIsSetForATest() {
        var engine = new WorkflowEngine(new SimpleWorkflowConfigurationRegistry(),
                                        new InMemoryWorkflowExecutionRepository(),
                                        mock(WorkflowCancellationService.class),
                                        mock(WorkflowStore.class),
                                        restoreUnitOfWorkFactory());

        assertThat(engine.restoreTimeout).isEqualTo(WorkflowEngine.DEFAULT_RESTORE_TIMEOUT);
    }
}
