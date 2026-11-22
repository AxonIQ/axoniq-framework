/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.index.inmemory.InMemoryWorkflowIndex;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
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
                                        new InMemoryWorkflowIndex(),
                                        "WorkflowEngine",
                                        restoreUnitOfWorkFactory());
        engine.setCheckpointingSupport(new WorkflowEngineCheckpointingSupport(engine, 0L));
        engine.restoreTimeout = SHORT_TIMEOUT;
        return engine;
    }

    private static WorkflowExecution execution(String workflowId) {
        var definitionId = VersionedType.of(new QualifiedName("Sharded"), "1.0.0");
        return WorkflowExecutionFixture.mockExecution(
                workflowId, new EventSourcedWorkflowState(workflowId, definitionId), true
        );
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
        var cancellationService = new WorkflowCancellationService();
        var engine = engine(repository, mock(WorkflowStore.class), cancellationService);
        engine.registerCancellation(owned);
        engine.registerCancellation(foreign);

        var release = engine.releaseWorkflowsFor(SEGMENT);

        assertThat(release).isCompleted();
        verify(owned).stopForShutdown();
        verify(foreign, never()).stopForShutdown();
        assertThat(cancellationService.registered(OWNED_ID)).isNull();
        assertThat(cancellationService.registered(FOREIGN_ID)).isNotNull();
        assertThat(repository.findAll()).containsExactly(foreign);
    }

    @Test
    void aReleaseLeavesTheCancellationThatReplacedTheOneOfAReleasedExecutionRegistered() {
        // given: a released execution, whose cancellation was replaced by another registration of its workflow
        var owned = execution(OWNED_ID);
        var repository = new InMemoryWorkflowExecutionRepository();
        repository.save(OWNED_ID, () -> owned);
        var cancellationService = new WorkflowCancellationService();
        var engine = engine(repository, mock(WorkflowStore.class), cancellationService);
        engine.registerCancellation(owned);
        var replacement = mock(WorkflowCancellation.class);
        cancellationService.register(OWNED_ID, replacement);

        // when
        engine.releaseWorkflowsFor(SEGMENT);

        // then
        assertThat(cancellationService.registered(OWNED_ID)).isSameAs(replacement);
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
                                        new InMemoryWorkflowIndex(),
                                        "WorkflowEngine",
                                        restoreUnitOfWorkFactory());

        assertThat(engine.restoreTimeout).isEqualTo(WorkflowEngine.DEFAULT_RESTORE_TIMEOUT);
    }
}
