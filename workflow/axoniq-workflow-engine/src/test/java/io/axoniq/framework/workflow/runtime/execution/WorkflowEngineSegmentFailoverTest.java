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

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.api.execution.context.WorkflowExecution;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.VersionedType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.idOnAnotherSegmentThan;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.owningSegment;
import static io.axoniq.framework.workflow.runtime.execution.SegmentTestFixtures.token;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Verifies that workflow executions follow their segment: a node materializes only the instances of the segments it
 * claims, and drops them again when it releases them.
 */
class WorkflowEngineSegmentFailoverTest {

    private static final String OWNED_ID = "sharded-0";

    private WorkflowConfigurationRegistry<?> configurationRegistry;
    private WorkflowExecutionRepository repository;
    private WorkflowStore workflowStore;
    private WorkflowEngine workflowEngine;

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

    @BeforeEach
    void setUp() {
        configurationRegistry = mock(WorkflowConfigurationRegistry.class);
        repository = new InMemoryWorkflowExecutionRepository();
        workflowStore = mock(WorkflowStore.class);
        workflowEngine = new WorkflowEngine(
                configurationRegistry,
                repository,
                mock(WorkflowCancellationService.class),
                workflowStore,
                restoreUnitOfWorkFactory()
        );
        workflowEngine.setCheckpointingSupport(mock(WorkflowEngineCheckpointingSupport.class));
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

    @Nested
    class ClaimAfterARelease {

        private static final VersionedType DEFINITION_ID =
                VersionedType.of(new QualifiedName("ShardedWorkflow"), "1.0.0");

        private final Segment owner = owningSegment(OWNED_ID);
        private final EventSourcedRunningWorkflows runningWorkflows = new EventSourcedRunningWorkflows();
        private final List<String> bodyStarts = new ArrayList<>();

        @BeforeEach
        void startEngine() {
            when(workflowStore.loadRunningWorkflows(any()))
                    .thenReturn(CompletableFuture.completedFuture(runningWorkflows));
            workflowEngine.start(token(10));
        }

        @Test
        void restoredExecutionStartsOnceItsSegmentCatchesUpAfterAReleaseWithoutExecutions() {
            // given
            claim(token(10));
            workflowEngine.releaseWorkflowsFor(owner);
            storeRunningInstance();

            // when
            claim(token(5));
            var startsBeforeCatchUp = List.copyOf(bodyStarts);
            workflowEngine.handle(new GenericEventMessage(new MessageType("unrelated"), Map.of()), delivery(token(10)));

            // then
            assertThat(startsBeforeCatchUp).isEmpty();
            assertThat(bodyStarts).containsExactly(OWNED_ID);
        }

        @Test
        void positionRecordedAfterAReleaseLeavesTheNextClaimGatedUntilItCatchesUp() {
            // given
            storeRunningInstance();
            claim(token(5));
            workflowEngine.releaseWorkflowsFor(owner);

            // when
            workflowEngine.recordSegmentPosition(owner, token(10));
            claim(token(5));
            var startsBeforeCatchUp = List.copyOf(bodyStarts);
            workflowEngine.recordSegmentPosition(owner, token(10));

            // then
            assertThat(startsBeforeCatchUp).isEmpty();
            assertThat(bodyStarts).containsExactly(OWNED_ID);
        }

        private void claim(TrackingToken from) {
            workflowEngine.restoreWorkflowsFor(owner, from, new StubProcessingContext(), new StubProcessingContext())
                          .join();
        }

        private ProcessingContext delivery(TrackingToken position) {
            var context = new StubProcessingContext();
            context.putResource(Segment.RESOURCE_KEY, owner);
            context.putResource(TrackingToken.RESOURCE_KEY, position);
            return context;
        }

        private void storeRunningInstance() {
            var state = new EventSourcedWorkflowState(OWNED_ID, Map.of(), DEFINITION_ID);
            runningWorkflows.evolve(MetadataUtils.create(OWNED_ID, WorkflowStatus.STARTED));
            when(workflowStore.loadWorkflow(eq(OWNED_ID), any())).thenReturn(CompletableFuture.completedFuture(state));
            var execution = WorkflowExecutionFixture.mockExecution(OWNED_ID, state, false);
            WorkflowExecutionFixture.recordBodyStartOn(execution, bodyStarts::add, OWNED_ID);
            var configuration = WorkflowExecutionFixture.mockConfiguration(OWNED_ID, execution);
            when(configurationRegistry.getWorkflowConfiguration(DEFINITION_ID)).thenReturn(Optional.of(configuration));
        }
    }
}
