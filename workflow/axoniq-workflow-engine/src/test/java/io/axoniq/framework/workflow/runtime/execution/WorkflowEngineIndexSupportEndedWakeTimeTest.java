/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WakesAtSet;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChange.WorkflowEnded;
import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.axonframework.messaging.eventhandling.processing.streaming.token.GlobalSequenceTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the {@link WorkflowEngineIndexSupport} never stores a wake time for a workflow whose end it already
 * stored. The JPA and JDBC indexes add a missing row for a wake time, so such a wake time brings the row back.
 *
 * @author Stefan Dragisic
 */
class WorkflowEngineIndexSupportEndedWakeTimeTest {

    private static final String MODULE = "orders";

    private final UnitOfWorkFactory unitOfWorkFactory = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
    private final RecordingWorkflowIndex index = new RecordingWorkflowIndex();
    // This node still holds a copy of every workflow, which may hibernate.
    private final WorkflowEngineIndexSupport testSubject =
            new WorkflowEngineIndexSupport(index, MODULE, unitOfWorkFactory, workflowId -> true);

    @Test
    void storesNoWakeTimeAfterTheEndOfTheWorkflow() {
        // given: another node ended the workflow, and this segment stored that end
        record("order-1", WorkflowStatus.STARTED, new GlobalSequenceTrackingToken(1));
        store(new GlobalSequenceTrackingToken(1));
        record("order-1", WorkflowStatus.FAILED, new GlobalSequenceTrackingToken(2));
        store(new GlobalSequenceTrackingToken(2));

        // when: the copy this node still holds hibernates
        testSubject.wakesAt(Segment.ROOT_SEGMENT, "order-1", Instant.parse("2026-12-03T14:15:35Z"));
        store(new GlobalSequenceTrackingToken(3));

        // then
        assertThat(index.stored).contains(new WorkflowEnded("order-1"));
        assertThat(index.stored).noneMatch(change -> change instanceof WakesAtSet set
                && set.workflowId().equals("order-1"));
    }

    @Test
    void storesNoWakeTimeWithTheEndOfTheWorkflowInOneBatch() {
        // given
        record("order-1", WorkflowStatus.STARTED, new GlobalSequenceTrackingToken(1));
        store(new GlobalSequenceTrackingToken(1));
        record("order-1", WorkflowStatus.FAILED, new GlobalSequenceTrackingToken(2));

        // when
        testSubject.wakesAt(Segment.ROOT_SEGMENT, "order-1", Instant.parse("2026-12-03T14:15:35Z"));
        store(new GlobalSequenceTrackingToken(2));

        // then
        assertThat(index.stored).noneMatch(change -> change instanceof WakesAtSet set
                && set.workflowId().equals("order-1"));
    }

    @Test
    void storesTheWakeTimeOfAWorkflowWhoseRowASourcedClaimReplaced() {
        // given: a sourced claim replaces the row of a running workflow by an end and a start
        testSubject.sourced(Segment.ROOT_SEGMENT, Set.of("order-1"), List.of())
                   .orTimeout(5, TimeUnit.SECONDS)
                   .join();

        // when
        testSubject.wakesAt(Segment.ROOT_SEGMENT, "order-1", Instant.parse("2026-12-03T14:15:35Z"));
        store(new GlobalSequenceTrackingToken(1));

        // then
        assertThat(index.stored).contains(new WakesAtSet("order-1", Instant.parse("2026-12-03T14:15:35Z")));
    }

    private void record(String workflowId, WorkflowStatus status, TrackingToken token) {
        var event = new GenericEventMessage(new MessageType(new QualifiedName("WorkflowEvent")),
                                            Map.of(),
                                            MetadataUtils.create(workflowId, status));
        testSubject.record(Segment.ROOT_SEGMENT, token, event);
    }

    private void store(TrackingToken token) {
        unitOfWorkFactory.create()
                         .executeWithResult(context -> testSubject.store(Segment.ROOT_SEGMENT, token, context))
                         .orTimeout(5, TimeUnit.SECONDS)
                         .join();
    }

    /**
     * An index that records every stored change, in order.
     */
    private static final class RecordingWorkflowIndex implements WorkflowIndex {

        private final List<WorkflowIndexChange> stored = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> store(String module,
                                             WorkflowIndexChanges changes,
                                             @Nullable ProcessingContext context) {
            stored.addAll(changes.changes());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public boolean storesWithToken() {
            return true;
        }

        @Override
        public CompletableFuture<Optional<WorkflowIndexRows>> load(String module,
                                                                   Segment segment,
                                                                   @Nullable ProcessingContext context) {
            throw new UnsupportedOperationException("not used when storing");
        }

        @Override
        public CompletableFuture<Optional<WorkflowIndexRows>> loadWorkflows(String module,
                                                                            Collection<String> workflowIds,
                                                                            @Nullable ProcessingContext context) {
            throw new UnsupportedOperationException("not used when storing");
        }

        @Override
        public CompletableFuture<Void> initialize(String module,
                                                  Set<String> runningWorkflowIds,
                                                  Collection<WaitAssociation> waits) {
            throw new UnsupportedOperationException("not used when storing");
        }
    }
}
