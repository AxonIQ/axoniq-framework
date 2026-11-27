/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.dsl.api.WorkflowStatus;
import io.axoniq.framework.workflow.runtime.execution.WorkflowIndexChanges.SegmentPosition;
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
import org.axonframework.messaging.eventhandling.processing.streaming.token.MergedTrackingToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

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
 * Tests which position of a segment the {@link WorkflowEngineIndexSupport} stores with a token, for the workflow
 * manager to read.
 *
 * @author Stefan Dragisic
 */
class WorkflowEngineIndexSupportPositionTest {

    private static final String MODULE = "orders";

    private final UnitOfWorkFactory unitOfWorkFactory = new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE);
    private final RecordingWorkflowIndex index = new RecordingWorkflowIndex();
    private final WorkflowEngineIndexSupport testSubject =
            new WorkflowEngineIndexSupport(index, MODULE, unitOfWorkFactory, workflowId -> true);

    @Test
    void isThePositionOfTheTokenStoredWithTheChanges() {
        // given
        var token = new GlobalSequenceTrackingToken(5);
        recordStart("order-1", token);

        // when
        store(token);

        // then
        assertThat(index.positions).containsExactly(position(5L));
    }

    @Test
    void isTheLowerPositionOfAMergedToken() {
        // given
        recordStart("order-1", new GlobalSequenceTrackingToken(3));

        // when
        store(new MergedTrackingToken(new GlobalSequenceTrackingToken(3), new GlobalSequenceTrackingToken(7)));

        // then
        assertThat(index.positions).containsExactly(position(3L));
    }

    @Test
    void movesWithTheNextChangeButNotWithATokenThatCarriesNone() {
        // given
        recordStart("order-1", new GlobalSequenceTrackingToken(5));
        store(new GlobalSequenceTrackingToken(5));

        // when: a token without changes, then one with a change
        store(new GlobalSequenceTrackingToken(6));
        recordStart("order-2", new GlobalSequenceTrackingToken(7));
        store(new GlobalSequenceTrackingToken(7));

        // then
        assertThat(index.positions).containsExactly(position(5L), position(7L));
    }

    private void recordStart(String workflowId, TrackingToken token) {
        var started = new GenericEventMessage(new MessageType(new QualifiedName("WorkflowEvent")),
                                              Map.of(),
                                              MetadataUtils.create(workflowId, WorkflowStatus.STARTED));
        testSubject.record(Segment.ROOT_SEGMENT, token, started);
    }

    private void store(TrackingToken token) {
        unitOfWorkFactory.create()
                         .executeWithResult(context -> testSubject.store(Segment.ROOT_SEGMENT, token, context))
                         .orTimeout(5, TimeUnit.SECONDS)
                         .join();
    }

    private static SegmentPosition position(long position) {
        return new SegmentPosition(SegmentKeyRange.of(Segment.ROOT_SEGMENT), position);
    }

    /**
     * An index that records the position of every store, in order.
     */
    private static final class RecordingWorkflowIndex implements WorkflowIndex {

        private final List<SegmentPosition> positions = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> store(String module,
                                             WorkflowIndexChanges changes,
                                             @Nullable ProcessingContext context) {
            if (changes.position() != null) {
                positions.add(changes.position());
            }
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
