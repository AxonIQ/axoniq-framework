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
package io.axoniq.framework.integrationtests.workflow;

import io.axoniq.framework.workflow.runtime.execution.WorkflowEventTagResolver;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.integrationtests.workflow.DcbFencingBackends.Backend.POSTGRES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * FND-5 blame splitter: drives the RAW PostgreSQL DCB engine through the exact marker hand-off the workflow layer
 * uses — append conditioned on the marker the PREVIOUS append's afterCommit returned — with NO workflow code at all.
 * A rejection here means the engine's returned marker does not cover the event it was returned for, i.e. the store,
 * not the workflow change, is at fault.
 *
 * @author Stefan Dragisic
 * @since 5.4.0
 */
class Fnd5PostgresMinimalReproTest {

    private static final String WORKFLOW_ID = "fnd5-min";
    private static final EventCriteria CRITERIA =
            EventCriteria.havingTags(Tag.of("workflowId", WORKFLOW_ID));

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    @SuppressWarnings({"unchecked", "rawtypes"})
    void markerReturnedByAfterCommitMustCoverTheAppendedEvent() {
        EventStorageEngine store = DcbFencingBackends.freshStore(POSTGRES);

        ConsistencyMarker marker = null; // spawn: checked from ORIGIN
        for (int i = 0; i < 50; i++) {
            var condition = marker == null
                    ? AppendCondition.withCriteria(CRITERIA)
                    : AppendCondition.withCriteria(CRITERIA).withMarker(marker);
            EventStorageEngine.AppendTransaction tx;
            try {
                tx = store.appendEvents(condition, null, List.of(tagged(event(i)))).join();
                var commitResult = tx.commit().join();
                marker = (ConsistencyMarker) tx.afterCommit(commitResult).join();
            } catch (Exception e) {
                throw new AssertionError(
                        "FND-5 at RAW ENGINE level: append #" + i + " conditioned on the marker afterCommit returned "
                                + "for append #" + (i - 1) + " (marker=" + marker + ") was rejected — the engine's "
                                + "returned marker does not cover the event it was returned for. Store bug, "
                                + "workflow change exonerated.", e);
            }
            assertThat(marker).as("afterCommit must return a marker").isNotNull();
        }

        // Sanity: all 50 events are actually in the store under the criteria.
        assertThat(DcbFencingBackends.eventsWithTags(store, Tag.of("workflowId", WORKFLOW_ID))).hasSize(50);
    }

    /**
     * The concurrent arm: while one writer chains conditional appends for its own workflowId on the marker each
     * afterCommit returns, a second thread appends unrelated filler events. If the writer is ever rejected, the
     * engine handed back a marker excluding the writer's own just-committed event (the S2 postgres symptom),
     * reproduced with zero workflow code.
     */
    @Test
    @Timeout(value = 600, unit = TimeUnit.SECONDS)
    @SuppressWarnings({"unchecked", "rawtypes"})
    void concurrentUnrelatedAppendsMustNotBreakTheMarkerHandOff() throws Exception {
        EventStorageEngine store = DcbFencingBackends.freshStore(POSTGRES);
        var stop = new java.util.concurrent.atomic.AtomicBoolean(false);
        var fillerFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var filler = new Thread(() -> {
            int i = 0;
            while (!stop.get()) {
                try {
                    var e = new GenericEventMessage(new MessageType("Fnd5Filler"), Map.of("i", i++),
                                                    io.axoniq.framework.workflow.runtime.util.MetadataUtils.create(
                                                            "fnd5-filler",
                                                            io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.STARTED,
                                                            new MessageType("Fnd5Filler")))
                            .withConverter(DcbFencingBackends.converter());
                    EventStorageEngine.AppendTransaction tx =
                            store.appendEvents(AppendCondition.none(), null, List.of(tagged(e))).join();
                    tx.afterCommit(tx.commit().join()).join();
                } catch (Throwable t) {
                    fillerFailure.set(t);
                    return;
                }
            }
        });
        filler.start();
        try {
            ConsistencyMarker marker = null;
            for (int i = 0; i < 200; i++) {
                var condition = marker == null
                        ? AppendCondition.withCriteria(CRITERIA)
                        : AppendCondition.withCriteria(CRITERIA).withMarker(marker);
                try {
                    EventStorageEngine.AppendTransaction tx =
                            store.appendEvents(condition, null, List.of(tagged(event(i)))).join();
                    var commitResult = tx.commit().join();
                    marker = (ConsistencyMarker) tx.afterCommit(commitResult).join();
                } catch (Exception e) {
                    throw new AssertionError(
                            "FND-5 at RAW ENGINE level (concurrent arm): append #" + i + " conditioned on the marker "
                                    + "afterCommit returned for append #" + (i - 1) + " (marker=" + marker + ") was "
                                    + "rejected while only unrelated filler events were appended concurrently. "
                                    + "Store bug, workflow change exonerated.", e);
                }
            }
        } finally {
            stop.set(true);
            filler.join(10_000);
        }
        assertThat(fillerFailure.get()).as("filler thread must not fail").isNull();
        assertThat(DcbFencingBackends.eventsWithTags(store, Tag.of("workflowId", WORKFLOW_ID))).hasSize(200);
    }

    private static EventMessage event(int i) {
        return new GenericEventMessage(new MessageType("Fnd5Probe" + i), Map.of("i", i),
                                       io.axoniq.framework.workflow.runtime.util.MetadataUtils.create(
                                               WORKFLOW_ID,
                                               io.axoniq.framework.workflow.runtime.api.execution.status.WorkflowStatus.STARTED,
                                               new MessageType("Fnd5Probe")))
                .withConverter(DcbFencingBackends.converter());
    }

    private static GenericTaggedEventMessage<EventMessage> tagged(EventMessage event) {
        var tags = new LinkedHashSet<>(new WorkflowEventTagResolver().resolve(event));
        return new GenericTaggedEventMessage<>(event, Set.copyOf(tags));
    }
}
