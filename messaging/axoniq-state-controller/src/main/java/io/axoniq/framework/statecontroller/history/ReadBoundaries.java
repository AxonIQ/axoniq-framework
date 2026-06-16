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

package io.axoniq.framework.statecontroller.history;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventstreaming.EventCriteria;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Accumulates the {@link EventCriteria consistency boundaries} a decision actually read through the
 * {@link History} surface, keyed on the in-flight {@link ProcessingContext}.
 * <p>
 * Each time a {@link SourcedHistory} materializes a scope it {@link #record(ProcessingContext, EventCriteria)
 * records} the folded {@link EventCriteria} that defines exactly which events that read observed — its Dynamic
 * Consistency Boundary (DCB) read surface. The decision-dispatch side then {@link #readCriteria(ProcessingContext)
 * reads back} this accumulated list to enforce that every accepted event is covered by at least one boundary the
 * decision read, closing the tagging-drift gap where an appended event tagged differently from any read scope
 * would slip past the DCB optimistic lock.
 * <p>
 * Only the {@link History} path populates this resource. The legacy
 * {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext} /
 * {@code SourcedEventStream} path never touches it, so for any command dispatched through that surface
 * {@link #readCriteria(ProcessingContext)} returns an empty list and the coverage guard never fires.
 * <p>
 * <h3>Threading.</h3>
 * The accumulated list is mutated without synchronization; this relies on AF5's
 * single-thread-per-{@link ProcessingContext} guarantee, the same contract {@link SourcedHistory} and
 * {@link HistoryFactory} depend on.
 * <p>
 * Marked {@link Internal @Internal} because it is wired by the State Controller's command-dispatch pipeline; it is
 * not part of the public {@link History} surface.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public final class ReadBoundaries {

    /**
     * Resource key under which the list of read {@link EventCriteria consistency boundaries} for a given
     * {@link ProcessingContext} is accumulated. Private because access goes through
     * {@link #record(ProcessingContext, EventCriteria)} and {@link #readCriteria(ProcessingContext)}.
     */
    private static final ResourceKey<List<EventCriteria>> READ_CRITERIA_KEY =
            ResourceKey.withLabel("io.axoniq.framework.statecontroller.ReadBoundaries");

    private ReadBoundaries() {
        // utility
    }

    /**
     * Records the {@code criteria} a {@link History} scope read against, appending it to the accumulating list for
     * the given {@code processingContext}. Each materialized scope (including every term of a multi-scope decision
     * and the folded multi-term builder criteria) contributes exactly one entry.
     *
     * @param processingContext the current processing context the read participates in
     * @param criteria          the folded {@link EventCriteria} defining the read scope's DCB read surface
     */
    public static void record(ProcessingContext processingContext, EventCriteria criteria) {
        Objects.requireNonNull(processingContext, "processingContext must not be null");
        Objects.requireNonNull(criteria, "criteria must not be null");
        processingContext.computeResourceIfAbsent(READ_CRITERIA_KEY, ArrayList::new).add(criteria);
    }

    /**
     * Returns the {@link EventCriteria consistency boundaries} recorded for the given {@code processingContext}
     * through the {@link History} surface, in the order they were read.
     * <p>
     * Returns an empty, unmodifiable list when no {@link History} scope was materialized — which is always the
     * case for the legacy {@link io.axoniq.framework.statecontroller.decisions.DecisionContext DecisionContext}
     * path, signalling the coverage guard to stay disabled.
     *
     * @param processingContext the current processing context
     * @return the recorded read criteria in read order, or an empty list when none were recorded
     */
    public static List<EventCriteria> readCriteria(ProcessingContext processingContext) {
        Objects.requireNonNull(processingContext, "processingContext must not be null");
        List<EventCriteria> recorded = processingContext.getResource(READ_CRITERIA_KEY);
        return recorded == null ? List.of() : recorded;
    }
}
