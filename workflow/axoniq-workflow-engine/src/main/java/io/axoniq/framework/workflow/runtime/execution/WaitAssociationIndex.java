/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import io.axoniq.framework.workflow.runtime.association.EqualsComparison;
import io.axoniq.framework.workflow.runtime.association.MetadataPropertyValueRetriever;
import io.axoniq.framework.workflow.runtime.association.PayloadPropertyValueRetriever;
import io.axoniq.framework.workflow.runtime.association.SerializedAssociation;
import io.axoniq.framework.workflow.runtime.association.ValueComparisonOperatorRegistry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * In-memory index of the open waits of the workflows on the segments this node claimed, so an event is routed only to
 * the workflows waiting for it.
 * <p>
 * Rows are loaded by {@link #segmentLoaded(Segment, Collection)} when a segment is claimed, and kept current by
 * {@link #evolve(EventMessage)} as the processor schedules events. A lookup checks every row. A row may match more
 * events than its wait accepts, never fewer: the waiting workflow still checks its full condition. Routing narrows only
 * while every claimed segment is loaded; otherwise it broadcasts.
 * <p>
 * Internal, because it is an implementation detail of the engine's event routing.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WaitAssociationIndex {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociationIndex.class);
    private static final Pattern PLAIN_PATH = Pattern.compile("[\\w.$-]+");

    private final Predicate<String> isMember;
    private final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();
    private final ConcurrentHashMap<RowKey, Row> rows = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ClaimedSegment> claimedSegments = new ConcurrentHashMap<>();

    /**
     * Creates an empty index.
     *
     * @param isMember tells whether a workflow belongs to the owning engine and is not terminal, so its waits are
     *                 indexed when its STARTED event is applied
     */
    WaitAssociationIndex(Predicate<String> isMember) {
        this.isMember = Objects.requireNonNull(isMember, "The membership predicate must not be null.");
    }

    /**
     * Records that this node claimed the given segment and is loading its rows.
     *
     * @param segment the claimed segment
     */
    void segmentClaimed(Segment segment) {
        claimedSegments.put(segment.getSegmentId(), new ClaimedSegment(segment, false));
    }

    /**
     * Adds the given rows of a claimed segment and marks the segment loaded.
     *
     * @param segment the claimed segment
     * @param waits   the open waits of the workflows the segment owns
     */
    void segmentLoaded(Segment segment, Collection<WaitAssociation> waits) {
        waits.forEach(this::put);
        claimedSegments.put(segment.getSegmentId(), new ClaimedSegment(segment, true));
    }

    /**
     * Forgets the given segment and drops the rows of the workflows it owns.
     *
     * @param segment the released segment
     */
    void segmentReleased(Segment segment) {
        claimedSegments.computeIfPresent(segment.getSegmentId(),
                                         (id, claimed) -> claimed.segment().equals(segment) ? null : claimed);
        rows.keySet().removeIf(key -> WorkflowSegmentOwnership.ownedBy(segment, key.workflowId()));
    }

    /**
     * Forgets every segment and row.
     */
    void clear() {
        claimedSegments.clear();
        rows.clear();
    }

    /**
     * Returns whether every segment this node claimed is loaded.
     *
     * @return {@code true} when no claimed segment is still loading
     */
    boolean allClaimedSegmentsLoaded() {
        return claimedSegments.values().stream().allMatch(ClaimedSegment::loaded);
    }

    /**
     * Returns whether every segment this node claimed satisfies the given predicate.
     *
     * @param predicate the condition every claimed segment must meet
     * @return {@code true} when no claimed segment fails the predicate
     */
    boolean allClaimedSegmentsMatch(Predicate<Segment> predicate) {
        return claimedSegments.values().stream().map(ClaimedSegment::segment).allMatch(predicate);
    }

    /**
     * Applies the given event to the open waits, following {@link WaitAssociationChange}. Applying the same event again
     * changes nothing.
     *
     * @param event the event the processor schedules
     */
    void evolve(EventMessage event) {
        switch (WaitAssociationChange.of(event)) {
            case WaitAssociationChange.WaitStarted(var wait) -> {
                if (isMember.test(wait.workflowId())) {
                    put(wait);
                }
            }
            case WaitAssociationChange.WaitTerminated(var workflowId, var stepName) ->
                    rows.remove(new RowKey(workflowId, stepName));
            case WaitAssociationChange.WorkflowTerminated(var workflowId) ->
                    rows.keySet().removeIf(key -> key.workflowId().equals(workflowId));
            case null -> {
                // the event opens or closes no wait
            }
        }
    }

    /**
     * Returns the workflows that may be waiting for the given event, by checking every row.
     *
     * @param event   the event to route
     * @param context the context the event is routed in
     * @return the identifiers of the workflows with a row matching the event
     * @throws RuntimeException when a value of the event cannot be read, in which case the caller must not narrow
     *                          delivery
     */
    Set<String> match(EventMessage event, ProcessingContext context) {
        var matches = new HashSet<String>();
        for (var row : rows.values()) {
            if (row.matches(event, context)) {
                matches.add(row.association().workflowId());
            }
        }
        return matches;
    }

    private void put(WaitAssociation wait) {
        var checks = new ArrayList<BiPredicate<EventMessage, ProcessingContext>>();
        for (var association : wait.associations()) {
            try {
                var parsed = SerializedAssociation.parse(registry, association);
                if (isCheckable(parsed)) {
                    checks.add(parsed.asEventMessagePredicate(registry));
                }
            } catch (RuntimeException e) {
                // An association that cannot be checked is skipped; that only widens the match.
                logger.debug("Not checking association [{}] of wait [{}] of workflow [{}].",
                             association, wait.stepName(), wait.workflowId(), e);
            }
        }
        rows.put(new RowKey(wait.workflowId(), wait.stepName()), new Row(wait, List.copyOf(checks)));
    }

    /**
     * Only an {@code =} association on a plain payload or metadata path is checked. Any other association is skipped,
     * so a value the default operator registry cannot parse never hides a waiting workflow.
     */
    private static boolean isCheckable(SerializedAssociation association) {
        return EqualsComparison.OPERATOR.equals(association.operator())
                && PLAIN_PATH.matcher(association.path()).matches()
                && (PayloadPropertyValueRetriever.QUALIFIER.equals(association.qualifier())
                || MetadataPropertyValueRetriever.QUALIFIER.equals(association.qualifier()));
    }

    private record ClaimedSegment(Segment segment, boolean loaded) {

    }

    private record RowKey(String workflowId, String stepName) {

    }

    private record Row(WaitAssociation association, List<BiPredicate<EventMessage, ProcessingContext>> checks) {

        boolean matches(EventMessage event, ProcessingContext context) {
            if (association.isWildcard()) {
                return true;
            }
            return event.type().qualifiedName().equals(association.eventName())
                    && checks.stream().allMatch(check -> check.test(event, context));
        }
    }
}
