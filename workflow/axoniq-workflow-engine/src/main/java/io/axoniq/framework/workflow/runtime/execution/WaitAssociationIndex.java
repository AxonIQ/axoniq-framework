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
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.Segment;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * In-memory index of the open waits of the workflows on the segments this node claimed, so an event is routed only to
 * the workflows waiting for it.
 * <p>
 * Rows are loaded by {@link #segmentLoaded(Segment, Collection)} when a segment is claimed, and kept current by
 * {@link #evolve(EventMessage)} as the processor schedules events. A row may match more events than its wait accepts,
 * never fewer: the waiting workflow still checks its full condition. Routing narrows only while every claimed segment is
 * loaded; otherwise it broadcasts.
 * <p>
 * A row is kept under its event name and the value of one of its {@code =} associations, like the association values of
 * a saga; a lookup reads that value from the event and finds the rows by it, so its cost does not grow with the number
 * of such rows. Rows without such an association are kept under their event name only and checked one by one, and rows
 * without an event name match every event. Every row a lookup finds is still checked against all its associations.
 * <p>
 * Internal, because it is an implementation detail of the engine's event routing.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WaitAssociationIndex {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociationIndex.class);
    private static final Pattern PLAIN_PATH = Pattern.compile("[\\w.$-]+");
    private static final Object WILDCARD = new Object();

    private final Predicate<String> isMember;
    private final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();
    private final ConcurrentHashMap<RowKey, Row> rows = new ConcurrentHashMap<>();
    // Rows by place: a LookupKey, an event name alone, or WILDCARD. A changed row is placed anew before it leaves its
    // old place.
    private final ConcurrentHashMap<Object, Set<RowKey>> places = new ConcurrentHashMap<>();
    // The association paths keyed rows of an event name use. Never shrinks: a definition uses only a few paths.
    private final ConcurrentHashMap<QualifiedName, Set<KeyPath>> keyPaths = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<RowKey>> rowsByWorkflow = new ConcurrentHashMap<>();
    // Counts rows that moved to another place. A lookup that overlaps a move may miss the row, so it tries again.
    private final AtomicLong moves = new AtomicLong();
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
        rowsByWorkflow.keySet()
                      .stream()
                      .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                      .toList()
                      .forEach(this::removeWorkflow);
    }

    /**
     * Forgets every segment and row.
     */
    void clear() {
        claimedSegments.clear();
        rows.clear();
        places.clear();
        keyPaths.clear();
        rowsByWorkflow.clear();
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
                    remove(new RowKey(workflowId, stepName));
            case WaitAssociationChange.WorkflowTerminated(var workflowId) -> removeWorkflow(workflowId);
            case null -> {
                // the event opens or closes no wait
            }
        }
    }

    /**
     * Returns the workflows that may be waiting for the given event. Reads the event's value at each association path
     * that rows of its name are keyed by, and checks only the rows found by those values, by the name alone, or without a
     * name.
     *
     * @param event   the event to route
     * @param context the context the event is routed in
     * @return the identifiers of the workflows with a row matching the event
     * @throws RuntimeException when a value of the event cannot be read, in which case the caller must not narrow
     *                          delivery
     */
    Set<String> match(EventMessage event, ProcessingContext context) {
        for (int attempt = 0; attempt < 3; attempt++) {
            long movesBefore = moves.get();
            var matches = lookup(event, context);
            if (moves.get() == movesBefore) {
                return matches;
            }
        }
        // Rows keep moving: check every row, which a move cannot hide.
        var matches = new HashSet<String>();
        addMatching(rows.keySet(), event, context, matches);
        return matches;
    }

    private Set<String> lookup(EventMessage event, ProcessingContext context) {
        var matches = new HashSet<String>();
        var eventName = event.type().qualifiedName();
        for (var path : keyPaths.getOrDefault(eventName, Set.of())) {
            var value = path.read(event, context);
            if (value != null) {
                addMatching(places.get(new LookupKey(eventName, path, String.valueOf(value))), event, context, matches);
            }
        }
        addMatching(places.get(eventName), event, context, matches);
        addMatching(places.get(WILDCARD), event, context, matches);
        return matches;
    }

    private void addMatching(@Nullable Set<RowKey> candidates,
                             EventMessage event,
                             ProcessingContext context,
                             Set<String> matches) {
        if (candidates == null) {
            return;
        }
        for (var key : candidates) {
            var row = rows.get(key);
            if (row != null && row.matches(event, context)) {
                matches.add(key.workflowId());
            }
        }
    }

    private void put(WaitAssociation wait) {
        var checks = new ArrayList<BiPredicate<EventMessage, ProcessingContext>>();
        var checkable = new ArrayList<SerializedAssociation>();
        for (var association : wait.associations()) {
            try {
                var parsed = SerializedAssociation.parse(registry, association);
                if (isCheckable(parsed)) {
                    checks.add(parsed.asEventMessagePredicate(registry));
                    checkable.add(parsed);
                }
            } catch (RuntimeException e) {
                // An association that cannot be checked is skipped; that only widens the match.
                logger.debug("Not checking association [{}] of wait [{}] of workflow [{}].",
                             association, wait.stepName(), wait.workflowId(), e);
            }
        }
        var key = new RowKey(wait.workflowId(), wait.stepName());
        var row = new Row(wait, List.copyOf(checks), lookupKeyOf(wait, checkable));
        if (row.lookupKey() instanceof LookupKey lookupKey) {
            keyPaths.computeIfAbsent(lookupKey.eventName(), name -> ConcurrentHashMap.newKeySet()).add(lookupKey.path());
        }
        rowsByWorkflow.compute(key.workflowId(), (ignored, keys) -> addTo(keys, key));
        places.compute(row.place(), (ignored, keys) -> addTo(keys, key));
        var previous = rows.put(key, row);
        if (previous != null && !previous.place().equals(row.place())) {
            moves.incrementAndGet();
            places.computeIfPresent(previous.place(), (ignored, keys) -> removeFrom(keys, key));
        }
    }

    /**
     * Keys a row by its first {@code =} association in serialized order, so the same wait always gets the same key. Any
     * of them would do: a lookup checks all associations of every row it finds.
     */
    private static @Nullable LookupKey lookupKeyOf(WaitAssociation wait, List<SerializedAssociation> checkable) {
        var eventName = wait.eventName();
        if (eventName == null) {
            return null;
        }
        return checkable.stream()
                        .min(Comparator.comparing(SerializedAssociation::serialize))
                        .map(association -> new LookupKey(
                                eventName,
                                new KeyPath(association.qualifier(), association.path()),
                                association.value()
                        ))
                        .orElse(null);
    }

    private void remove(RowKey key) {
        var row = rows.remove(key);
        if (row != null) {
            places.computeIfPresent(row.place(), (ignored, keys) -> removeFrom(keys, key));
        }
        rowsByWorkflow.computeIfPresent(key.workflowId(), (ignored, keys) -> removeFrom(keys, key));
    }

    private void removeWorkflow(String workflowId) {
        var keys = rowsByWorkflow.get(workflowId);
        if (keys != null) {
            List.copyOf(keys).forEach(this::remove);
        }
    }

    private static Set<RowKey> addTo(@Nullable Set<RowKey> keys, RowKey key) {
        var result = keys == null ? ConcurrentHashMap.<RowKey>newKeySet() : keys;
        result.add(key);
        return result;
    }

    private static @Nullable Set<RowKey> removeFrom(Set<RowKey> keys, RowKey key) {
        keys.remove(key);
        return keys.isEmpty() ? null : keys;
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

    /**
     * Where an association reads its value: a plain payload or metadata path.
     */
    private record KeyPath(String qualifier, String path) {

        @Nullable Object read(EventMessage event, ProcessingContext context) {
            var retriever = PayloadPropertyValueRetriever.QUALIFIER.equals(qualifier)
                    ? PayloadPropertyValueRetriever.payloadProperty(path)
                    : MetadataPropertyValueRetriever.metadataProperty(path);
            return retriever.apply(event, context);
        }
    }

    /**
     * The event name, path and value a keyed row waits for. The value is text, because {@code =} compares the text of
     * the event's value, so a lookup by it finds exactly the rows that association accepts.
     */
    private record LookupKey(QualifiedName eventName, KeyPath path, String value) {

    }

    private record Row(WaitAssociation association,
                       List<BiPredicate<EventMessage, ProcessingContext>> checks,
                       @Nullable LookupKey lookupKey) {

        Object place() {
            var eventName = association.eventName();
            if (eventName == null) {
                return WILDCARD;
            }
            return lookupKey != null ? lookupKey : eventName;
        }

        boolean matches(EventMessage event, ProcessingContext context) {
            if (association.isWildcard()) {
                return true;
            }
            return event.type().qualifiedName().equals(association.eventName())
                    && checks.stream().allMatch(check -> check.test(event, context));
        }
    }
}
