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
 * Open waits are loaded by {@link #segmentLoaded(Segment, Collection)} when a segment is claimed, and kept current by
 * {@link #evolve(EventMessage)} as the processor schedules events. An open wait may match more events than the wait
 * accepts, never fewer: the waiting workflow still checks its full condition. Routing narrows only while every claimed
 * segment is loaded; otherwise it broadcasts.
 * <p>
 * An open wait is kept under its event name and the value of one of its {@code =} associations; a lookup reads that
 * value from the event and finds the open waits by it, so its cost does not grow with the number of such waits. Open
 * waits without such an association are kept under their event name only and checked one by one, and open waits
 * without an event name match every event. Every open wait a lookup finds is still checked against all its
 * associations.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WaitAssociationIndex {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociationIndex.class);
    private static final Pattern PLAIN_PATH = Pattern.compile("[\\w.$-]+");

    private final Predicate<String> isMember;
    private final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();
    private final ConcurrentHashMap<WaitAssociationKey, IndexedWait> openWaits = new ConcurrentHashMap<>();
    // A changed open wait is added under its new placement before it leaves its old one.
    private final ConcurrentHashMap<Placement, Set<WaitAssociationKey>> openWaitKeysByPlacement =
            new ConcurrentHashMap<>();
    // The association paths that open waits of an event name are placed by. Never shrinks: a definition uses only a
    // few paths.
    private final ConcurrentHashMap<QualifiedName, Set<KeyPath>> keyPaths = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<WaitAssociationKey>> openWaitKeysByWorkflow = new ConcurrentHashMap<>();
    // Counts open waits whose placement changed. A lookup that runs during such a change may miss the wait, so it
    // tries again.
    private final AtomicLong placementChanges = new AtomicLong();
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
     * Records that this node claimed the given segment and is loading its open waits.
     *
     * @param segment the claimed segment
     */
    void segmentClaimed(Segment segment) {
        claimedSegments.put(segment.getSegmentId(), new ClaimedSegment(segment, false));
    }

    /**
     * Adds the given open waits of a claimed segment and marks the segment loaded.
     *
     * @param segment the claimed segment
     * @param waits   the open waits of the workflows the segment owns
     */
    void segmentLoaded(Segment segment, Collection<WaitAssociation> waits) {
        waits.forEach(this::put);
        claimedSegments.put(segment.getSegmentId(), new ClaimedSegment(segment, true));
    }

    /**
     * Forgets the given segment and drops the open waits of the workflows it owns.
     *
     * @param segment the released segment
     */
    void segmentReleased(Segment segment) {
        claimedSegments.computeIfPresent(segment.getSegmentId(),
                                         (id, claimed) -> claimed.segment().equals(segment) ? null : claimed);
        openWaitKeysByWorkflow.keySet()
                              .stream()
                              .filter(workflowId -> WorkflowSegmentOwnership.ownedBy(segment, workflowId))
                              .toList()
                              .forEach(this::removeWorkflow);
    }

    /**
     * Forgets every segment and open wait.
     */
    void clear() {
        claimedSegments.clear();
        openWaits.clear();
        openWaitKeysByPlacement.clear();
        keyPaths.clear();
        openWaitKeysByWorkflow.clear();
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
                    remove(new WaitAssociationKey(workflowId, stepName));
            case WaitAssociationChange.WorkflowTerminated(var workflowId) -> removeWorkflow(workflowId);
            case null -> {
                // the event opens or closes no wait
            }
        }
    }

    /**
     * Returns the workflows that may be waiting for the given event. Reads the event's value at each association path
     * that open waits of its name are placed by, and checks only the open waits found by those values, by the name
     * alone, or without a name.
     *
     * @param event   the event to route
     * @param context the context the event is routed in
     * @return the identifiers of the workflows with an open wait matching the event
     * @throws RuntimeException when a value of the event cannot be read, in which case the caller must not narrow
     *                          delivery
     */
    Set<String> match(EventMessage event, ProcessingContext context) {
        for (int attempt = 0; attempt < 3; attempt++) {
            long placementChangesBefore = placementChanges.get();
            var matches = lookup(event, context);
            if (placementChanges.get() == placementChangesBefore) {
                return matches;
            }
        }
        // Placements keep changing: check every open wait, which a placement change cannot hide.
        return findMatching(openWaits.keySet(), event, context);
    }

    private Set<String> lookup(EventMessage event, ProcessingContext context) {
        var matches = new HashSet<String>();
        var eventName = event.type().qualifiedName();
        for (var path : keyPaths.getOrDefault(eventName, Set.of())) {
            var value = path.read(event, context);
            if (value != null) {
                var placement = new Placement.ByValue(eventName, path, String.valueOf(value));
                matches.addAll(findMatching(openWaitKeysByPlacement.get(placement), event, context));
            }
        }
        matches.addAll(findMatching(openWaitKeysByPlacement.get(new Placement.ByEventName(eventName)),
                                    event,
                                    context));
        matches.addAll(findMatching(openWaitKeysByPlacement.get(Placement.ANY_EVENT), event, context));
        return matches;
    }

    private Set<String> findMatching(@Nullable Set<WaitAssociationKey> candidates,
                                     EventMessage event,
                                     ProcessingContext context) {
        var matches = new HashSet<String>();
        if (candidates == null) {
            return matches;
        }
        for (var key : candidates) {
            var openWait = openWaits.get(key);
            if (openWait != null && openWait.matches(event, context)) {
                matches.add(key.workflowId());
            }
        }
        return matches;
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
                logger.debug("Not checking association [{}] of wait [{}] of workflow [{}]; this only widens its "
                                     + "match.", association, wait.stepName(), wait.workflowId(), e);
            }
        }
        var key = new WaitAssociationKey(wait.workflowId(), wait.stepName());
        var openWait = new IndexedWait(wait.eventName(), List.copyOf(checks), placementOf(wait, checkable));
        if (openWait.placement() instanceof Placement.ByValue byValue) {
            keyPaths.computeIfAbsent(byValue.eventName(), name -> ConcurrentHashMap.newKeySet()).add(byValue.path());
        }
        openWaitKeysByWorkflow.compute(key.workflowId(), (ignored, keys) -> addTo(keys, key));
        openWaitKeysByPlacement.compute(openWait.placement(), (ignored, keys) -> addTo(keys, key));
        var previous = openWaits.put(key, openWait);
        if (previous != null && !previous.placement().equals(openWait.placement())) {
            placementChanges.incrementAndGet();
            openWaitKeysByPlacement.computeIfPresent(previous.placement(), (ignored, keys) -> removeFrom(keys, key));
        }
    }

    /**
     * Places an open wait by its first {@code =} association in serialized order, so the same wait always gets the
     * same placement. Any of them would do: a lookup checks all associations of every open wait it finds.
     */
    private static Placement placementOf(WaitAssociation wait, List<SerializedAssociation> checkable) {
        var eventName = wait.eventName();
        if (eventName == null) {
            return Placement.ANY_EVENT;
        }
        return checkable.stream()
                        .min(Comparator.comparing(SerializedAssociation::serialize))
                        .<Placement>map(association -> new Placement.ByValue(
                                eventName,
                                new KeyPath(association.qualifier(), association.path()),
                                association.value()
                        ))
                        .orElse(new Placement.ByEventName(eventName));
    }

    private void remove(WaitAssociationKey key) {
        var openWait = openWaits.remove(key);
        if (openWait != null) {
            openWaitKeysByPlacement.computeIfPresent(openWait.placement(), (ignored, keys) -> removeFrom(keys, key));
        }
        openWaitKeysByWorkflow.computeIfPresent(key.workflowId(), (ignored, keys) -> removeFrom(keys, key));
    }

    private void removeWorkflow(String workflowId) {
        var keys = openWaitKeysByWorkflow.get(workflowId);
        if (keys != null) {
            List.copyOf(keys).forEach(this::remove);
        }
    }

    private static Set<WaitAssociationKey> addTo(@Nullable Set<WaitAssociationKey> keys, WaitAssociationKey key) {
        var result = keys == null ? ConcurrentHashMap.<WaitAssociationKey>newKeySet() : keys;
        result.add(key);
        return result;
    }

    private static @Nullable Set<WaitAssociationKey> removeFrom(Set<WaitAssociationKey> keys, WaitAssociationKey key) {
        keys.remove(key);
        return keys.isEmpty() ? null : keys;
    }

    /**
     * Only an {@code =} association on a plain payload or metadata path is checked. Any other association is skipped,
     * so a value the default operator registry cannot parse never hides a waiting workflow. A lookup also needs the
     * exact text of one value, which only {@code =} gives.
     */
    private static boolean isCheckable(SerializedAssociation association) {
        return EqualsComparison.OPERATOR.equals(association.operator())
                && PLAIN_PATH.matcher(association.path()).matches()
                && (PayloadPropertyValueRetriever.QUALIFIER.equals(association.qualifier())
                || MetadataPropertyValueRetriever.QUALIFIER.equals(association.qualifier()));
    }

    private record ClaimedSegment(Segment segment, boolean loaded) {

    }

    private record WaitAssociationKey(String workflowId, String stepName) {

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
     * Where the index keeps an open wait, so a lookup for an event finds it.
     */
    private sealed interface Placement {

        /**
         * The placement of every open wait without an event name, which matches every event.
         */
        Placement ANY_EVENT = new AnyEvent();

        /**
         * An open wait with an {@code =} association, found by the event's value at that association's path. The value
         * is text, because {@code =} compares the text of the event's value, so a lookup by it finds exactly the open
         * waits that association accepts.
         */
        record ByValue(QualifiedName eventName, KeyPath path, String value) implements Placement {

        }

        /**
         * An open wait without an {@code =} association, found by its event name alone and checked one by one.
         */
        record ByEventName(QualifiedName eventName) implements Placement {

        }

        /**
         * An open wait whose STARTED payload could not be read, found for every event.
         */
        record AnyEvent() implements Placement {

        }
    }

    /**
     * An open wait as the index keeps it. A {@code null} event name matches every event.
     */
    private record IndexedWait(@Nullable QualifiedName eventName,
                               List<BiPredicate<EventMessage, ProcessingContext>> checks,
                               Placement placement) {

        boolean matches(EventMessage event, ProcessingContext context) {
            if (eventName == null) {
                return true;
            }
            return event.type().qualifiedName().equals(eventName)
                    && checks.stream().allMatch(check -> check.test(event, context));
        }
    }
}
