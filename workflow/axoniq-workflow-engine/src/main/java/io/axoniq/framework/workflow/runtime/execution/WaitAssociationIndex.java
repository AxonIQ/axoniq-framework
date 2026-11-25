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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

/**
 * In-memory index of the open waits of the hibernated workflows of a {@link WorkflowEngine}, so a broadcast event wakes
 * only the hibernated workflows that may wait for it.
 * <p>
 * The {@link LiveWorkflows} registry owns it: it adds the open waits of a workflow when the workflow hibernates or
 * is adopted as hibernated, and removes them when it wakes or is dropped, each time under the lock of that workflow.
 * The waits are parsed into {@link Row rows} beforehand, see {@link #rowOf(String, String, QualifiedName, Collection)},
 * so adding them only updates the index. A row may match more events
 * than its wait accepts, never fewer: the woken workflow still checks its full condition. A wait whose condition is a
 * predicate has no associations, so it matches every event of its name.
 * <p>
 * A row is kept under its event name and the value of one of its {@code =} associations, like the association values
 * of a saga; a lookup reads that value from the event and finds the rows by it, so its cost does not grow with the
 * number of such rows. Rows without such an association are kept under their event name only and checked one by one,
 * and rows without an event name match every event. Every row a lookup finds is still checked against all its
 * associations. A value that cannot be read never hides a row: the lookup then checks every row, and a check that
 * fails counts as a match.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WaitAssociationIndex {

    private static final Logger logger = LoggerFactory.getLogger(WaitAssociationIndex.class);
    private static final Pattern PLAIN_PATH = Pattern.compile("[\\w.$-]+");
    private static final Object WILDCARD = new Object();

    private final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();
    private final ConcurrentHashMap<RowKey, IndexedWait> rows = new ConcurrentHashMap<>();
    // Rows by place: a LookupKey, an event name alone, or WILDCARD.
    private final ConcurrentHashMap<Object, Set<RowKey>> places = new ConcurrentHashMap<>();
    // The association paths keyed rows of an event name use. Never shrinks: a definition uses only a few paths.
    private final ConcurrentHashMap<QualifiedName, Set<KeyPath>> keyPaths = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<RowKey>> rowsByWorkflow = new ConcurrentHashMap<>();

    /**
     * Parses an open wait of a workflow into the row this index keeps for it, without adding it.
     *
     * @param workflowId   the identifier of the waiting workflow
     * @param stepName     the name of the waiting step
     * @param eventName    the name of the awaited event, or {@code null} to match every event
     * @param associations the canonical serialized associations the awaited event must all satisfy
     * @return the row of the wait, to {@link #add(Row) add} once the workflow hibernates
     */
    Row rowOf(String workflowId, String stepName, @Nullable QualifiedName eventName, Collection<String> associations) {
        var checks = new ArrayList<BiPredicate<EventMessage, ProcessingContext>>();
        var checkable = new ArrayList<SerializedAssociation>();
        for (var association : associations) {
            try {
                var parsed = SerializedAssociation.parse(registry, association);
                if (isCheckable(parsed)) {
                    checks.add(parsed.asEventMessagePredicate(registry));
                    checkable.add(parsed);
                }
            } catch (RuntimeException e) {
                logger.debug("Not checking association [{}] of wait [{}] of workflow [{}]; this only widens its "
                                     + "match.", association, stepName, workflowId, e);
            }
        }
        return new Row(new RowKey(workflowId, stepName),
                       new IndexedWait(eventName, List.copyOf(checks), lookupKeyOf(eventName, checkable)));
    }

    /**
     * Adds the given row of an open wait of a hibernated workflow, replacing an earlier row of the same step.
     *
     * @param row the row, parsed by {@link #rowOf(String, String, QualifiedName, Collection)}
     */
    void add(Row row) {
        var key = row.key();
        var indexed = row.indexed();
        var workflowId = key.workflowId();
        remove(key);
        if (indexed.lookupKey() instanceof LookupKey lookupKey) {
            keyPaths.computeIfAbsent(lookupKey.eventName(), name -> ConcurrentHashMap.newKeySet())
                    .add(lookupKey.path());
        }
        rowsByWorkflow.compute(workflowId, (ignored, keys) -> addTo(keys, key));
        places.compute(indexed.place(), (ignored, keys) -> addTo(keys, key));
        rows.put(key, indexed);
    }

    /**
     * Removes every row of the given workflow.
     *
     * @param workflowId the identifier of the workflow
     */
    void removeWorkflow(String workflowId) {
        var keys = rowsByWorkflow.get(workflowId);
        if (keys != null) {
            List.copyOf(keys).forEach(this::remove);
        }
    }

    /**
     * Removes every row.
     */
    void clear() {
        rows.clear();
        places.clear();
        rowsByWorkflow.clear();
    }

    /**
     * Returns the workflows that may be waiting for the given event. Reads the event's value at each association path
     * that rows of its name are keyed by, and checks only the rows found by those values, by the name alone, or
     * without a name. When a value cannot be read, every row is checked instead.
     *
     * @param event   the event to route
     * @param context the context the event is routed in
     * @return the identifiers of the workflows with a row that may match the event
     */
    Set<String> match(EventMessage event, ProcessingContext context) {
        var matches = new HashSet<String>();
        var eventName = event.type().qualifiedName();
        try {
            for (var path : keyPaths.getOrDefault(eventName, Set.of())) {
                var value = path.read(event, context);
                if (value != null) {
                    addMatching(places.get(new LookupKey(eventName, path, String.valueOf(value))),
                                event, context, matches);
                }
            }
        } catch (RuntimeException e) {
            logger.debug("Could not read a value of event [{}]; checking every hibernated wait.", eventName, e);
            addMatching(rows.keySet(), event, context, matches);
            return matches;
        }
        addMatching(places.get(eventName), event, context, matches);
        addMatching(places.get(WILDCARD), event, context, matches);
        return matches;
    }

    /**
     * Returns whether one of the rows of the given workflow may match the given event.
     *
     * @param workflowId the identifier of the workflow
     * @param event      the event to check
     * @param context    the context the event is routed in
     * @return {@code true} when a row of the workflow may match the event
     */
    boolean matches(String workflowId, EventMessage event, ProcessingContext context) {
        var keys = rowsByWorkflow.getOrDefault(workflowId, Set.of());
        var matches = new HashSet<String>();
        addMatching(keys, event, context, matches);
        return !matches.isEmpty();
    }

    private void addMatching(@Nullable Collection<RowKey> candidates,
                             EventMessage event,
                             ProcessingContext context,
                             Set<String> matches) {
        if (candidates == null) {
            return;
        }
        for (var key : candidates) {
            var indexed = rows.get(key);
            if (indexed != null && indexed.matches(event, context)) {
                matches.add(key.workflowId());
            }
        }
    }

    /**
     * Keys a row by its first {@code =} association in serialized order, so the same wait always gets the same key.
     * Any of them would do: a lookup checks all associations of every row it finds.
     */
    private static @Nullable LookupKey lookupKeyOf(@Nullable QualifiedName eventName,
                                                   List<SerializedAssociation> checkable) {
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
        var indexed = rows.remove(key);
        if (indexed != null) {
            places.computeIfPresent(indexed.place(), (ignored, keys) -> removeFrom(keys, key));
        }
        rowsByWorkflow.computeIfPresent(key.workflowId(), (ignored, keys) -> removeFrom(keys, key));
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
     * so a value the default operator registry cannot parse never hides a waiting workflow. A lookup also needs the
     * exact text of one value, which only {@code =} gives.
     */
    private static boolean isCheckable(SerializedAssociation association) {
        return EqualsComparison.OPERATOR.equals(association.operator())
                && PLAIN_PATH.matcher(association.path()).matches()
                && (PayloadPropertyValueRetriever.QUALIFIER.equals(association.qualifier())
                || MetadataPropertyValueRetriever.QUALIFIER.equals(association.qualifier()));
    }

    private record RowKey(String workflowId, String stepName) {

    }

    /**
     * An open wait of a workflow, parsed into the row the index keeps for it.
     *
     * @param key     the workflow and step of the wait
     * @param indexed the wait as the index keeps it
     */
    record Row(RowKey key, IndexedWait indexed) {

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

    /**
     * An open wait as the index keeps it. A {@code null} event name matches every event.
     */
    private record IndexedWait(@Nullable QualifiedName eventName,
                               List<BiPredicate<EventMessage, ProcessingContext>> checks,
                               @Nullable LookupKey lookupKey) {

        Object place() {
            if (eventName == null) {
                return WILDCARD;
            }
            return lookupKey != null ? lookupKey : eventName;
        }

        boolean matches(EventMessage event, ProcessingContext context) {
            if (eventName == null) {
                return true;
            }
            if (!event.type().qualifiedName().equals(eventName)) {
                return false;
            }
            for (var check : checks) {
                try {
                    if (!check.test(event, context)) {
                        return false;
                    }
                } catch (RuntimeException e) {
                    // A value that cannot be checked never hides a waiting workflow.
                    return true;
                }
            }
            return true;
        }
    }
}
