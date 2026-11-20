/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.annotation.Event;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Event-sourced {@link WaitAssociations}.
 * <p>
 * Sourced from the events tagged {@code workflowEvent=waitForStep}, which open and close waits, together with the
 * events tagged {@code workflowEvent=lifecycle}, because a workflow that reaches a terminal status ends its waits
 * without writing step events for them. The rows follow the rules of {@link WaitAssociationChange}.
 * <p>
 * Example rows, one {@link WaitAssociation} per open wait:
 * <pre>
 * workflowId | stepName      | eventName       | associations
 * -----------+---------------+-----------------+------------------------------------------
 * order-1    | awaitPayment  | PaymentReceived | payload:orderId=42
 * order-2    | awaitShipping | OrderShipped    | payload:orderId=43, metadata:tenant=acme
 * order-3    | cooldown      | java.lang.Void  | (none: a sleep, never matches an event)
 * </pre>
 * <p>
 * The {@link Event#version() version} is the release that introduced the snapshot format, see {@link #getWaits()}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
@Event(version = "5.5.0")
@JsonAutoDetect(
        fieldVisibility = JsonAutoDetect.Visibility.NONE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE
)
public class EventSourcedWaitAssociations implements WaitAssociations {

    /**
     * Singleton identifier of the wait-associations entity.
     */
    public static final String ENTITY_ID = "__wait-associations";

    private final Map<String, Map<String, WaitAssociation>> associationsPerWorkflow = new LinkedHashMap<>();

    /**
     * Returns the criteria selecting the events that open and close waits.
     *
     * @return event criteria for wait associations reconstruction
     */
    public static EventCriteria criteriaBuilder() {
        return EventCriteria.either(
                EventCriteria.havingTags(Tag.of(WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE,
                                                WorkflowEventTags.TAG_VALUE_EVENT_TYPE_WAIT_STEP)),
                EventCriteria.havingTags(Tag.of(WorkflowEventTags.TAG_WORKFLOW_EVENT_TYPE,
                                                WorkflowEventTags.TAG_VALUE_EVENT_TYPE_LIFECYCLE))
        );
    }

    /**
     * Applies the given workflow event to the open waits.
     *
     * @param event a wait-step or lifecycle event
     */
    public void evolve(EventMessage event) {
        switch (WaitAssociationChange.of(event)) {
            case WaitAssociationChange.WaitStarted(var association) -> add(association);
            case WaitAssociationChange.WaitTerminated(var workflowId, var stepName) -> {
                var rows = associationsPerWorkflow.get(workflowId);
                if (rows != null) {
                    rows.remove(stepName);
                    if (rows.isEmpty()) {
                        associationsPerWorkflow.remove(workflowId);
                    }
                }
            }
            case WaitAssociationChange.WorkflowTerminated(var workflowId) ->
                    associationsPerWorkflow.remove(workflowId);
            case null -> {
                // the event opens or closes no wait
            }
        }
    }

    private void add(WaitAssociation association) {
        associationsPerWorkflow.computeIfAbsent(association.workflowId(), id -> new LinkedHashMap<>())
                               .put(association.stepName(), association);
    }

    /**
     * Returns the open waits as plain values, for a converter writing this list as a snapshot.
     *
     * @return the plain values of the open waits
     */
    @JsonProperty("waits")
    public List<WaitSnapshot> getWaits() {
        return waitAssociations().stream().map(WaitSnapshot::of).toList();
    }

    /**
     * Sets the open waits from plain values, for a converter reading this list from a snapshot.
     *
     * @param waits the plain values of the open waits
     */
    @JsonProperty("waits")
    public void setWaits(List<WaitSnapshot> waits) {
        associationsPerWorkflow.clear();
        waits.forEach(wait -> add(wait.toWaitAssociation()));
    }

    @Override
    public Collection<WaitAssociation> waitAssociations() {
        return associationsPerWorkflow.values()
                                      .stream()
                                      .flatMap(rows -> rows.values().stream())
                                      .collect(Collectors.toUnmodifiableList());
    }

    /**
     * The plain values of a {@link WaitAssociation}, which a converter can write and read.
     *
     * @param workflowId   the identifier of the waiting workflow
     * @param stepName     the name of the waiting step
     * @param eventName    the qualified name of the awaited event, or {@code null} for a row that matches every routed
     *                     event
     * @param associations the canonical serialized associations the awaited event must all satisfy
     */
    public record WaitSnapshot(String workflowId,
                               String stepName,
                               @Nullable String eventName,
                               Set<String> associations) {

        private static WaitSnapshot of(WaitAssociation wait) {
            var eventName = wait.eventName();
            return new WaitSnapshot(wait.workflowId(),
                                    wait.stepName(),
                                    eventName == null ? null : eventName.name(),
                                    wait.associations());
        }

        private WaitAssociation toWaitAssociation() {
            return new WaitAssociation(workflowId,
                                       stepName,
                                       eventName == null ? null : new QualifiedName(eventName),
                                       associations);
        }
    }
}
