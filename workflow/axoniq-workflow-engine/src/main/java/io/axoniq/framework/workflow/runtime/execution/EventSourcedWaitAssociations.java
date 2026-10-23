/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
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
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public class EventSourcedWaitAssociations implements WaitAssociations {

    /**
     * Singleton identifier of the wait-associations entity.
     */
    public static final String ENTITY_ID = "__wait-associations";

    private final Map<String, Map<String, WaitAssociation>> rowsByWorkflow = new LinkedHashMap<>();

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
            case WaitAssociationChange.WaitStarted(var row) ->
                    rowsByWorkflow.computeIfAbsent(row.workflowId(), id -> new LinkedHashMap<>())
                                  .put(row.stepName(), row);
            case WaitAssociationChange.WaitTerminated(var workflowId, var stepName) -> {
                var rows = rowsByWorkflow.get(workflowId);
                if (rows != null) {
                    rows.remove(stepName);
                    if (rows.isEmpty()) {
                        rowsByWorkflow.remove(workflowId);
                    }
                }
            }
            case WaitAssociationChange.WorkflowTerminated(var workflowId) -> rowsByWorkflow.remove(workflowId);
            case null -> {
                // the event opens or closes no wait
            }
        }
    }

    @Override
    public Collection<WaitAssociation> waitAssociations() {
        return rowsByWorkflow.values()
                             .stream()
                             .flatMap(rows -> rows.values().stream())
                             .collect(Collectors.toUnmodifiableList());
    }
}
