/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.configuration;

import io.axoniq.framework.workflow.runtime.util.MetadataUtils;
import org.axonframework.messaging.core.sequencing.SequencingPolicy;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Optional;

/**
 * Sequences the events of the workflow history processor, so each event reaches the segment of the workflow whose
 * history it extends exactly once.
 * <p>
 * An event carrying {@code workflowId} metadata, including an event published by a workflow, is sequenced by that id.
 * Any other event is sequenced by its own identifier: the history projector ignores events without a workflow id, so
 * delivering it to one segment is enough. The policy is stateless, so both evaluations of the processor agree.
 * Internal, because it only serves the history processor of a {@link WorkflowModule}.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
final class WorkflowHistorySequencingPolicy implements SequencingPolicy<EventMessage> {

    @Override
    public Optional<Object> sequenceIdentifierFor(EventMessage eventMessage, ProcessingContext processingContext) {
        var metadata = eventMessage.metadata();
        return MetadataUtils.hasWorkflowId().test(metadata)
                ? Optional.of(MetadataUtils.getWorkflowId(metadata))
                : Optional.of(eventMessage.identifier());
    }
}
