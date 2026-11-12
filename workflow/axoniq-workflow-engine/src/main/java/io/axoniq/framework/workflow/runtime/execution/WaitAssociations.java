/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution;

import org.axonframework.common.annotation.Internal;

import java.util.Collection;

/**
 * The durable record of which workflow waits for which event, loaded through
 * {@link WorkflowStore#loadWaitAssociations(org.axonframework.messaging.core.unitofwork.ProcessingContext)}.
 * <p>
 * The {@link WorkflowEngine} loads it each time its processor claims a segment, and keeps the rows of the workflows
 * that segment owns in its live {@link WaitAssociationIndex}. The default implementation,
 * {@link EventSourcedWaitAssociations}, is sourced from the workflow events in the event store.
 *
 * @author Stefan Dragisic
 * @since 5.5.0
 */
@Internal
public interface WaitAssociations {

    /**
     * Returns every open wait in the store, of every workflow.
     *
     * @return an immutable copy of the open wait rows
     */
    Collection<WaitAssociation> waitAssociations();
}
