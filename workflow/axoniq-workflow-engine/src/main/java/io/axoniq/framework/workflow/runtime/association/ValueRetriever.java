/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Retrieves association value from the event message.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface ValueRetriever {

    /**
     * Returns the qualifier used in the serialized association form.
     *
     * @return qualifier, for example {@code payload} or {@code metadata}
     */
    String qualifier();

    /**
     * Returns the source-specific path used in the serialized association form.
     *
     * @return source-specific path, such as a payload property or metadata key
     */
    String path();

    /**
     * Retrieves association value from the event message.
     *
     * @param eventMessage      event message to retrieve value from
     * @param processingContext processing context for retrieving addition resources
     * @return retrieved value
     */
    Object apply(EventMessage eventMessage, ProcessingContext processingContext);
}
