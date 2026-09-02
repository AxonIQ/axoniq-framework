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
package io.axoniq.workflow.runtime.association;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Retrieves association value from the event message.
 *
 * @author Simon Zambrovski
 * @since 0.1.0
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
