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
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * Specification for a {@code ctx.publish(stepName, event)} call.
 * <p>
 * Unlike the other step definitions this one carries no {@link PrimitiveMetadata}: the published event keeps its own
 * {@link org.axonframework.messaging.core.MessageType}, so there is no event name to customize. Only the step name is
 * needed.
 *
 * @param stepName logical step name recorded with the published event
 * @param event    event to publish
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record PublishStepDefinition(
        String stepName,
        EventMessage event
) {

    /**
     * Fails fast when a mandatory component is {@code null}; the package's null-marked contract alone is not enforced
     * at runtime, so a user ignoring it is detected here at construction.
     */
    public PublishStepDefinition {
        Objects.requireNonNull(stepName, "stepName must not be null");
        Objects.requireNonNull(event, "event must not be null");
    }

    /**
     * Returns a copy of this definition with the provided step name.
     *
     * @param stepName logical step name
     * @return copied definition
     */
    public PublishStepDefinition stepName(String stepName) {
        return new PublishStepDefinition(stepName, event);
    }

    /**
     * Returns a copy of this definition with the provided event.
     *
     * @param event event to publish
     * @return copied definition
     */
    public PublishStepDefinition event(EventMessage event) {
        return new PublishStepDefinition(stepName, event);
    }
}
