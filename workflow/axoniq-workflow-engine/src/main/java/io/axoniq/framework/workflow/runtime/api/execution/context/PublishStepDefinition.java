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
package io.axoniq.framework.workflow.runtime.api.execution.context;

import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;

/**
 * Specification for a {@code ctx.publish(stepName, event)} call.
 * <p>
 * The {@link PrimitiveMetadata#eventNameCustomizer()} is not applied: the published event keeps its own
 * {@link org.axonframework.messaging.core.MessageType}.
 *
 * @param primitiveMetadata metadata of the primitive; only {@link PrimitiveMetadata#stepName()} is used
 * @param event             event to publish
 * @author Stefan Dragisic
 * @since 5.4.0
 */
public record PublishStepDefinition(
        PrimitiveMetadata primitiveMetadata,
        EventMessage event
) {

    /**
     * Fails fast when a mandatory component is {@code null}; the package's null-marked contract alone is not enforced
     * at runtime, so a user ignoring it is detected here at construction.
     */
    public PublishStepDefinition {
        Objects.requireNonNull(primitiveMetadata, "primitiveMetadata must not be null");
        Objects.requireNonNull(event, "event must not be null");
    }

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata to use
     * @return copied definition
     */
    public PublishStepDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new PublishStepDefinition(primitiveMetadata, event);
    }

    /**
     * Returns a copy of this definition with the provided step name.
     *
     * @param stepName logical step name
     * @return copied definition
     */
    public PublishStepDefinition stepName(String stepName) {
        return primitiveMetadata(primitiveMetadata.stepName(stepName));
    }

    /**
     * Returns a copy of this definition with the provided event.
     *
     * @param event event to publish
     * @return copied definition
     */
    public PublishStepDefinition event(EventMessage event) {
        return new PublishStepDefinition(primitiveMetadata, event);
    }
}
