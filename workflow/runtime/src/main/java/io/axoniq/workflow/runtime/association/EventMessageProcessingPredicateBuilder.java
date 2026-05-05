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

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.BiPredicate;

/**
 * Builds event message processing bi-predicate.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface EventMessageProcessingPredicateBuilder
        extends ComponentBuilder<BiPredicate<EventMessage, ProcessingContext>> {

    /**
     * Build a predicate for a message.
     *
     * @return event message predicate
     */
    @Nonnull
    BiPredicate<EventMessage, ProcessingContext> build();

    /**
     * Build a predicate for a message.
     *
     * @param configuration configuration
     * @return event message predicate
     */
    @Nonnull
    default BiPredicate<EventMessage, ProcessingContext> build(@Nonnull Configuration configuration) {
        return build();
    }
}
