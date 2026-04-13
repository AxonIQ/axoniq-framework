/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Predicate;

/**
 * Builds message predicates.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface PredicateBuilder extends ComponentBuilder<Predicate<EventMessage>> {

    /**
     * Builds predicate for a message using a specified converter.
     *
     * @param converter converter to use.
     * @return predicate on a message.
     */
    @Nonnull
    Predicate<EventMessage> build(@Nonnull Converter converter);

    /**
     * Build a predicate for a message.
     *
     * @param processingContext message processing context.
     * @return event message predicate.
     */
    @Nonnull
    default Predicate<EventMessage> build(@Nonnull ProcessingContext processingContext) {
        return build(processingContext.component(Converter.class));
    }

    /**
     * Build a predicate for a message.
     *
     * @param configuration configuration.
     * @return event message predicate.
     */
    @Nonnull
    default Predicate<EventMessage> build(@Nonnull Configuration configuration) {
        return build(configuration.getComponent(Converter.class));
    }
}
