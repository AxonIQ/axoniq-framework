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
package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Represents a condition for event receipt.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface EventCondition {


    /**
     * Returns the predicate on an {@link EventMessage} and its accompanying {@link ProcessingContext} through which
     * this condition can be evaluated.
     *
     * @return the constructed predicate
     */
    @Nonnull
    BiPredicate<EventMessage, ProcessingContext> predicate();

    /**
     * Returns a qualified name of the event message.
     *
     * @return qualified name of the event
     */
    @Nonnull
    QualifiedName qualifiedName();

    /**
     * Returns canonical serialized associations carried by this condition, if any.
     *
     * @return serialized association strings
     */
    @Nonnull
    default Set<String> associations() {
        return Set.of();
    }
}
