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
package io.axoniq.workflow.runtime.api.execution.context;

import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Predicate;

/**
 * Represents a condition for event receipt.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
@FunctionalInterface
public interface EventCondition {


    /**
     * Returns the predicate on the event message.
     *
     * @return predicate.
     */
    @Nonnull
    default Predicate<EventMessage> predicate() {
        return eventMessage -> true;
    }

    /**
     * Returns a qualified name of the event message.
     *
     * @return qualified name of the event.
     */
    @Nonnull
    QualifiedName qualifiedName();
}
