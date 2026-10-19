/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Represents a condition for event receipt.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface EventCondition {


    /**
     * Returns the predicate on an {@link EventMessage} and its accompanying {@link ProcessingContext} through which
     * this condition can be evaluated.
     *
     * @return the constructed predicate
     */
    BiPredicate<EventMessage, ProcessingContext> predicate();

    /**
     * Returns a qualified name of the event message.
     *
     * @return qualified name of the event
     */
    QualifiedName qualifiedName();

    /**
     * Returns canonical serialized associations carried by this condition, if any.
     *
     * @return serialized association strings
     */
    default Set<String> associations() {
        return Set.of();
    }
}
