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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.execution;

import io.axoniq.workflow.runtime.api.EventCondition;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.function.Predicate;

/**
 * Helper for construction of event conditions.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class EventConditions {

    private EventConditions() {
        // hide instantiation
    }

    /**
     * Constructs an event condition without message predicate for the qualified name represented by the given type.
     *
     * @param clazz type of message (message type resolve will use this type to deduce the message type).
     * @return condition.
     */
    public static ComponentBuilder<EventCondition> fromType(Class<?> clazz) {
        return (c) -> (EventCondition) () ->
                c.getComponent(MessageTypeResolver.class).resolve(clazz)
                 .orElse(new MessageType(clazz))
                 .qualifiedName();
    }

    /**
     * Constructs an event condition from qualified name.
     *
     * @param qualifiedName qualified name.
     * @return event condition.
     */
    public static EventCondition fromQualifiedName(@Nonnull QualifiedName qualifiedName) {
        return () -> qualifiedName;
    }

    /**
     * Constructs an event condition from qualified name and a message predicate.
     *
     * @param qualifiedName qualified name.
     * @param predicate     message predicate.
     * @return event condition.
     */
    public static EventCondition fromQualifiedName(@Nonnull QualifiedName qualifiedName,
                                                   @Nonnull Predicate<EventMessage> predicate) {
        return new EventCondition() {

            @Nonnull
            @Override
            public QualifiedName qualifiedName() {
                return qualifiedName;
            }

            @Nonnull
            @Override
            public Predicate<EventMessage> predicate() {
                return predicate;
            }
        };
    }

    /**
     * Returns event condition which can't be fulfilled ever.
     *
     * @return a never-condition.
     */
    public static EventCondition never() {
        return () -> new QualifiedName(Void.class);
    }

    public static EventCondition never(@Nonnull QualifiedName qualifiedName) {
        return new EventCondition() {
            @Nonnull
            @Override
            public QualifiedName qualifiedName() {
                return qualifiedName;
            }

            @Nonnull
            @Override
            public Predicate<EventMessage> predicate() {
                return e -> false;
            }
        };
    }
}
