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
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Helper for construction of {@link EventCondition}.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class EventConditions {

    private EventConditions() {
        // hide instantiation
    }

    /**
     * Constructs an event condition without **message predicate** for the qualified name represented by the given type.
     * The resulting event condition will always trigger if the qualified name is matching.
     *
     * @param clazz type of message (message type resolve will use this type to deduce the message type).
     * @return condition component builder.
     */
    public static ComponentBuilder<EventCondition> fromType(@Nonnull Class<?> clazz) {
        Objects.requireNonNull(clazz, "Class must not be null");
        return (c) -> (EventCondition) () ->
                c.getComponent(MessageTypeResolver.class).resolve(clazz)
                 .orElse(new MessageType(clazz))
                 .qualifiedName();
    }

    /**
     * Constructs an event condition with a message predicate for the qualified name represented by the given type.
     *
     * @param clazz                 type of message (message type resolve will use this type to deduce the message
     *                              type).
     * @param eventMessagePredicate predicate for the message.
     * @return event condition builder.
     */
    public static ComponentBuilder<EventCondition> fromType(@Nonnull Class<?> clazz,
                                                            @Nonnull ComponentBuilder<Predicate<EventMessage>> eventMessagePredicate) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(eventMessagePredicate, "Predicate must not be null");
        return (c) -> new EventCondition() {

            @Nonnull
            @Override
            public Predicate<EventMessage> predicate() {
                return eventMessagePredicate.build(c);
            }

            @Nonnull
            @Override
            public QualifiedName qualifiedName() {
                return c.getComponent(MessageTypeResolver.class).resolve(clazz)
                        .orElse(new MessageType(clazz))
                        .qualifiedName();
            }
        };
    }

    /**
     * Constructs an event condition without **message predicate** from the qualified name which matches all messages
     * with the given qualified name.
     *
     * @param qualifiedName qualified name.
     * @return event condition.
     */
    public static EventCondition fromQualifiedName(@Nonnull QualifiedName qualifiedName) {
        return () -> Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
    }

    /**
     * Constructs an event condition from the qualified name and a message predicate.
     *
     * @param qualifiedName qualified name.
     * @param predicate     message predicate.
     * @return event condition.
     */
    public static EventCondition fromQualifiedName(@Nonnull QualifiedName qualifiedName,
                                                   @Nonnull Predicate<EventMessage> predicate) {
        Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
        Objects.requireNonNull(predicate, "Predicate name must not be null");

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

    /**
     * Creates a condition for an event with the given qualified name, which will never match. (This one is for testing).
     *
     * @param qualifiedName event qualified name.
     * @return event condition.
     */
    public static EventCondition never(@Nonnull QualifiedName qualifiedName) {
        Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
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
