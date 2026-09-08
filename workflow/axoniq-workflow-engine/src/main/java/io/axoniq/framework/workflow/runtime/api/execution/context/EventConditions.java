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

import io.axoniq.framework.workflow.runtime.association.Associations;
import io.axoniq.framework.workflow.runtime.association.SerializedAssociation;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Helper for construction of {@link EventCondition}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class EventConditions {

    /**
     * Bi-predicate, which will never match.
     */
    static final BiPredicate<EventMessage, ProcessingContext> NEVER = (e, pc) -> false;
    /**
     * Bi-predicate, which will always match.
     */
    static final BiPredicate<EventMessage, ProcessingContext> ALWAYS = (e, pc) -> true;

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
    public static ComponentBuilder<EventCondition> fromType(Class<?> clazz) {
        return c -> new EventCondition() {

            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return ALWAYS;
            }

            @Override
            public QualifiedName qualifiedName() {
                Objects.requireNonNull(clazz, "Class must not be null");
                return c.getComponent(MessageTypeResolver.class).resolve(clazz)
                        .orElse(new MessageType(clazz))
                        .qualifiedName();
            }
        };
    }

    /**
     * Constructs an event condition with serialized associations for the qualified name represented by the given type.
     *
     * @param clazz        type of message
     * @param associations association constraints in canonical serial form
     * @return event condition builder
     */
    public static ComponentBuilder<EventCondition> fromType(
            Class<?> clazz,
            Associations associations
    ) {
        Objects.requireNonNull(clazz, "Class must not be null");
        Objects.requireNonNull(associations, "Associations must not be null");
        return c -> serializedAssociationCondition(
                c.getComponent(MessageTypeResolver.class).resolve(clazz)
                 .orElse(new MessageType(clazz))
                 .qualifiedName(),
                associations
        );
    }

    /**
     * Constructs an event condition without **message predicate** from the qualified name which matches all messages
     * with the given qualified name.
     *
     * @param qualifiedName qualified name.
     * @return event condition.
     */
    public static EventCondition fromQualifiedName(QualifiedName qualifiedName) {
        return new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return ALWAYS;
            }

            @Override
            public QualifiedName qualifiedName() {
                return Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
            }
        };
    }

    /**
     * Constructs an event condition from the qualified name and serialized associations.
     *
     * @param qualifiedName qualified name
     * @param associations  serialized association constraints
     * @return event condition
     */
    public static EventCondition fromQualifiedName(
            QualifiedName qualifiedName,
            Associations associations
    ) {
        Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
        Objects.requireNonNull(associations, "Associations must not be null");
        return serializedAssociationCondition(qualifiedName, associations);
    }

    /**
     * Returns event condition which can't be fulfilled ever.
     *
     * @return a never-condition.
     */
    public static EventCondition never() {
        return never(new QualifiedName(Void.class));
    }

    /**
     * Creates a condition for an event with the given qualified name, which will never match. (This one is for
     * testing).
     *
     * @param qualifiedName event qualified name.
     * @return event condition.
     */
    public static EventCondition never(QualifiedName qualifiedName) {
        Objects.requireNonNull(qualifiedName, "Qualified name must not be null");
        return new EventCondition() {
            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                return NEVER;
            }

            @Override
            public QualifiedName qualifiedName() {
                return qualifiedName;
            }
        };
    }

    private static EventCondition serializedAssociationCondition(
            QualifiedName qualifiedName,
            Associations associations
    ) {
        return new EventCondition() {

            private volatile BiPredicate<EventMessage, ProcessingContext> predicate;

            @Override
            public BiPredicate<EventMessage, ProcessingContext> predicate() {
                var current = predicate;
                if (current == null) {
                    current = associationPredicate(associations);
                    predicate = current;
                }
                return current;
            }

            @Override
            public QualifiedName qualifiedName() {
                return qualifiedName;
            }

            @Override
            public Set<String> associations() {
                return associations.criteria();
            }
        };
    }

    private static BiPredicate<EventMessage, ProcessingContext> associationPredicate(
            Associations associations
    ) {
        var registry = associations.registry();
        var predicates = associations.criteria().stream()
                                     .map(serialized -> SerializedAssociation.parse(registry, serialized))
                                     .map(serialized -> serialized.asEventMessagePredicate(registry))
                                     .toList();
        return (eventMessage, pc) -> predicates.stream().allMatch(predicate -> predicate.test(eventMessage, pc));
    }
}
