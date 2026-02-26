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
package io.axoniq.workflow.runtime.engine.association;

import jakarta.annotation.Nonnull;
import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Helper to build associations and create predicates out of them.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class AssociationsPredicateBuilder {

    private static final ValueComparisonOperatorRegistry registry = new ValueComparisonOperatorRegistry();
    private final Set<AssociationValue> values;

    /**
     * Creates an association using value equality for comparison.
     *
     * @param associationKey   property name.
     * @param operator         operator for value comparison.
     * @param associationValue value.
     * @return association value.
     */
    static AssociationValue create(@Nonnull String associationKey,
                                   @Nonnull String operator,
                                   @Nonnull Object associationValue) {
        Objects.requireNonNull(associationKey, "Association key is mandatory");
        Objects.requireNonNull(operator, "Operator is mandatory");
        Objects.requireNonNull(associationValue, "Association value is mandatory");
        return new AssociationValue(associationKey, registry.get(operator), associationValue);
    }

    /**
     * Create a builder with association.
     *
     * @param name     property name.
     * @param operator operator for value comparison.
     * @param value    value.
     * @return association builder.
     */
    public static AssociationsPredicateBuilder associate(@Nonnull String name, @Nonnull String operator,
                                                         @Nonnull Object value) {
        return new AssociationsPredicateBuilder(Set.of(
                create(name, operator, value)
        ));
    }

    /**
     * Creates a new association builder using provided association values.
     *
     * @param values association values.
     */
    public AssociationsPredicateBuilder(@Nonnull Set<AssociationValue> values) {
        this.values = Objects.requireNonNull(values, "The set of association values must not be null.");
    }


    /**
     * Creates a new builder containing all old and a new association.
     *
     * @param name     property name.
     * @param operator operator for value comparison.
     * @param value    value.
     * @return association builder.
     */
    public AssociationsPredicateBuilder and(@Nonnull String name, @Nonnull String operator, @Nonnull Object value) {
        var newValues = new HashSet<>(this.values);
        newValues.add(create(name, operator, value));
        return new AssociationsPredicateBuilder(newValues);
    }

    /**
     * Build a predicate for a message.
     *
     * @param converter converter to convert message payload.
     * @return event message predicate.
     */
    public Predicate<EventMessage> build(@Nonnull Converter converter) {
        return (eventMessage) -> {
            var payload = eventMessage.payloadAs(new TypeReference<Map<String, Object>>() {
            }, converter);
            return this.values.stream().allMatch(av -> av.asPayloadPredicate().test(payload));
        };
    }

    /**
     * Build a predicate for a message.
     *
     * @param processingContext message processing context.
     * @return event message predicate.
     */
    public Predicate<EventMessage> build(@Nonnull ProcessingContext processingContext) {
        return build(processingContext.component(Converter.class));
    }
}
