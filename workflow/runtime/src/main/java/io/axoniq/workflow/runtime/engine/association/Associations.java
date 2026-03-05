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
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Helper to build associations and create predicates out of them.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class Associations implements PredicateBuilder {

    private final ValueComparisonOperatorRegistry registry;
    private final Set<AssociationValue> values;

    /**
     * Create a builder with association.
     *
     * @param name     property name.
     * @param operator operator for value comparison.
     * @param value    value.
     * @return association builder.
     */
    public static Associations associate(
            @Nonnull String name,
            @Nonnull String operator,
            @Nonnull Object value
    ) {
        var instance = new Associations(Set.of());
        return instance.and(name, operator, value);
    }

    /**
     * Creates associations parsing string representations.
     *
     * @param registry     registry to use.
     * @param associations associations to parse.
     * @return associations object.
     */
    public static Associations parse(
            @Nonnull ValueComparisonOperatorRegistry registry, String... associations) {
        return new Associations(registry, parseAssociationValues(registry, associations));
    }

    /**
     * Creates a new association builder using provided association values.
     *
     * @param values association values.
     */
    public Associations(@Nonnull Set<AssociationValue> values) {
        this(new ValueComparisonOperatorRegistry(), values);
    }

    /**
     * Creates a new association builder using provided association values and operator registry.
     *
     * @param registry operator registry.
     * @param values   association values.
     */
    public Associations(@Nonnull ValueComparisonOperatorRegistry registry, @Nonnull Set<AssociationValue> values) {
        this.values = Objects.requireNonNull(values, "The set of association values must not be null.");
        this.registry = Objects.requireNonNull(registry, "Registry must not be null");
    }


    /**
     * Creates a new builder containing all old and a new association.
     *
     * @param name     property name.
     * @param operator operator for value comparison.
     * @param value    value.
     * @return association builder.
     */
    public Associations and(@Nonnull String name, @Nonnull String operator, @Nonnull Object value) {
        var newValues = new HashSet<>(this.values);
        newValues.add(create(name, operator, value));
        return new Associations(newValues);
    }

    @Nonnull
    @Override
    public Predicate<EventMessage> build(@Nonnull Converter converter) {
        return (eventMessage) -> {
            var payload = eventMessage.payloadAs(new TypeReference<Map<String, Object>>() {
            }, converter);
            return this.values.stream().allMatch(av -> av.asPayloadPredicate().test(payload));
        };
    }

    /**
     * Creates an association using value equality for comparison.
     *
     * @param associationKey   property name.
     * @param operator         operator for value comparison.
     * @param associationValue value.
     * @return association value.
     */
    private AssociationValue create(
            @Nonnull String associationKey,
            @Nonnull String operator,
            @Nonnull Object associationValue
    ) {
        Objects.requireNonNull(associationKey, "Association key is mandatory");
        Objects.requireNonNull(operator, "Operator is mandatory");
        Objects.requireNonNull(associationValue, "Association value is mandatory");
        return new AssociationValue(associationKey, registry.get(operator), associationValue);
    }

    /**
     * Parse association values from their string representations.
     *
     * @param associations strings in form of <key><op><value>.
     * @return set of association values.
     * @throws IllegalArgumentException if strings are in the wrong format or use unsupported operators.
     */
    public static Set<AssociationValue> parseAssociationValues(@Nonnull ValueComparisonOperatorRegistry registry,
                                                               String... associations) {
        var operators = registry.getOperatorNames();
        return Arrays.stream(associations)
                     .map(conditionString -> {
                              var foundOperators = operators.stream().filter(conditionString::contains)
                                                            .toList();
                              if (foundOperators.size() == 1) {
                                  var op = foundOperators.getFirst();
                                  var split = conditionString.split(op);
                                  if (split.length != 2) {
                                      throw BadAssociationFormatException.wrongFormat(
                                              op,
                                              conditionString
                                      );
                                  }
                                  return new AssociationValue(split[0], registry.get(op), split[1]);
                              } else {
                                  throw BadAssociationFormatException.unsupportedOperator(
                                          operators,
                                          conditionString
                                  );
                              }
                          }
                     ).collect(Collectors.toSet());
    }
}
