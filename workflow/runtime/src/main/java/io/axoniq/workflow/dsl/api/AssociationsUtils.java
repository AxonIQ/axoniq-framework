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
package io.axoniq.workflow.dsl.api;

import io.axoniq.workflow.runtime.association.AssociationValue;
import io.axoniq.workflow.runtime.association.BadAssociationFormatException;
import io.axoniq.workflow.runtime.association.EventMessageProcessingPredicateBuilder;
import io.axoniq.workflow.runtime.association.PayloadPropertyValueRetriever;
import io.axoniq.workflow.runtime.association.ValueComparisonOperatorRegistry;
import io.axoniq.workflow.runtime.association.ValueRetriever;
import jakarta.annotation.Nonnull;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Helper to build associations and create predicates out of them.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class AssociationsUtils implements EventMessageProcessingPredicateBuilder {

    private final ValueComparisonOperatorRegistry registry;
    private final Set<AssociationValue> values;

    /**
     * Create a builder with association.
     *
     * @param valueRetriever event value retriever.
     * @param operator       operator for value comparison.
     * @param value          value.
     * @return association builder.
     */
    public static AssociationsUtils associate(
            @Nonnull ValueRetriever valueRetriever,
            @Nonnull String operator,
            @Nonnull Object value
    ) {
        var instance = new AssociationsUtils(Set.of());
        return instance.and(valueRetriever, operator, value);
    }

    /**
     * Create a builder with association.
     *
     * @param retriever value retriever, see @link {@link PayloadPropertyValueRetriever#payloadProperty(String)} for
     *                  example.
     * @param matcher   variable matcher, see @link {@link VariableMatcher#equals(Object)}} for example.
     * @return fluent builder association utils.
     */
    public static AssociationsUtils associate(@Nonnull ValueRetriever retriever, @Nonnull VariableMatcher matcher) {
        return AssociationsUtils.associate(
                retriever,
                matcher.operator,
                matcher.value
        );
    }


    public record VariableMatcher(
            @Nonnull String operator,
            @Nonnull Object value) {

    }

    /**
     * Creates associations parsing string representations.
     *
     * @param registry     registry to use.
     * @param associations associations to parse.
     * @return associations object.
     */
    public static AssociationsUtils parse(
            @Nonnull ValueComparisonOperatorRegistry registry, String... associations) {
        return new AssociationsUtils(registry, parseAssociationValues(registry, associations));
    }

    /**
     * Creates a new association builder using provided association values.
     *
     * @param values association values.
     */
    public AssociationsUtils(@Nonnull Set<AssociationValue> values) {
        this(new ValueComparisonOperatorRegistry(), values);
    }

    /**
     * Creates a new association builder using provided association values and operator registry.
     *
     * @param registry operator registry.
     * @param values   association values.
     */
    public AssociationsUtils(@Nonnull ValueComparisonOperatorRegistry registry, @Nonnull Set<AssociationValue> values) {
        this.values = Objects.requireNonNull(values, "The set of association values must not be null.");
        this.registry = Objects.requireNonNull(registry, "Registry must not be null");
    }


    /**
     * Creates a new builder containing all old and a new association.
     *
     * @param valueRetriever value retriever.
     * @param operator       operator for value comparison.
     * @param value          value.
     * @return association builder.
     */
    public AssociationsUtils and(@Nonnull ValueRetriever valueRetriever,
                                 @Nonnull String operator,
                                 @Nonnull Object value) {
        var newValues = new HashSet<>(this.values);
        newValues.add(create(valueRetriever, operator, value));
        return new AssociationsUtils(newValues);
    }

    @Nonnull
    @Override
    public BiPredicate<EventMessage, ProcessingContext> build() {
        return (eventMessage, pc) -> this.values.stream()
                                                .allMatch(av ->
                                                                  av.asEventMessagePredicate()
                                                                    .test(eventMessage, pc));
    }

    /**
     * Creates an association using operator for comparison.
     *
     * @param valueRetriever   retriever of value from the event message.
     * @param operator         operator for value comparison.
     * @param associationValue value.
     * @return association value.
     */
    private AssociationValue create(
            @Nonnull ValueRetriever valueRetriever,
            @Nonnull String operator,
            @Nonnull Object associationValue
    ) {
        Objects.requireNonNull(valueRetriever, "Value retriever is mandatory");
        Objects.requireNonNull(operator, "Operator is mandatory");
        Objects.requireNonNull(associationValue, "Association value is mandatory");
        return new AssociationValue(
                valueRetriever,
                registry.get(operator),
                () -> associationValue
        );
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
                                  if (split.length != 2 || split[0].isBlank() || split[1].isBlank()) {
                                      throw BadAssociationFormatException.wrongFormat(
                                              op,
                                              conditionString
                                      );
                                  }
                                  return new AssociationValue(
                                          new PayloadPropertyValueRetriever(split[0]),
                                          registry.get(op),
                                          () -> split[1]);
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
