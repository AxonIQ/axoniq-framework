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
package io.axoniq.workflow.runtime.association;

import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Canonical association DSL value object.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record Associations(
        @Nonnull ValueComparisonOperatorRegistry registry,
        @Nonnull Set<String> criteria
) {

    /**
     * Creates new associations.
     *
     * @param registry operator registry to use
     * @param criteria association criteria
     */
    public Associations {
        registry = Objects.requireNonNull(registry, "Registry must not be null");
        criteria = Objects.requireNonNull(criteria, "The set of associations criteria must not be null.");
    }

    /**
     * Create new associations providing the first one.
     *
     * @param retriever event value retriever.
     * @param operator  operator for value comparison.
     * @param value     value.
     * @return association builder.
     */
    public static Associations associate(
            @Nonnull ValueRetriever retriever,
            @Nonnull String operator,
            @Nonnull Object value
    ) {
        return new Associations(
                new ValueComparisonOperatorRegistry(),
                Set.of(SerializedAssociation.from(retriever, operator, value).serialize())
        );
    }

    /**
     * Create associations providing the first one.
     *
     * @param retriever value retriever, see {@link PayloadPropertyValueRetriever#payloadProperty(String)} for example.
     * @param matcher   variable matcher, see {@link VariableMatcher} for example.
     * @return fluent builder association utils.
     */
    public static Associations associate(@Nonnull ValueRetriever retriever,
                                         @Nonnull VariableMatcher matcher) {
        return Associations.associate(
                retriever,
                matcher.operator,
                matcher.value
        );
    }


    /**
     * Creates new associations parsing string representations.
     *
     * @param registry     registry to use
     * @param associations associations to parse
     * @return associations instance
     */
    public static Associations parse(
            @Nonnull ValueComparisonOperatorRegistry registry, String... associations) {
        return new Associations(registry, normalizeAssociations(registry, associations));
    }

    /**
     * Add additional associations to existing ones.
     *
     * @param retriever value retriever
     * @param operator  operator for value comparison
     * @param value     value
     * @return new associations containing old associations and new one
     */
    public Associations and(@Nonnull ValueRetriever retriever,
                            @Nonnull String operator,
                            @Nonnull Object value) {
        var newValues = new HashSet<>(this.criteria);
        newValues.add(SerializedAssociation.from(retriever, operator, value).serialize());
        return new Associations(this.registry, newValues);
    }

    /**
     * Add additional association to existing ones.
     *
     * @param retriever       value retriever
     * @param variableMatcher value matcher
     * @return new associations containing old associations and new one
     */
    public Associations and(@Nonnull ValueRetriever retriever,
                            @Nonnull VariableMatcher variableMatcher) {
        return and(retriever, variableMatcher.operator, variableMatcher.value);
    }

    /**
     * Encapsulates the value matcher containing of operator and the value.
     *
     * @param operator String representation of the operator
     * @param value    value to match
     * @author Simon Zambrovski
     * @since 1.0.0
     */
    public record VariableMatcher(
            @Nonnull String operator,
            @Nonnull Object value) {

    }


    @Nonnull
    private static Set<String> normalizeAssociations(@Nonnull ValueComparisonOperatorRegistry registry,
                                                     String... associations) {
        return Arrays.stream(associations)
                     .map(conditionString -> SerializedAssociation.parse(registry, conditionString).serialize())
                     .collect(Collectors.toCollection(HashSet::new));
    }
}
