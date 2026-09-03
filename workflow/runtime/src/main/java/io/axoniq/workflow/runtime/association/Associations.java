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


import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Immutable value object representing one or more association constraints used to correlate incoming events with a
 * waiting workflow step.
 * <p>
 * Each entry in {@link #criteria()} is stored in the canonical serialized association DSL form
 * {@code <source>:<path><operator><value>}, for example {@code payload:orderId=123}. At runtime the workflow engine
 * evaluates these criteria against candidate events to determine whether an event belongs to the waiting step.
 * <p>
 * There are two primary construction styles:
 * <ol>
 *   <li>Programmatic construction using {@link #associate(ValueRetriever, Matcher)} or
 *   {@link #associate(ValueRetriever, String, Object)}. This is the normal author-facing path used by workflow code.
 *   Start with one association and optionally extend it with {@link #and(ValueRetriever, Matcher)} or
 *   {@link #and(ValueRetriever, String, Object)}.</li>
 *   <li>Parsing canonical string representations using
 *   {@link #parse(ValueComparisonOperatorRegistry, String...)}, which is useful for configuration-driven or
 *   persisted association definitions.</li>
 * </ol>
 * <p>
 * Typical usage from workflow code looks like this:
 * <pre>{@code
 * var associations = Associations.associate(
 *         payloadProperty("orderId"),
 *         Matcher("=", workflowOrderId)
 * ).and(
 *         payloadProperty("tenantId"),
 *         Matcher("=", tenantId)
 * );
 * }</pre>
 * The DSL layer usually wraps this with friendlier helpers such as
 * {@code associate(payloadProperty("orderId"), equalsTo(...))}, but those helpers still produce this same
 * {@code Associations} value object underneath.
 * <p>
 * This type is immutable. Methods such as {@link #and(ValueRetriever, Matcher)} return a new
 * {@code Associations} instance instead of mutating the existing one, which makes it safe to reuse base association
 * sets across workflow definitions.
 *
 * @author Simon Zambrovski
 * @since 0.2.0
 */
public record Associations(
        ValueComparisonOperatorRegistry registry,
        Set<String> criteria
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
     * @param retriever value retriever, see {@link PayloadPropertyValueRetriever#payloadProperty(String)} for example.
     * @param operator  operator for value comparison
     * @param value     right side of comparison
     * @return an association upon which can be build further, for fluent interface.
     */
    public static Associations associate(
            ValueRetriever retriever,
            String operator,
            Object value
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
     * @param matcher   variable matcher, see {@link Matcher} for example.
     * @return an association upon which can be build further, for fluent interface
     */
    public static Associations associate(ValueRetriever retriever,
                                         Matcher matcher) {
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
            ValueComparisonOperatorRegistry registry, String... associations) {
        return new Associations(registry, normalizeAssociations(registry, associations));
    }

    /**
     * Add additional associations to existing ones.
     *
     * @param retriever value retriever for the left side of the comparison, see
     *                  {@link PayloadPropertyValueRetriever#payloadProperty(String)} for example.
     * @param operator  operator for value comparison
     * @param value     right side of comparison
     * @return new associations containing old associations and new one
     */
    public Associations and(ValueRetriever retriever,
                            String operator,
                            Object value) {
        var newValues = new HashSet<>(this.criteria);
        newValues.add(SerializedAssociation.from(retriever, operator, value).serialize());
        return new Associations(this.registry, newValues);
    }

    /**
     * Add additional association to existing ones.
     *
     * @param retriever       value retriever for the left side of the comparison, see
     *                        {@link PayloadPropertyValueRetriever#payloadProperty(String)} for example.
     * @param matcher value matcher including operator and value for comparison
     * @return new associations containing old associations and new one
     */
    public Associations and(ValueRetriever retriever,
                            Matcher matcher) {
        return and(retriever, matcher.operator, matcher.value);
    }

    /**
     * Encapsulates the value matcher containing of operator and the value.
     *
     * @param operator String representation of the operator
     * @param value    value to match
     * @author Simon Zambrovski
     * @since 0.2.0
     */
    public record Matcher(
            String operator,
            Object value) {

    }


    private static Set<String> normalizeAssociations(ValueComparisonOperatorRegistry registry,
                                                     String... associations) {
        return Arrays.stream(associations)
                     .map(conditionString -> SerializedAssociation.parse(registry, conditionString).serialize())
                     .collect(Collectors.toCollection(HashSet::new));
    }
}
