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

import java.util.Map;
import java.util.function.Predicate;

/**
 * Describes association between an event and the workflow instance.
 *
 * @param associationKey   represents the property name of the event (payload).
 * @param operator         comparison operator.
 * @param associationValue value
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record AssociationValue(
        @Nonnull String associationKey,
        @Nonnull ValueComparisonOperator operator,
        @Nonnull Object associationValue
) {

    /**
     * Applies the comparison operator to the given {@code actualValue} and the {@link #associationValue()}.
     *
     * @param actualValue value to compare
     * @return {@code true} if the comparison matches, {@code false} otherwise
     */
    public boolean apply(Object actualValue) {
        return operator.apply(actualValue, associationValue);
    }

    /**
     * Returns a predicate on a payload map.
     *
     * @return predicate to be applied on payload using the association key as a key in the map and comparing the
     * payload value using the specified operator.
     */
    public Predicate<Map<String, Object>> asPayloadPredicate() {
        return map -> map.containsKey(associationKey) && apply(map.get(associationKey));
    }
}
