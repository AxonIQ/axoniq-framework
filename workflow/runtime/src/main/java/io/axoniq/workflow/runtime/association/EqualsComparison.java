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

/**
 * Executes comparison based on equality of objects.
 * <p>
 * This is a simple equals operator allowing to specify association string of type: "key=value". The comparison will be
 * converted into a predicate on the event message accessing a payload attribute and comparing it with the given value
 * by applying the {@link Object#equals(Object)} method.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class EqualsComparison implements ValueComparisonOperator {

    /**
     * String representation of the operator.
     */
    public static final String OPERATOR = "=";

    @Override
    public Boolean apply(Object o, Object o2) {
        if (o == null) {
            return o2 == null;
        }
        return o.equals(o2);
    }

    @Override
    public String name() {
        return OPERATOR;
    }
}
