/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

/**
 * Executes comparison based on equality of objects.
 * <p>
 * This is a simple equals operator allowing to specify association string of type: "key=value". The comparison will be
 * converted into a predicate on the event message accessing a payload attribute and comparing it with the given value
 * by applying the {@link Object#equals(Object)} method.
 * </p>
 *
 * @author Simon Zambrovski
 * @since 5.4.0
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
