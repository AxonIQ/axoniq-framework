/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;

import java.util.function.BiFunction;

/**
 * Association value comparison operator.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public interface ValueComparisonOperator extends BiFunction<Object, Object, Boolean> {

    /**
     * Returns the name of the operator.
     *
     * @return name of the operator.
     */
    String name();
}
