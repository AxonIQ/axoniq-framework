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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.workflow.runtime.association;

import java.util.function.BiFunction;

/**
 * Association value comparison operator.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public interface ValueComparisonOperator extends BiFunction<Object, Object, Boolean> {

    /**
     * Returns the name of the operator.
     *
     * @return name of the operator.
     */
    String name();
}
