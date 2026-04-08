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

package org.axonframework.messaging.eventhandling.processing.streaming.pooled;

import java.util.function.Function;


/**
 * Functional interface returning the maximum amount of segments a {@link Coordinator} may claim, based on the given
 * {@code processorName}.
 *
 * @author Manish
 * @since 4.10.0
 */
public interface MaxSegmentProvider extends Function<String, Integer> {

    /**
     * Returns the maximum amount of segments to claim for the given {@code processorName}.
     *
     * @param processorName The name of the processor for which to provide the maximum amount of segments it can claim.
     * @return The maximum number of segments that can be claimed for the given {@code processorName}.
     */
    int getMaxSegments(String processorName);

    /**
     * Returns the maximum amount of segments to claim for the given {@code processorName}.
     *
     * @param processorName The name of a processor for which to provide the maximum amount of segments it can claim.
     * @return The maximum number of segments that can be claimed for the given {@code processorName}.
     */
    @Override
    default Integer apply(String processorName) {
        return getMaxSegments(processorName);
    }

    /**
     * A {@link MaxSegmentProvider} that always returns {@link Short#MAX_VALUE}.
     *
     * @return A {@link MaxSegmentProvider} that always returns {@link Short#MAX_VALUE}.
     */
    static MaxSegmentProvider maxShort() {
        return processorName -> Short.MAX_VALUE;
    }
}
