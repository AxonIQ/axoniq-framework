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
package io.axoniq.workflow.runtime.api.execution.context;

import io.axoniq.workflow.runtime.api.payload.PayloadReducer;
import jakarta.annotation.Nonnull;

/**
 * Reusable payload mapping shared by primitive specs.
 *
 * @param parameterPayloadReducer reducer for step input payload.
 * @param resultPayloadReducer    reducer for step result payload.
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public record PayloadMapping(
        @Nonnull PayloadReducer parameterPayloadReducer,
        @Nonnull PayloadReducer resultPayloadReducer
) {

    /**
     * Returns a copy of this mapping with the provided parameter payload reducer.
     *
     * @param parameterPayloadReducer reducer used to prepare the step input payload
     * @return copied payload mapping with updated parameter reducer
     */
    public PayloadMapping parameterPayloadReducer(@Nonnull PayloadReducer parameterPayloadReducer) {
        return new PayloadMapping(parameterPayloadReducer, resultPayloadReducer);
    }

    /**
     * Returns a copy of this mapping with the provided result payload reducer.
     *
     * @param resultPayloadReducer reducer used to update the workflow payload from the step result
     * @return copied payload mapping with updated result reducer
     */
    public PayloadMapping resultPayloadReducer(@Nonnull PayloadReducer resultPayloadReducer) {
        return new PayloadMapping(parameterPayloadReducer, resultPayloadReducer);
    }
}
