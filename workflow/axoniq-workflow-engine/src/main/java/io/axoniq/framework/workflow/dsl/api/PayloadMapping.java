/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

/**
 * Reusable payload mapping shared by primitive specs.
 *
 * @param parameterPayloadReducer reducer for step input payload.
 * @param resultPayloadReducer    reducer for step result payload.
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record PayloadMapping(
        PayloadReducer parameterPayloadReducer,
        PayloadReducer resultPayloadReducer
) {

    /**
     * Returns a copy of this mapping with the provided parameter payload reducer.
     *
     * @param parameterPayloadReducer reducer used to prepare the step input payload
     * @return copied payload mapping with updated parameter reducer
     */
    public PayloadMapping parameterPayloadReducer(PayloadReducer parameterPayloadReducer) {
        return new PayloadMapping(parameterPayloadReducer, resultPayloadReducer);
    }

    /**
     * Returns a copy of this mapping with the provided result payload reducer.
     *
     * @param resultPayloadReducer reducer used to update the workflow payload from the step result
     * @return copied payload mapping with updated result reducer
     */
    public PayloadMapping resultPayloadReducer(PayloadReducer resultPayloadReducer) {
        return new PayloadMapping(parameterPayloadReducer, resultPayloadReducer);
    }
}
