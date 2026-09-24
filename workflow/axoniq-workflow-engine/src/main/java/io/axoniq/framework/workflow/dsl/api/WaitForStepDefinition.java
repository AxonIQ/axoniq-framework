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
package io.axoniq.framework.workflow.dsl.api;

import java.time.Duration;

/**
 * Specification for waiting for an event within a workflow.
 *
 * @param primitiveMetadata metadata of the primitive
 * @param eventCondition    condition for event receipt
 * @param payloadMapping    mapping of payloads
 * @param timing            timing configuration
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record WaitForStepDefinition(
        PrimitiveMetadata primitiveMetadata,
        EventCondition eventCondition,
        PayloadMapping payloadMapping,
        Timing timing
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata of the primitive
     * @return copied step definition with updated metadata
     */
    public WaitForStepDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new WaitForStepDefinition(primitiveMetadata, eventCondition, payloadMapping, timing);
    }

    /**
     * Returns a copy of this definition with the provided step name.
     *
     * @param stepName logical step name
     * @return copied step definition with updated step name
     */
    public WaitForStepDefinition stepName(String stepName) {
        return primitiveMetadata(primitiveMetadata.stepName(stepName));
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied step definition with updated event naming
     */
    public WaitForStepDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided event condition.
     *
     * @param eventCondition condition for event receipt
     * @return copied step definition with updated event condition
     */
    public WaitForStepDefinition eventCondition(EventCondition eventCondition) {
        return new WaitForStepDefinition(primitiveMetadata, eventCondition, payloadMapping, timing);
    }

    /**
     * Returns a copy of this definition with the provided payload mapping.
     *
     * @param payloadMapping mapping of payloads
     * @return copied step definition with updated payload mapping
     */
    public WaitForStepDefinition payloadMapping(PayloadMapping payloadMapping) {
        return new WaitForStepDefinition(primitiveMetadata, eventCondition, payloadMapping, timing);
    }

    /**
     * Returns a copy of this definition with the provided result payload reducer.
     *
     * @param resultPayloadReducer reducer that updates the workflow payload from the event result
     * @return copied step definition with updated result payload reducer
     */
    public WaitForStepDefinition resultPayloadReducer(PayloadReducer resultPayloadReducer) {
        return payloadMapping(payloadMapping.resultPayloadReducer(resultPayloadReducer));
    }

    /**
     * Returns a copy of this definition with the provided timing configuration.
     *
     * @param timing timing configuration
     * @return copied step definition with updated timing
     */
    public WaitForStepDefinition timing(Timing timing) {
        return new WaitForStepDefinition(primitiveMetadata, eventCondition, payloadMapping, timing);
    }

    /**
     * Returns a copy of this definition with the provided timeout.
     *
     * @param timeout timeout duration
     * @return copied step definition with updated timeout
     */
    public WaitForStepDefinition timeout(Duration timeout) {
        return timing(timing.timeout(timeout));
    }

    /**
     * Returns a copy of this definition with the provided timeout in seconds.
     *
     * @param timeoutSeconds timeout in seconds
     * @return copied step definition with updated timeout
     */
    public WaitForStepDefinition timeout(long timeoutSeconds) {
        return timing(timing.timeout(timeoutSeconds));
    }
}
