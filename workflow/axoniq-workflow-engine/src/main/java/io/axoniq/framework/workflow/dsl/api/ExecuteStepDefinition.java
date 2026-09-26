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

import io.axoniq.framework.workflow.dsl.api.retry.RetryPolicy;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.Map;

/**
 * Specification for executing an action within a workflow.
 *
 * @param primitiveMetadata metadata of the primitive
 * @param inputPayload      local input payload for the actions
 * @param action            action to execute
 * @param payloadMapping    mapping of payloads
 * @param timing            timing configuration
 * @param retryPolicy       retry policy for the action
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public record ExecuteStepDefinition(
        PrimitiveMetadata primitiveMetadata,
        Map<String, @Nullable Object> inputPayload,
        PayloadProcessor action,
        PayloadMapping payloadMapping,
        Timing timing,
        RetryPolicy retryPolicy
) {

    /**
     * Returns a copy of this definition with the provided primitive metadata.
     *
     * @param primitiveMetadata metadata of the primitive
     * @return copied step definition with updated metadata
     */
    public ExecuteStepDefinition primitiveMetadata(PrimitiveMetadata primitiveMetadata) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }

    /**
     * Returns a copy of this definition with the provided step name.
     *
     * @param stepName logical step name
     * @return copied step definition with updated step name
     */
    public ExecuteStepDefinition stepName(String stepName) {
        return primitiveMetadata(primitiveMetadata.stepName(stepName));
    }

    /**
     * Returns a copy of this definition with the provided event name customizer.
     *
     * @param eventNameCustomizer customizer for published event names
     * @return copied step definition with updated event naming
     */
    public ExecuteStepDefinition eventNameCustomizer(EventNameCustomizer eventNameCustomizer) {
        return primitiveMetadata(primitiveMetadata.eventNameCustomizer(eventNameCustomizer));
    }

    /**
     * Returns a copy of this definition with the provided step-local input payload.
     *
     * @param inputPayload local input payload for the action
     * @return copied step definition with updated input payload
     */
    public ExecuteStepDefinition inputPayload(Map<String, @Nullable Object> inputPayload) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }

    /**
     * Returns a copy of this definition with the provided action.
     *
     * @param action action to execute
     * @return copied step definition with updated action
     */
    public ExecuteStepDefinition action(PayloadProcessor action) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }

    /**
     * Returns a copy of this definition with the provided payload mapping.
     *
     * @param payloadMapping mapping of payloads
     * @return copied step definition with updated payload mapping
     */
    public ExecuteStepDefinition payloadMapping(PayloadMapping payloadMapping) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }

    /**
     * Returns a copy of this definition with the provided parameter payload reducer.
     *
     * @param parameterPayloadReducer reducer that prepares the action input
     * @return copied step definition with updated parameter payload reducer
     */
    public ExecuteStepDefinition parameterPayloadReducer(PayloadReducer parameterPayloadReducer) {
        return payloadMapping(payloadMapping.parameterPayloadReducer(parameterPayloadReducer));
    }

    /**
     * Returns a copy of this definition with the provided result payload reducer.
     *
     * @param resultPayloadReducer reducer that updates the workflow payload from the step result
     * @return copied step definition with updated result payload reducer
     */
    public ExecuteStepDefinition resultPayloadReducer(PayloadReducer resultPayloadReducer) {
        return payloadMapping(payloadMapping.resultPayloadReducer(resultPayloadReducer));
    }

    /**
     * Returns a copy of this definition with the provided timing configuration.
     *
     * @param timing timing configuration
     * @return copied step definition with updated timing
     */
    public ExecuteStepDefinition timing(Timing timing) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }

    /**
     * Returns a copy of this definition with the provided timeout.
     *
     * @param timeout timeout duration
     * @return copied step definition with updated timeout
     */
    public ExecuteStepDefinition timeout(Duration timeout) {
        return timing(timing.timeout(timeout));
    }

    /**
     * Returns a copy of this definition with the provided timeout in seconds.
     *
     * @param timeoutSeconds timeout in seconds
     * @return copied step definition with updated timeout
     */
    public ExecuteStepDefinition timeout(long timeoutSeconds) {
        return timing(timing.timeout(timeoutSeconds));
    }

    /**
     * Returns a copy of this definition with the provided retry policy.
     *
     * @param retryPolicy retry policy for the action
     * @return copied step definition with updated retry policy
     */
    public ExecuteStepDefinition retryPolicy(RetryPolicy retryPolicy) {
        return new ExecuteStepDefinition(primitiveMetadata, inputPayload, action, payloadMapping, timing, retryPolicy);
    }
}
