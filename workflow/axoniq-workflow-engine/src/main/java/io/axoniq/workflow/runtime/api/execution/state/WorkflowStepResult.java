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
package io.axoniq.workflow.runtime.api.execution.state;

import org.jspecify.annotations.Nullable;

import org.axonframework.common.TypeReference;
import org.axonframework.conversion.Converter;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a result of a step execution.
 * @since 5.4.0
 */
public interface WorkflowStepResult {

    String getStepName();

    /**
     * Check (non-blocking) if the step has finished (reached a terminal state).
     *
     * @return true, if the state is terminal.
     */
    boolean isCompleted();

    /**
     * Retrieves optional result.
     *
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished.
     */
    Optional<Map<String, @Nullable Object>> result();

    /**
     * Retrieves an optional result as a specific type.
     *
     * @param type the type to convert the result to
     * @param <T>  the type of the result
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    default <T> Optional<T> resultAs(Class<T> type) {
        return resultAs((Type) type);
    }

    /**
     * Retrieves an optional result as a specific type.
     *
     * @param type the type to convert the result to
     * @param <T>  the type of the result
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    default <T> Optional<T> resultAs(TypeReference<T> type) {
        return resultAs(type.getType());
    }

    /**
     * Retrieves an optional result as a specific type.
     *
     * @param type the type to convert the result to
     * @param <T>  the type of the result
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    <T> Optional<T> resultAs(Type type);

    /**
     * Retrieves an optional result as a specific type using a converter.
     *
     * @param type      the type to convert the result to
     * @param converter the converter to use
     * @param <T>       the type of the result
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    default <T> Optional<T> resultAs(Class<T> type, Converter converter) {
        return resultAs((Type) type, converter);
    }

    /**
     * Retrieves an optional result as a specific type.
     *
     * @param type the type to convert the result to
     * @param <T>  the type of the result
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    default <T> Optional<T> resultAs(TypeReference<T> type, Converter converter) {
        return resultAs(type.getType(), converter);
    }

    /**
     * Retrieves an optional result as a specific type.
     *
     * @param type the type to convert the result to
     * @return result if the step has finished with a success. Will return empty optional, if the step is not finished
     */
    <T> Optional<T> resultAs(Type type, Converter converter);

    /**
     * Retrieves an optional error.
     *
     * @return error if the step has finished with an error. Will return empty optional, if the step is not finished
     */
    Optional<StepFailedException> error();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation completed successfully
     */
    boolean success();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation completed with an exception
     */
    boolean failure();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation was interrupted by an external party
     */
    boolean canceled();

    /**
     * Blocks until step execution reaches a terminal state.
     *
     * @return true, if the operation was interrupted by timeout specified on start
     */
    boolean timeout();

    /**
     * Blocks until step execution reaches a terminal state.
     */
    void await();

    /**
     * Cancels this step if it is still running. No-op if already in a terminal state.
     */
    void cancel();

    /**
     * Cancels this step with a reason if it is still running. No-op if already in a terminal state.
     *
     * @param reason human-readable cancellation reason.
     */
    void cancel(String reason);
}
