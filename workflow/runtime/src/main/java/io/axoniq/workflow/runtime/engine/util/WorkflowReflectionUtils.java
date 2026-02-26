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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.util;

import jakarta.annotation.Nonnull;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/**
 * Reflection utilities.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class WorkflowReflectionUtils {

    private WorkflowReflectionUtils() {
        // avoid
    }

    /**
     * Creates an instance of specified type.
     *
     * @param clazz type to instantiate.
     * @param <T>   type of instance.
     * @return optional instance or empty on every error.
     */
    public static <T> Optional<T> createDefaultInstance(Class<T> clazz) {
        try {
            return Optional.of(clazz.getConstructor().newInstance());
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            // ignore
        }
        return Optional.empty();
    }

    /**
     * Checks if provided class is a subtype of given type and throws an exception if this checks fails.
     *
     * @param expected expected type.
     * @param clazz    class to check.
     * @return provided class.
     */
    @Nonnull
    public static <C> Class<C> requireIsAssignableFrom(@Nonnull Class<?> expected, @Nonnull Class<C> clazz) {
        if (!expected.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format(
                    "Provided type %s must be instance of WorkflowState, but it was not.",
                    clazz.getName()));
        }
        return clazz;
    }
}
