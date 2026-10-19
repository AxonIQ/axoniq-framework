/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.util;

import org.axonframework.common.annotation.Internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Reflection utilities.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
@Internal
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
    public static <C> Class<C> requireIsAssignableFrom(Class<?> expected, Class<C> clazz) {
        if (!expected.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format(
                    "Provided type %s must be instance of WorkflowState, but it was not.",
                    clazz.getName()));
        }
        return clazz;
    }

    /**
     * Invokes a method.
     *
     * @param instance object to invoke on.
     * @param method   method to invoke.
     * @param args     args of the method.
     * @return result of invocation.
     */
    public static Object invoke(Object instance, Method method, Object... args) {
        try {
            return method.invoke(instance, args);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getCause());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Cannot access " + method, e);
        } catch (Exception e) {
            throw new RuntimeException("Error invoking " + method, e);
        }
    }
}
