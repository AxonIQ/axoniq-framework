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

package org.axonframework.common;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility for creating classes by name containing a class cache.
 */
public final class ClassUtils {

    private static ClassLoader classLoader = ClassUtils.class.getClassLoader();
    private static ConcurrentHashMap<String, Class<?>> cache = new ConcurrentHashMap<>();

    /**
     * Avoid instantiation.
     */
    private ClassUtils() {
    }


    /**
     * Loads a class by name
     *
     * @param className The name of the class to load.
     * @param <C>       The class type.
     * @return The loaded class.
     */
    public static <C> Class<C> loadClass(String className) {
        //noinspection unchecked
        return (Class<C>) cache.computeIfAbsent(className, (name) -> {
            try {
                return classLoader.loadClass(name);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not load class " + name, e);
            }
        });
    }
}
