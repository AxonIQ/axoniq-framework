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

package org.axonframework.common.util;

/**
 * Resolves certain parameters from the classpath.
 *
 * @author Milan Savic
 * @since 4.6.0
 */
public final class ClasspathResolver {

    private static final boolean PROJECT_REACTOR_ON_CLASSPATH;

    static {
        boolean fluxOnClasspath = true;
        try {
            Class.forName("reactor.core.publisher.Flux", false, ClasspathResolver.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            fluxOnClasspath = false;
        }
        PROJECT_REACTOR_ON_CLASSPATH = fluxOnClasspath;
    }

    /**
     * Return {@code true} if Project Reactor is on classpath, {@code false} otherwise.
     *
     * @return {@code true} if Project Reactor is on classpath, {@code false} otherwise.
     */
    public static boolean projectReactorOnClasspath() {
        return PROJECT_REACTOR_ON_CLASSPATH;
    }

    private ClasspathResolver() {
        // not to be instantiated
    }
}
