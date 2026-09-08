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

package io.axoniq.framework.integrationtests.testsuite.infrastructure;

import org.axonframework.common.configuration.ComponentRegistry;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Utility class for Axon Framework integration tests.
 * <p>
 * It provides a way to disable multi-tenancy in the test infrastructure, if the multi-tenancy module is available on
 * the classpath.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
final class AxonIntegrationTestSupport {

    /**
     * The marker class we need to check for to determine if multi-tenancy is available on the classpath.
     */
    private static final String MULTI_TENANCY_UTILS =
            "io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils";

    @Nullable
    private static final Class<?> multiTenancyUtils;

    /**
     * Consumer that disables multi-tenancy via {@code MultiTenancyUtils.disable(ComponentRegistry)} if the
     * multi-tenancy module is available on the classpath, otherwise a no-op.
     */
    static Consumer<ComponentRegistry> disableMultiTenancy;

    static {
        Class<?> multiTenancyUtilsClass;
        try {
            multiTenancyUtilsClass = Class.forName("io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils");
        } catch (ClassNotFoundException e) {
            multiTenancyUtilsClass = null;
        }
        multiTenancyUtils = multiTenancyUtilsClass;
        disableMultiTenancy = createDisableMultiTenancyConsumer();
    }

    /**
     * @return {@code true} if multi-tenancy is available on the classpath, {@code false} otherwise.
     */
    static boolean isMultiTenancyAvailable() {
        return multiTenancyUtils != null;
    }

    private static Consumer<ComponentRegistry> createDisableMultiTenancyConsumer() {
        if (disableMultiTenancy == null) {
            return registry -> {
                // no-op
            };
        }

        Method disable;
        try {
             disable = requireNonNull(multiTenancyUtils).getMethod("disable", ComponentRegistry.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }

        return registry -> {
            try {
                disable.invoke(null, registry);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private AxonIntegrationTestSupport() {
        // utility class
    }
}
