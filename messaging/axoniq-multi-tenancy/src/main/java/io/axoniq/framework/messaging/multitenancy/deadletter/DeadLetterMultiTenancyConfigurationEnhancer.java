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

package io.axoniq.framework.messaging.multitenancy.deadletter;

import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;

/**
 * Makes an enabled dead-letter queue configuration tenant-aware by routing its configured queue factory per tenant.
 * <p>
 * This enhancer does nothing if the optional dependency for dead-letter queue support is not present on the classpath.
 *
 * @author Jan Galinski
 * @since 5.4.0
 */
@Internal
public class DeadLetterMultiTenancyConfigurationEnhancer implements ConfigurationEnhancer {

    private static final String DEAD_LETTER_QUEUE_CONFIGURATION =
            "io.axoniq.framework.messaging.eventhandling.deadletter.DeadLetterQueueConfiguration";
    private static final String DEAD_LETTER_ENHANCER_DELEGATE =
            "io.axoniq.framework.messaging.multitenancy.deadletter.DeadLetterMultiTenancyConfigurationEnhancerDelegate";

    /**
     * The order at which dead-letter queue support is configured after the general and Axon Server multi-tenancy
     * components.
     */
    public static final int ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER + 4;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    /**
     * Checks whether the optional dead-letter queue module is available to the supplied class loader.
     *
     * @param classLoader the class loader to inspect
     * @return {@code true} when dead-letter queue support is available
     */
    public static boolean isDeadLetterQueuePresent(ClassLoader classLoader) {
        try {
            Class.forName(DEAD_LETTER_QUEUE_CONFIGURATION, false, classLoader);
            return true;
        } catch (ClassNotFoundException | LinkageError ignored) {
            return false;
        }
    }

    @Override
    public void enhance(ComponentRegistry componentRegistry) {
        // important, see bug(#480): when DLQ is not on the classpath, SPI must not depend on any DLQ classes,
        // otherwise the SPI will fail to load and the application will not start.
        // Thus, we use reflection here to call the actual enhance() logic.
        // note: kept simple on purpose, as this is only called once during configuration and not in a hot path.
        // If we find that this is an expensive operation, we can cache a MethodHandle, but that is premature optimization right now.
        ClassLoader classLoader = getClass().getClassLoader();
        if (!isDeadLetterQueuePresent(classLoader)) {
            return;
        }
        try {
            Class<?> delegate = Class.forName(DEAD_LETTER_ENHANCER_DELEGATE, false, classLoader);
            delegate.getDeclaredMethod("enhance", ComponentRegistry.class).invoke(null, componentRegistry);
        } catch (ClassNotFoundException | LinkageError ignored) {
            // The optional dead-letter module is not usable from this class loader.
        } catch (ReflectiveOperationException e) {
            Throwable cause = e instanceof java.lang.reflect.InvocationTargetException invocation
                    ? invocation.getCause()
                    : e;
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AxonConfigurationException("Failed to configure tenant-aware dead-letter queue support.", cause);
        }
    }
}
