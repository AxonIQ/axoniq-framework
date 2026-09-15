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

    private final ClassLoader classLoader;

    /**
     * The order at which dead-letter queue support is configured after the general and Axon Server multi-tenancy
     * components.
     */
    public static final int ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER + 4;

    /**
     * Creates an enhancer using the class loader which loaded this class to detect optional dead-letter queue support.
     */
    public DeadLetterMultiTenancyConfigurationEnhancer() {
        this(DeadLetterMultiTenancyConfigurationEnhancer.class.getClassLoader());
    }

    /**
     * Creates an enhancer using the supplied class loader to detect optional dead-letter queue support.
     *
     * @param classLoader the class loader used to detect optional dead-letter queue support
     */
    DeadLetterMultiTenancyConfigurationEnhancer(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

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
        // Keep optional dead-letter queue types out of the ServiceLoader provider's linkage surface.
        // The delegate is loaded reflectively only when the optional module is available.
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
