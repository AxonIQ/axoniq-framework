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

package org.axonframework.common.configuration;

import org.jspecify.annotations.NonNull;

/**
 * Classpath {@link ConfigurationEnhancer} registered through the serviceloader mechanism in tests. Useful for testing
 * whether registrations are picked up or not.
 * <p>
 * Wrap the building of the configuration in a call to {@link #withActiveTestEnhancer(Runnable)} to activate this
 * enhancer. You can then use {@link #hasEnhanced()} to check whether the enhancer was called.
 *
 * @since 5.0.0
 * @author Mitchell Herrijgers
 */
public class TestConfigurationEnhancer implements ConfigurationEnhancer {

    private static boolean active = false;
    private static boolean hasEnhanced = false;

    public static void withActiveTestEnhancer(Runnable runnable) {
        active = true;
        try {
            runnable.run();
        } finally {
            active = false;
            hasEnhanced = false;
        }
    }

    public static boolean hasEnhanced() {
        return hasEnhanced;
    }

    @Override
    public void enhance(@NonNull ComponentRegistry configurer) {
        if (active) {
            hasEnhanced = true;
        }
    }
}
