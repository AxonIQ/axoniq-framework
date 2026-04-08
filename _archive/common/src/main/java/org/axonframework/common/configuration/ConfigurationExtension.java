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

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.DescribableComponent;

/**
 * A modular extension of an {@link ExtendedConfiguration}. Parameterized with
 * the parent configuration type {@code P} so that implementations get full typed
 * access to the parent's API.
 * <p>
 * Extensions are pure data — they hold settings but carry no behavior.
 * Behavior that acts on extension data belongs in a {@link ConfigurationEnhancer}.
 * <p>
 * Extensions are registered via {@link ExtensibleConfigurer#extend(Class, java.util.function.Supplier)}
 * and retrieved via {@link ExtendedConfiguration#extension(Class)}. No reflection is used — the factory
 * function provided at registration time is responsible for creating the extension instance.
 *
 * @param <P> the parent configuration type this extension is designed for
 * @author Mateusz Nowak
 * @since 5.1.0
 */
@Internal
public interface ConfigurationExtension<P extends ExtendedConfiguration> extends DescribableComponent {

    /**
     * Returns the name of this configuration extension.
     *
     * @return the configuration extension's name
     */
    String name();

    /**
     * Validates this extension's settings.
     *
     * @throws AxonConfigurationException if any settings are invalid
     */
    void validate() throws AxonConfigurationException;
}
