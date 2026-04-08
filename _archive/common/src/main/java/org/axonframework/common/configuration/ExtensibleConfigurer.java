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

import org.axonframework.common.annotation.Internal;

import java.util.function.Supplier;

/**
 * A configurer that supports registering {@link ConfigurationExtension} instances.
 * <p>
 * Extensions are created eagerly when {@link #extend(Class, Supplier)} is called — the factory
 * is invoked immediately and the result is stored. If {@code extend()} is called multiple times
 * for the same type, the new instance always replaces the previous one.
 * <p>
 * For reading extensions, see {@link ExtendedConfiguration}.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see ExtendedConfiguration
 */
@Internal
public interface ExtensibleConfigurer {

    /**
     * Registers an extension factory for the given type and returns {@code this} configurer for chaining.
     * <p>
     * The factory is invoked immediately — the extension is created eagerly, not lazily.
     * If called multiple times for the same type, the new instance always replaces the previous one.
     * <p>
     * Example:
     * <pre>{@code
     * config.extend(DeadLetterQueueConfiguration.class, () -> new DeadLetterQueueConfiguration().enabled().factory(myFactory))
     *       .extend(MetricsExtension.class, () -> new MetricsExtension().enabled());
     * }</pre>
     *
     * @param extensionType the extension class
     * @param factory       a supplier that returns a configured extension
     * @param <T>           the extension type
     * @return {@code this} configurer, for fluent chaining
     */
    <T extends ConfigurationExtension<?>> ExtensibleConfigurer extend(Class<T> extensionType, Supplier<T> factory);
}
