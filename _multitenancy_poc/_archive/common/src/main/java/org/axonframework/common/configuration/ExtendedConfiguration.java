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
import org.jspecify.annotations.Nullable;

/**
 * A configuration that supports reading modular {@link ConfigurationExtension} instances.
 * <p>
 * Extensions must be registered first via {@link ExtensibleConfigurer#extend(Class, java.util.function.Supplier)}.
 * If no extension of the requested type has been registered, {@code null} is returned.
 * <p>
 * For registering extensions, see {@link ExtensibleConfigurer}.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see ExtensibleConfigurer
 */
@Internal
public interface ExtendedConfiguration {

    /**
     * Returns the extension of the given type, or {@code null} if no extension of that type has been registered.
     * <p>
     * Returns the instance created by the factory registered via
     * {@link ExtensibleConfigurer#extend(Class, java.util.function.Supplier)}.
     *
     * @param extensionType the extension class
     * @param <T>           the extension type
     * @return the extension instance, or {@code null} if not registered
     */
    @Nullable
    <T extends ConfigurationExtension<?>> T extension(Class<T> extensionType);
}
