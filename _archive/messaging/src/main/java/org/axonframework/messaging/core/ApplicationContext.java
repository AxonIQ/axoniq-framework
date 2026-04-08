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

package org.axonframework.messaging.core;

import org.jspecify.annotations.Nullable;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * An {@code ApplicationContext} is a container for components that are registered in the
 * {@link ComponentRegistry} of the
 * {@link ApplicationConfigurer}. It allows retrieval of components by their type and
 * optionally by their name.
 * <p>
 * This interface is typically used to retrieve components that are registered in the
 * {@link ApplicationConfigurer}'s
 * {@link ComponentRegistry}. It is designed to be used in places where you have access
 * to the {@link ProcessingContext}.
 *
 * @author Mateusz Nowak
 * @author Mitchell Herrijgers
 * @author Steven van Beelen
 * @since 5.0.0
 */
@FunctionalInterface
public interface ApplicationContext {

    /**
     * Returns the component declared under the given {@code type} or throws a {@link ComponentNotFoundException} if it
     * does not exist.
     *
     * @param type The type of component, typically the interface the component implements.
     * @param <C>  The type of component.
     * @return The component registered for the given type.
     * @throws ComponentNotFoundException Whenever there is no component present for the given {@code type}.
     */
    default <C> C component(Class<C> type) {
        return component(type, (String) null);
    }

    /**
     * Returns the component declared under the given {@code type} and {@code name} or throws a
     * {@link ComponentNotFoundException} if it does not exist.
     *
     * @param type The type of component, typically the interface the component implements.
     * @param name The name of the component to retrieve. Use {@code null} when there is no name or use
     *             {@link #component(Class)} instead.
     * @param <C>  The type of component.
     * @return The component registered for the given {@code type} and {@code name}.
     * @throws ComponentNotFoundException Whenever there is no component present for the given {@code type} and
     *                                    {@code name}.
     */
    <C> C component(Class<C> type, @Nullable String name);
}
