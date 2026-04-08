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

/**
 * Interface describing a module of Axon Framework's configuration API.
 * <p>
 * Modules are relatively independent. They can be {@link ComponentRegistry#registerModule(Module) registered} on a
 * parent {@link ApplicationConfigurer} or registered in a nested style on another {@link Module} through the dedicated
 * register module operation. Furthermore, a module is able to access the registered {@link Component Components} from
 * the parent {@code ApplicationConfigurer} it is registered too. However, the parent is <b>not</b> able to retrieve
 * components from these {@code Modules}, ensuring encapsulation.
 *
 * @author Allard Buijze
 * @author Steven van Beelen
 * @since 3.0.0
 */
public interface Module {

    /**
     * The identifying name of {@code this Module}.
     *
     * @return The identifying name of {@code this Module}.
     */
    String name();

    /**
     * Builds {@code this Module}, resulting in the {@link Configuration} containing all registered components.
     * <p>
     * The given {@code parent} allows access to components that have been registered with it. Note that this operation
     * is typically invoked through {@link ApplicationConfigurer#build()} and as such should not be invoked directly.
     *
     * @param parent                    The parent {@code Configuration} {@code this Module} belongs in, giving it
     *                                  access to the parent's components.
     * @param lifecycleRegistry         The registry where lifecycle handlers can be registered by this module.
     * @return The fully initialized {@link Configuration} instance from {@code this Module} specifically.
     */
    Configuration build(Configuration parent, LifecycleRegistry lifecycleRegistry);
}
