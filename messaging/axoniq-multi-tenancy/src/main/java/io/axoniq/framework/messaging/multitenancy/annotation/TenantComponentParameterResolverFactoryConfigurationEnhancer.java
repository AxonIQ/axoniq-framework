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

package io.axoniq.framework.messaging.multitenancy.annotation;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;

/**
 * Configuration enhancer that registers the {@link TenantComponentParameterResolverFactory} to the
 * {@link ComponentRegistry} of the {@link org.axonframework.common.configuration.Configuration}.
 * <p>
 * Contributed through the {@link java.util.ServiceLoader}, so handler parameters annotated with {@link TenantScoped}
 * resolve as soon as the {@code axoniq-multi-tenancy} module is on the classpath. This enhancer keeps running even
 * when multi-tenancy is switched off, because an unresolvable parameter fails handler inspection and with it the whole
 * configuration.
 * <p>
 * Deliberately carries neither an {@link ConfigurationEnhancer#order()} nor a
 * {@link org.axonframework.common.annotation.RegistrationScope}. An earlier order makes
 * {@link ParameterResolverFactoryUtils#registerToComponentRegistry(ComponentRegistry, java.util.function.Function)}
 * contribute the factory as the registry's {@code ParameterResolverFactory} component rather than as a decorator on
 * it, and a {@code CURRENT} scope stops this enhancer running again for a child registry. A child registry copies
 * enhancers and decorators but not components, and a module resolves handler parameters with its own registry's
 * factory, so restricting either one costs handlers inside a module their tenant-scoped parameters.
 *
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
public class TenantComponentParameterResolverFactoryConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                registry,
                TenantComponentParameterResolverFactory::new
        );
    }
}
