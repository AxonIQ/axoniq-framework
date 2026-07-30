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

import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationDefaults;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.reflection.ParameterResolverFactoryUtils;

/**
 * Configuration enhancer that registers the {@link TenantComponentParameterResolverFactory} to the
 * {@link ComponentRegistry} of the {@link org.axonframework.common.configuration.Configuration}.
 * <p>
 * Contributed through the {@link java.util.ServiceLoader}, so multi-tenancy is active as soon as the
 * {@code axoniq-multi-tenancy} module is on the classpath. Use
 * {@link io.axoniq.framework.messaging.multitenancy.MultiTenancyUtils#disable(ComponentRegistry)} to opt out.
 *
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
@RegistrationScope(scope = RegistrationScope.Scope.CURRENT)
public class TenantComponentParameterResolverFactoryConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The order of {@code this} enhancer compared to others.
     * <p>
     * Runs just after {@link MultiTenancyConfigurationDefaults}, keeping the multi-tenancy enhancers one contiguous
     * block so all multi-tenancy defaults are in place before other enhancers and user registrations that build on
     * them.
     *
     * @since 5.3.0
     */
    public static final int ENHANCER_ORDER = MultiTenancyConfigurationDefaults.ENHANCER_ORDER + 1;

    @Override
    public int order() {
        return ENHANCER_ORDER;
    }

    @Override
    public void enhance(ComponentRegistry registry) {
        ParameterResolverFactoryUtils.registerToComponentRegistry(
                registry,
                TenantComponentParameterResolverFactory::new
        );
    }
}
