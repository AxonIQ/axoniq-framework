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

package io.axoniq.framework.extension.dataprotection;

import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;

/**
 * A {@link ConfigurationEnhancer} that decorates <em>any</em> {@link CryptoEngine} by registering the available
 * {@link EntitlementManager} to it.
 * <p>
 * When no {@code EntitlementManager} is present, this enhancer should break the configuration start-up.
 *
 * @author Steven van Beelen
 * @since 1.0.0
 */
public class DataProtectionConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerDecorator(
                DecoratorDefinition.forType(CryptoEngine.class)
                                   .with((config, name, delegate) -> {
                                       delegate.registerEntitlementManager(
                                               config.getComponent(EntitlementManager.class)
                                       );
                                       return delegate;
                                   })
        );
    }

    @Override
    public int order() {
        return ConfigurationEnhancer.super.order();
    }
}
