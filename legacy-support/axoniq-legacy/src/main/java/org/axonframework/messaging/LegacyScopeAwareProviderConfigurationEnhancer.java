/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.messaging;

import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;

/**
 * A {@link ConfigurationEnhancer} that registers the configuration's {@link LegacyScopeAwareProvider} as its
 * {@link ScopeAwareProvider} component.
 * <p>
 * The provider is created with the {@link ScopeAwareProviderSettings} component, or
 * {@link ScopeAwareProviderSettings#DEFAULT} without one. It becomes ready in the start phase after
 * {@link Phase#INBOUND_EVENT_CONNECTORS}, when the event processors have built every Saga manager, and releases
 * waiting callers in that shutdown phase. An application that registers a {@code ScopeAwareProvider} itself replaces
 * the provider, and receives no components from the configuration.
 * <p>
 * The enhancer is registered through the {@link java.util.ServiceLoader} mechanism.
 *
 * @author Jakob Hatzl
 * @since 5.4.0
 */
@RegistrationScope("Register one provider at the root; Saga managers built by modules find it through their parent.")
public class LegacyScopeAwareProviderConfigurationEnhancer implements ConfigurationEnhancer {

    /**
     * The phase in which the provider becomes ready: right after the event processors started, and thereby built the
     * Saga managers.
     */
    static final int READY_PHASE = Phase.INBOUND_EVENT_CONNECTORS + 1;

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(
                ComponentDefinition.ofType(ScopeAwareProvider.class)
                                   .withBuilder(config -> new LegacyScopeAwareProvider(
                                           config.getOptionalComponent(ScopeAwareProviderSettings.class)
                                                 .orElse(ScopeAwareProviderSettings.DEFAULT)
                                                 .readinessTimeout()
                                   ))
                                   .onStart(READY_PHASE, provider -> {
                                       if (provider instanceof LegacyScopeAwareProvider legacyProvider) {
                                           legacyProvider.markReady();
                                       }
                                   })
                                   .onShutdown(READY_PHASE, provider -> {
                                       if (provider instanceof LegacyScopeAwareProvider legacyProvider) {
                                           legacyProvider.release();
                                       }
                                   })
        );
    }
}
