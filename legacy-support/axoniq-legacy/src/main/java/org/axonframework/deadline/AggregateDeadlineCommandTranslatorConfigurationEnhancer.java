/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.annotation.RegistrationScope;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.messaging.LegacyScopeAwareProvider;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;

import java.util.function.BiConsumer;

/**
 * A {@link ConfigurationEnhancer} that builds the configuration's {@link AggregateDeadlineCommandTranslator}, and
 * registers it with the configuration's {@link LegacyScopeAwareProvider}, the same way a Saga manager registers
 * itself.
 *
 * @author Steven van Beelen
 * @see AggregateDeadlineCommandTranslator
 * @see LegacyScopeAwareProvider
 * @since 5.4.0
 */
@RegistrationScope(
        "Register one CommandTranslator at the root; it registers itself with the Configuration's "
                + "ScopeAwareProvider, exactly like a Saga manager does."
)
public class AggregateDeadlineCommandTranslatorConfigurationEnhancer implements ConfigurationEnhancer {

    @Override
    public void enhance(ComponentRegistry registry) {
        registry.registerIfNotPresent(
                ComponentDefinition.ofType(AggregateDeadlineCommandTranslator.class)
                                   .withBuilder(config -> new AggregateDeadlineCommandTranslator(
                                           config.getComponent(CommandGateway.class)
                                   ))
                                   .onStart(
                                           Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS,
                                           (BiConsumer<Configuration, AggregateDeadlineCommandTranslator>)
                                                   (config, translator) -> config.getOptionalComponent(
                                                                                         ScopeAwareProvider.class
                                                                                 )
                                                                                 .filter(LegacyScopeAwareProvider.class::isInstance)
                                                                                 .map(LegacyScopeAwareProvider.class::cast)
                                                                                 .ifPresent(provider -> provider.register(
                                                                                         translator
                                                                                 ))
                                   )
        );
    }
}
