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
