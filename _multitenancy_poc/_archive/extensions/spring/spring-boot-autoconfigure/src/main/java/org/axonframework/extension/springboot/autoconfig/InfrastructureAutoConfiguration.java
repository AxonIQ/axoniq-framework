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

package org.axonframework.extension.springboot.autoconfig;

import org.axonframework.extension.spring.config.MessageHandlerLookup;
import org.axonframework.extension.spring.config.SpringEventSourcedEntityLookup;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Role;

/**
 * Infrastructure autoconfiguration class for Axon Framework application. Constructs the look-up components, like the
 * {@link MessageHandlerLookup} and {@link SpringEventSourcedEntityLookup} to find Axon components and register them with the
 * corresponding configuration enhancers.
 *
 * @author Allard Buijze
 * @since 3.0.4
 */
@AutoConfiguration
public class InfrastructureAutoConfiguration {

    /**
     * Provides Spring message handler lookup.
     *
     * @return The lookup for annotations for later message handling registrations.
     */
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @Bean
    public static MessageHandlerLookup messageHandlerLookup() {
        return new MessageHandlerLookup();
    }

    /**
     * Provides a Spring aggregate lookup.
     *
     * @return The lookup scanning for annotations for later entity registrations.
     */
    @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
    @Bean
    public static SpringEventSourcedEntityLookup springEventSourcedEntityLookup() {
        return new SpringEventSourcedEntityLookup();
    }
}
