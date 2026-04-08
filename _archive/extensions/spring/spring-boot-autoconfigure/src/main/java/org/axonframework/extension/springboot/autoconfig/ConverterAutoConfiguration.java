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

import org.axonframework.conversion.GeneralConverter;
import org.axonframework.extension.springboot.ConverterProperties;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration class dedicated to configuring the delegation behaviour for default {@link MessageConverter}
 * and {@link EventConverter}
 *
 * @author Steven van Beelen
 * @since 5.0.0
 */
@AutoConfiguration
@AutoConfigureBefore(AxonAutoConfiguration.class)
@EnableConfigurationProperties(ConverterProperties.class)
public class ConverterAutoConfiguration {

    /**
     * Bean creation method constructing a {@link MessageConverter} delegating to the {@link GeneralConverter} in
     * case it uses {@code default}.
     *
     * @param generalConverter the {@link GeneralConverter}, used to construct the {@link MessageConverter} in case
     *                         it uses {@code default}
     * @return the {@link MessageConverter} to be used by Axon Framework
     */
    @Bean(name = "messageConverter")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.converter.messages", havingValue = "default", matchIfMissing = true)
    public MessageConverter delegatingMessageConverter(GeneralConverter generalConverter) {
        return new DelegatingMessageConverter(generalConverter);
    }

    /**
     * Bean creation method constructing an {@link EventConverter} delegating to the {@link MessageConverter} in case
     * it uses {@code default}.
     *
     * @param messageConverter the {@link MessageConverter}, used to construct the {@link EventConverter} in case it
     *                         uses {@code default}
     * @return the {@link EventConverter} to be used by Axon Framework.
     */
    @Bean(name = "eventConverter")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "axon.converter.events", havingValue = "default", matchIfMissing = true)
    public EventConverter delegatingEventConverter(MessageConverter messageConverter) {
        return new DelegatingEventConverter(messageConverter);
    }
}
