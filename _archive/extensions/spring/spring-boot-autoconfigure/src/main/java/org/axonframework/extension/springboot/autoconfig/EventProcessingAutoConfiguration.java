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

import org.axonframework.extension.spring.config.DefaultProcessorModuleFactory;
import org.axonframework.extension.spring.config.EventProcessorDefinition;
import org.axonframework.extension.spring.config.EventProcessorSettings;
import org.axonframework.extension.spring.config.ProcessorModuleFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.pooled.PooledStreamingEventProcessorModule;
import org.axonframework.extension.springboot.EventProcessorProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

import java.util.List;
import java.util.Map;

/**
 * Auto configuration for event processors.
 *
 * @author Milan Savic
 * @author Simon Zambrovski
 * @since 4.0
 */
@AutoConfiguration
@EnableConfigurationProperties(EventProcessorProperties.class)
public class EventProcessingAutoConfiguration {

    /**
     * Constructs event processing settings.
     *
     * @param environment The spring boot environment.
     * @return The event processor settings keyed by processor name.
     * @see EventProcessorProperties#getProcessors(Environment)
     */
    @Bean
    public EventProcessorSettings.MapWrapper eventProcessorSettings(Environment environment) {
        Map<String, EventProcessorSettings> map = EventProcessorProperties.getProcessors(environment);
        // Retain the default behavior
        map.putIfAbsent(EventProcessorSettings.DEFAULT, new EventProcessorProperties.ProcessorSettings());

        return new EventProcessorSettings.MapWrapper(map);
    }

    @ConditionalOnMissingBean
    @Bean
    ProcessorModuleFactory processorModuleFactory(
            List<EventProcessorDefinition> eventProcessorDefinitions,
            EventProcessorSettings.MapWrapper eventProcessorSettings,
            List<PooledStreamingEventProcessorModule.Customization> extensionsCustomizations
    ) {
        return new DefaultProcessorModuleFactory(
                eventProcessorDefinitions, eventProcessorSettings.settings(), extensionsCustomizations
        );
    }
}
