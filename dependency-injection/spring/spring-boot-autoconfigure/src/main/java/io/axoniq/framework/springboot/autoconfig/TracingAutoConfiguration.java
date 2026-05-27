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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springboot.TracingProperties;
import io.axoniq.framework.tracing.NoOpSpanFactory;
import io.axoniq.framework.tracing.SpanAttributesProvider;
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.attributes.AggregateIdentifierSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageIdSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageNameSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MessageTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.MetadataSpanAttributesProvider;
import io.axoniq.framework.tracing.attributes.PayloadTypeSpanAttributesProvider;
import io.axoniq.framework.tracing.messaging.MessagingTracingSettings;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration wiring AxoniqFramework distributed tracing into a Spring Boot application.
 * <p>
 * Provides a default {@link SpanFactory} (the {@link NoOpSpanFactory} unless another factory bean is present, such as
 * the {@code OpenTelemetrySpanFactory} contributed by {@link OpenTelemetryTracingAutoConfiguration}), exposes the
 * built-in {@link SpanAttributesProvider} beans, and registers a {@link ConfigurationEnhancer} that hands the factory
 * and the {@code axon.tracing.*}-derived settings to the framework's {@code ComponentRegistry}.
 * <p>
 * The whole configuration backs off when {@code axon.tracing.enabled} is set to {@code false}, and each built-in
 * attribute provider can be toggled individually through {@code axon.tracing.attribute-providers.*} properties.
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
@AutoConfiguration
@ConditionalOnClass(SpanFactory.class)
@ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(TracingProperties.class)
public class TracingAutoConfiguration {

    /**
     * Provides the default {@link SpanFactory} when no other factory bean is defined.
     * <p>
     * Returns the {@link NoOpSpanFactory} singleton, which produces no spans and imposes no measurable overhead.
     *
     * @return the no-op {@link SpanFactory}
     */
    @Bean
    @ConditionalOnMissingBean(SpanFactory.class)
    public SpanFactory spanFactory() {
        return NoOpSpanFactory.INSTANCE;
    }

    /**
     * Provides the built-in {@link MessageIdSpanAttributesProvider}.
     *
     * @return the message-id attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-id",
            havingValue = "true", matchIfMissing = true)
    public MessageIdSpanAttributesProvider messageIdSpanAttributesProvider() {
        return new MessageIdSpanAttributesProvider();
    }

    /**
     * Provides the built-in {@link MessageNameSpanAttributesProvider}.
     *
     * @return the message-name attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-name",
            havingValue = "true", matchIfMissing = true)
    public MessageNameSpanAttributesProvider messageNameSpanAttributesProvider() {
        return new MessageNameSpanAttributesProvider();
    }

    /**
     * Provides the built-in {@link MessageTypeSpanAttributesProvider}.
     *
     * @return the message-type attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "message-type",
            havingValue = "true", matchIfMissing = true)
    public MessageTypeSpanAttributesProvider messageTypeSpanAttributesProvider() {
        return new MessageTypeSpanAttributesProvider();
    }

    /**
     * Provides the built-in {@link PayloadTypeSpanAttributesProvider}.
     *
     * @return the payload-type attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "payload-type",
            havingValue = "true", matchIfMissing = true)
    public PayloadTypeSpanAttributesProvider payloadTypeSpanAttributesProvider() {
        return new PayloadTypeSpanAttributesProvider();
    }

    /**
     * Provides the built-in {@link MetadataSpanAttributesProvider}.
     *
     * @return the metadata attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "metadata",
            havingValue = "true", matchIfMissing = true)
    public MetadataSpanAttributesProvider metadataSpanAttributesProvider() {
        return new MetadataSpanAttributesProvider();
    }

    /**
     * Provides the built-in {@link AggregateIdentifierSpanAttributesProvider}.
     *
     * @return the aggregate-identifier attribute provider
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing.attribute-providers", name = "aggregate-identifier",
            havingValue = "true", matchIfMissing = true)
    public AggregateIdentifierSpanAttributesProvider aggregateIdentifierSpanAttributesProvider() {
        return new AggregateIdentifierSpanAttributesProvider();
    }

    /**
     * Constructs a {@link ConfigurationEnhancer} that bridges the Spring-managed tracing beans into the framework's
     * {@code ComponentRegistry}.
     * <p>
     * The enhancer registers every discovered {@link SpanAttributesProvider} on the {@code spanFactory}, exposes the
     * factory as a framework component, and registers the {@link MessagingTracingSettings} derived from the
     * {@code axon.tracing.*} properties so the messaging tracing enhancer can decide which components to decorate.
     *
     * @param spanFactory the {@link SpanFactory} to expose to the framework
     * @param providers   the discovered {@link SpanAttributesProvider} beans contributing span attributes
     * @param properties  the bound {@link TracingProperties}
     * @return a {@link ConfigurationEnhancer} registering the tracing components with the framework
     */
    @Bean
    public ConfigurationEnhancer tracingConfigurationEnhancer(SpanFactory spanFactory,
                                                              ObjectProvider<SpanAttributesProvider> providers,
                                                              TracingProperties properties) {
        return registry -> {
            providers.orderedStream().forEach(spanFactory::registerAttributesProvider);
            registry.registerComponent(SpanFactory.class, c -> spanFactory);
            registry.registerComponent(
                    MessagingTracingSettings.class,
                    c -> new MessagingTracingSettings(properties.getCommandBus().isEnabled())
            );
        };
    }
}
