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

import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.opentelemetry.OpenTelemetrySpanFactory;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration contributing an OpenTelemetry-backed {@link SpanFactory} when OpenTelemetry is on the classpath.
 * <p>
 * This configuration runs before {@link TracingAutoConfiguration} so the {@link OpenTelemetrySpanFactory} is preferred
 * over the default {@link io.axoniq.framework.tracing.NoOpSpanFactory}. When an {@link OpenTelemetry} bean is present
 * in the context it is used; otherwise the factory falls back to {@link GlobalOpenTelemetry#get()}.
 * <p>
 * The configuration backs off when {@code axon.tracing.enabled} is set to {@code false}, or when another
 * {@link SpanFactory} bean is already defined.
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
@AutoConfiguration(before = TracingAutoConfiguration.class)
@ConditionalOnClass({OpenTelemetry.class, OpenTelemetrySpanFactory.class})
@ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OpenTelemetryTracingAutoConfiguration {

    /**
     * Provides an {@link OpenTelemetrySpanFactory} as the {@link SpanFactory} when none is defined yet.
     * <p>
     * Uses an {@link OpenTelemetry} bean from the context when available, falling back to
     * {@link GlobalOpenTelemetry#get()} otherwise.
     *
     * @param openTelemetry the {@link OpenTelemetry} instance to back the factory, if available
     * @return an OpenTelemetry-backed {@link SpanFactory}
     */
    @Bean
    @ConditionalOnMissingBean(SpanFactory.class)
    public SpanFactory openTelemetrySpanFactory(ObjectProvider<OpenTelemetry> openTelemetry) {
        return new OpenTelemetrySpanFactory(openTelemetry.getIfAvailable(GlobalOpenTelemetry::get));
    }
}
