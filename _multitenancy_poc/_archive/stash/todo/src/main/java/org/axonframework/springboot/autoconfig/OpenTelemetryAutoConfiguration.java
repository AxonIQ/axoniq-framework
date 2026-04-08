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

package org.axonframework.springboot.autoconfig;

import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.extension.tracing.opentelemetry.OpenTelemetrySpanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Automatically configured the {@link OpenTelemetrySpanFactory} as the method of providing tracing in Axon Framework.
 * For this to take effect, the {@code axon-tracing-opentelemetry} dependency must be on the classpath.
 *
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
@AutoConfiguration
@AutoConfigureBefore({AxonTracingAutoConfiguration.class, LegacyAxonAutoConfiguration.class})
@ConditionalOnClass(name = "org.axonframework.extension.tracing.opentelemetry.OpenTelemetrySpanFactory")
public class OpenTelemetryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(SpanFactory.class)
    public SpanFactory spanFactory() {
        return OpenTelemetrySpanFactory.builder().build();
    }
}
