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
import io.axoniq.framework.tracing.SpanFactory;
import io.axoniq.framework.tracing.opentelemetry.OpenTelemetrySpanFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests validating the wiring of {@link TracingAutoConfiguration} and {@link OpenTelemetryTracingAutoConfiguration}
 * through Spring Boot's {@link ApplicationContextRunner}.
 *
 * @author AxonIQ
 * @since 5.2.0
 */
class TracingAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TracingAutoConfiguration.class,
                    OpenTelemetryTracingAutoConfiguration.class
            ));

    @Test
    void defaultsContributeOpenTelemetrySpanFactory() {
        // given / when / then
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(SpanFactory.class);
            assertThat(context.getBean(SpanFactory.class)).isInstanceOf(OpenTelemetrySpanFactory.class);
        });
    }

    @Test
    void tracingDisabledBacksOffSpanFactory() {
        // given / when / then
        contextRunner.withPropertyValues("axon.tracing.enabled=false")
                     .run(context -> assertThat(context).doesNotHaveBean(SpanFactory.class));
    }

    @Test
    void commandBusEnabledPropertyBindsToFalse() {
        // given / when / then
        contextRunner.withPropertyValues("axon.tracing.command-bus.enabled=false")
                     .run(context -> {
                         assertThat(context).hasSingleBean(TracingProperties.class);
                         TracingProperties properties = context.getBean(TracingProperties.class);
                         assertThat(properties.getCommandBus().isEnabled()).isFalse();
                     });
    }
}
