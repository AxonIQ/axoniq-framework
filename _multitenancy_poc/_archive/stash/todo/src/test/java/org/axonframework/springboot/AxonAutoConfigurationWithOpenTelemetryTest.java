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

package org.axonframework.springboot;

import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.extension.tracing.opentelemetry.OpenTelemetrySpanFactory;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("TODO #3594")
class AxonAutoConfigurationWithOpenTelemetryTest {

    @Test
    void spanFactoryIsOpenTelemetrySpanFactory() {
        new ApplicationContextRunner()
                .withUserConfiguration(Context.class)
                .withPropertyValues("axon.axonserver.enabled=false")
                .run(context -> {
                    assertNotNull(context);

                    assertTrue(context.containsBean("spanFactory"));
                    assertNotNull(context.getBean(SpanFactory.class));
                    assertEquals(OpenTelemetrySpanFactory.class, context.getBean(SpanFactory.class).getClass());
                });
    }

    @EnableAutoConfiguration(exclude = {
            JmxAutoConfiguration.class,
            WebClientAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            DataSourceAutoConfiguration.class,
    })
    @EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
    @Configuration
    public static class Context {

    }
}
