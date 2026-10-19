/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.customisation;

import org.axonframework.messaging.tracing.LoggingSpanFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SpringCustomSpanFactoryConfiguration {

    // tag::custom-span-factory[]
    @Bean
    SpanFactory spanFactory() {
        return LoggingSpanFactory.INSTANCE;
    }
    // end::custom-span-factory[]
}
