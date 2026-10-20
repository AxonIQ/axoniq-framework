/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.customisation;

import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.axonframework.messaging.tracing.attributes.MessageIdSpanAttributesProvider;
import org.axonframework.messaging.tracing.attributes.MetadataSpanAttributesProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

@Configuration(proxyBeanMethods = false)
public class SpringSpanAttributesConfiguration {

    // tag::custom-provider[]
    @Bean
    SpanAttributesProvider tenantSpanAttributesProvider() {
        return new TenantSpanAttributesProvider();
    }
    // end::custom-provider[]

    // tag::custom-keys[]
    @Bean
    MessageIdSpanAttributesProvider messageIdSpanAttributesProvider() {
        return new MessageIdSpanAttributesProvider("company.message.id");
    }

    @Bean
    MetadataSpanAttributesProvider metadataSpanAttributesProvider() {
        return new MetadataSpanAttributesProvider("company.metadata.", Set.of());
    }
    // end::custom-keys[]
}
