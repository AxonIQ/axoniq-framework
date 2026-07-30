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
