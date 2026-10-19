/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.customisation;

import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.tracing.LoggingSpanFactory;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.tracing.attributes.MessageIdSpanAttributesProvider;
import org.axonframework.messaging.tracing.attributes.MetadataSpanAttributesProvider;
import org.axonframework.messaging.tracing.attributes.SpanAttributesProviderRegistry;
import org.axonframework.messaging.tracing.configuration.MessagingTracingSettings;

import java.util.Set;

public final class DeclarativeSpanCustomisation {

    // tag::custom-span-factory[]
    public MessagingConfigurer registerSpanFactory(MessagingConfigurer configurer) {
        return configurer.componentRegistry(registry -> registry.registerComponent(
                SpanFactory.class,
                configuration -> LoggingSpanFactory.INSTANCE
        ));
    }
    // end::custom-span-factory[]

    // tag::custom-provider[]
    public MessagingConfigurer registerProvider(MessagingConfigurer configurer) {
        return configurer.componentRegistry(registry -> SpanAttributesProviderRegistry.register(
                registry,
                configuration -> new TenantSpanAttributesProvider()
        ));
    }
    // end::custom-provider[]

    // tag::custom-keys[]
    public MessagingConfigurer customizeBuiltInKeys(MessagingConfigurer configurer) {
        return configurer.componentRegistry(registry -> {
            registry.registerComponent(
                    MessagingTracingSettings.class,
                    configuration -> MessagingTracingSettings.enabledByDefault()
                            .withSpanAttributesProviders(
                                    new MessagingTracingSettings.SpanAttributesProviders(false, true, false)
                            )
            );
            SpanAttributesProviderRegistry.register(
                    registry,
                    configuration -> new MessageIdSpanAttributesProvider("company.message.id")
            );
            SpanAttributesProviderRegistry.register(
                    registry,
                    configuration -> new MetadataSpanAttributesProvider("company.metadata.", Set.of())
            );
        });
    }
    // end::custom-keys[]
}
