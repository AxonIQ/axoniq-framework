/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package tracing.customisation;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.tracing.SpanAttributesProvider;
import org.jspecify.annotations.Nullable;

import java.util.Map;

// tag::provider[]
public final class TenantSpanAttributesProvider implements SpanAttributesProvider {

    @Override
    public Map<String, String> provideForMessage(Message message, @Nullable ProcessingContext context) {
        String tenant = message.metadata().get("tenant");
        return tenant == null ? Map.of() : Map.of("company.tenant", tenant);
    }
}
// end::provider[]
