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
