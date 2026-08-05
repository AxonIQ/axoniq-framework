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

package migration.paths.multitenancy;

import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.messaging.core.Message;

import java.util.Map;

/**
 * Tagging the message that enters the system with its tenant, which every message it causes then inherits.
 */
public class TenantTagging {

    // tag::tag-message-with-tenant[]
    public Message tagWithTenant(Message message, String tenantId) {
        return message.andMetadata(Map.of(TenantDescriptor.TENANT_ID_KEY, tenantId));
    }
    // end::tag-message-with-tenant[]
}
