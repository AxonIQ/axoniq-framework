/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
