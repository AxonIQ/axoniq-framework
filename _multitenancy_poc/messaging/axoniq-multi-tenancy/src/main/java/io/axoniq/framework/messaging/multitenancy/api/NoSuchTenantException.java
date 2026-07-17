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
package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.AxonNonTransientException;

/**
 * Exception thrown when a tenant is not found.
 *
 * @author Stefan Dragisic
 * @since 5.3.0
 */
@SuppressWarnings("java:S110")
public class NoSuchTenantException extends AxonNonTransientException {

    /**
     * Construct a NoSuchTenantException referring to the given {@code tenantId}.
     *
     * @param tenantId The tenant identifier that could not be found.
     */
    public NoSuchTenantException(String tenantId) {
        super("Tenant with identifier [" + tenantId + "] is unknown");
    }
}
