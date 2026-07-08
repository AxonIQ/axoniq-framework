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
 * Exception thrown when a tenant can not be resolved.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @since 5.3.0
 */
@SuppressWarnings("java:S110") // S110: "TenantNotFoundException" has 5 parent classes.
public class TenantNotResolvedException extends AxonNonTransientException {

    /**
     * Construct a {@code TenantNotFoundException}.
     *
     * @param message the message (template) for the exception
     * @param args    the arguments for the message template
     */
    public TenantNotResolvedException(String message, Object... args) {
        super(message.formatted(args));
    }

    /**
     * Construct a {@code TenantNotFoundException} referring to the given {@code tenantId}.
     *
     * @param tenantId the tenant identifier that could not be found
     * @return a {@code TenantNotFoundException} with a message indicating the tenant is unknown
     */
    public static TenantNotResolvedException forTenantId(String tenantId) {
        return new TenantNotResolvedException("Tenant with identifier [%s] is unknown", tenantId);
    }
}
