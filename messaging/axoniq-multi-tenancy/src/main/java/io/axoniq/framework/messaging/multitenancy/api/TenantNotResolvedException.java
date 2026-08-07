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

import java.util.function.Supplier;

/**
 * Exception thrown when no tenant can be established for a message or an operation.
 *
 * @author Stefan Dragisic
 * @author Jan Galinski
 * @since 4.6.0
 */
@SuppressWarnings("java:S110") // S110: "TenantNotResolvedException" has 5 parent classes.
public class TenantNotResolvedException extends AxonNonTransientException {

    /**
     * Returns a {@code Supplier} that creates a new {@code TenantNotResolvedException} with the given message and
     * arguments, useful in lambda expressions or method references.
     *
     * @param message the message (template) for the exception
     * @param args the arguments for the message template
     * @return a {@code Supplier} that creates a new {@code TenantNotResolvedException} with the given message and arguments
     */
    public static Supplier<TenantNotResolvedException> tenantNotResolved(String message, Object... args) {
        return () -> new TenantNotResolvedException(message, args);
    }

    /**
     * Construct a {@code TenantNotResolvedException}.
     *
     * @param message the message (template) for the exception
     * @param args    the arguments for the message template
     */
    public TenantNotResolvedException(String message, Object... args) {
        super(args.length > 0 ? message.formatted(args) : message);
    }

    /**
     * Construct a {@code TenantNotResolvedException} referring to the given {@code tenantId}.
     *
     * @param tenantId the tenant identifier that could not be resolved
     * @return a {@code TenantNotResolvedException} with a message indicating the tenant is unknown
     */
    public static TenantNotResolvedException forTenantId(String tenantId) {
        return new TenantNotResolvedException("Tenant with identifier [%s] is unknown", tenantId);
    }
}
