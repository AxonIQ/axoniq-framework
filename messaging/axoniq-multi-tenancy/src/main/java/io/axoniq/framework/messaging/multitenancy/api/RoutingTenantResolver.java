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

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Wraps a {@link TenantResolver} to resolve a {@link Message} (or the {@link ProcessingContext} carrying it) to one of
 * the known tenants, yielding an {@link Optional} instead of throwing when no known tenant matches.
 * <p>
 * The tenant may originate either from the {@link ProcessingContext} resource or from the message itself, depending on
 * how the message was dispatched and where in the handling chain resolution happens. Resolution only ever yields a
 * tenant present in the configured {@link TenantDescriptors}, so a stale or unknown tenant never routes.
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class RoutingTenantResolver implements DescribableComponent {

    private final TenantResolver tenantResolver;
    private final TenantDescriptors tenantDescriptors;

    /**
     * Creates a {@code RoutingTenantResolver} wrapping the given {@code tenantResolver} and resolving against the
     * given {@code tenantDescriptors}.
     *
     * @param tenantResolver    the {@link TenantResolver} to wrap
     * @param tenantDescriptors the known tenants to resolve against
     */
    public RoutingTenantResolver(TenantResolver tenantResolver, TenantDescriptors tenantDescriptors) {
        this.tenantResolver = Objects.requireNonNull(tenantResolver, "The tenant resolver must not be null");
        this.tenantDescriptors = Objects.requireNonNull(tenantDescriptors, "The tenant descriptors must not be null");
    }

    /**
     * Resolves the tenant carried by the given {@code context}, taken from its {@link TenantUtils#TENANT_RESOURCE_KEY
     * tenant resource} when present, otherwise from the message the context carries.
     *
     * @param context the processing context to resolve the tenant from
     * @return the known tenant of the context, or empty when none is available or it is not a known tenant
     */
    public Optional<TenantDescriptor> resolveFromContext(@Nullable ProcessingContext context) {
        if (context == null) {
            return Optional.empty();
        }
        List<TenantDescriptor> knownTenants = tenantDescriptors.tenants();
        return TenantUtils.tenantDescriptorOptional(context)
                          .filter(knownTenants::contains)
                          .or(() -> resolve(Message.fromContext(context), knownTenants));
    }

    /**
     * Resolves the single tenant shared by all given {@code messages}.
     * <p>
     * Returns the resolved tenant when every message resolves to it. Returns {@link Optional#empty()} when the batch is
     * empty or when any message cannot be attributed to a known tenant, so an unresolved message is never silently
     * dropped.
     *
     * @param messages the messages to resolve a single tenant from
     * @return the tenant shared by all messages, or empty when it cannot be determined
     * @throws TenantNotResolvedException if the messages resolve to more than one tenant
     */
    public Optional<TenantDescriptor> resolveSharedTenant(Collection<? extends Message> messages) {
        List<TenantDescriptor> knownTenants = tenantDescriptors.tenants();
        Set<TenantDescriptor> resolvedTenants = new LinkedHashSet<>();
        for (Message message : messages) {
            Optional<TenantDescriptor> resolvedTenant = resolve(message, knownTenants);
            if (resolvedTenant.isEmpty()) {
                return Optional.empty();
            }
            resolvedTenants.add(resolvedTenant.get());
        }
        if (resolvedTenants.size() > 1) {
            throw new TenantNotResolvedException(
                    "Events in a single publish batch must all belong to the same tenant, but found: %s",
                    resolvedTenants.stream().map(TenantDescriptor::tenantId).toList());
        }
        return resolvedTenants.stream().findFirst();
    }

    private Optional<TenantDescriptor> resolve(@Nullable Message message, List<TenantDescriptor> knownTenants) {
        if (message == null) {
            return Optional.empty();
        }
        try {
            TenantDescriptor resolved = tenantResolver.resolveTenant(message, knownTenants);
            return knownTenants.contains(resolved) ? Optional.of(resolved) : Optional.empty();
        } catch (TenantNotResolvedException unresolved) {
            return Optional.empty();
        }
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("tenantResolver", tenantResolver);
        descriptor.describeProperty("tenantDescriptors", tenantDescriptors);
    }
}
