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

import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;

/**
 * Utility class for multi-tenancy API.
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
public final class MultiTenancyApiUtils {

    /**
     * The key used to store the {@link TenantDescriptor} in a {@link ProcessingContext} or {@link Metadata}.
     */
    public static final String TENANT_ID_KEY = "tenantId";

    /**
     * The {@link ResourceKey} used to store the {@link TenantDescriptor} in a {@link ProcessingContext}.
     */
    public static final ResourceKey<TenantDescriptor> TENANT_RESOURCE_KEY = ResourceKey.withLabel(TENANT_ID_KEY);

    /**
     * Retrieves the {@link TenantDescriptor} from the given {@link ProcessingContext}.
     *
     * @param processingContext the {@link ProcessingContext} to retrieve the {@link TenantDescriptor} from
     * @return the {@link TenantDescriptor} stored in the {@link ProcessingContext}
     * @throws TenantNotResolvedException if no {@link TenantDescriptor} is found in the {@link ProcessingContext}
     * @see #tenantDescriptorOptional(ProcessingContext)
     */
    public static TenantDescriptor tenantDescriptor(ProcessingContext processingContext)
            throws TenantNotResolvedException {
        return tenantDescriptorOptional(processingContext)
                .orElseThrow(tenantNotResolved("No tenant descriptor found in processing context"));
    }

    /**
     * A {@link Function} that tries to retrive the {@link TenantDescriptor} from a {@link ProcessingContext}.
     *
     * @param processingContext the {@link ProcessingContext} to retrieve the {@link TenantDescriptor} from
     * @return the {@link TenantDescriptor} from the {@link ProcessingContext}, wrapped in an {@link Optional}
     */
    public static Optional<TenantDescriptor> tenantDescriptorOptional(ProcessingContext processingContext) {
        return Optional.ofNullable(processingContext.getResource(TENANT_RESOURCE_KEY));
    }

    /**
     * A wrapper around a {@link TenantResolver} that returns an {@link Optional} instead of throwing an exception when
     * no tenant could be resolved. Also provides additional convenience methods for resolving a tenant from a
     * collection of {@link Message messages}.
     *
     * We need this in scenarios where we just can't be sure if we get the tenant from the ProcessingContext or the
     * message, depending on how it was dispatched and where in the chain we are.
     *
     * @author Jan Galinski
     * @since 5.3.0
     */
    public static class OptionalTenantResolver implements Function<@Nullable Message, Optional<TenantDescriptor>> {

        private final TenantResolver tenantResolver;
        private final TenantDescriptors tenantDescriptors;

        /**
         * Creates a new {@code OptionalTenantResolver} that wraps the given {@code tenantResolver} and uses an empty
         * collection of {@link TenantDescriptor tenants} for resolution.
         *
         * @param tenantResolver the {@link TenantResolver} to wrap
         * @see #OptionalTenantResolver(TenantResolver, TenantDescriptors)
         */
        public OptionalTenantResolver(TenantResolver tenantResolver) {
            this(tenantResolver, Collections::emptyList);
        }

        /**
         * Creates a new {@code OptionalTenantResolver} that wraps the given {@code tenantResolver} and uses the given
         * {@code tenantDescriptors} for resolution.
         *
         * @param tenantResolver    the {@link TenantResolver} to wrap
         * @param tenantDescriptors the {@link TenantDescriptors} to use for resolution
         */
        public OptionalTenantResolver(TenantResolver tenantResolver, TenantDescriptors tenantDescriptors) {
            this.tenantResolver = tenantResolver;
            this.tenantDescriptors = tenantDescriptors;
        }

        @Override
        public Optional<TenantDescriptor> apply(@Nullable Message message) {
            if (message == null) {
                return Optional.empty();
            }
            try {
                return Optional.of(tenantResolver.resolveTenant(message, tenantDescriptors.tenants()));
            } catch (TenantNotResolvedException e) {
                return Optional.empty();
            }
        }

        /**
         * Resolves the tenant from a collection of {@link Message messages}. If all messages resolve to the same
         * tenant, that tenant is returned. If any message resolves to a different tenant, a
         * {@link TenantNotResolvedException} is thrown.
         *
         * @param messages the collection of messages to resolve the tenant from
         * @return the resolved tenant, or {@link Optional#empty()} if no tenant could be resolved
         * @throws TenantNotResolvedException if any message resolves to a different tenant than the others
         */
        public Optional<TenantDescriptor> apply(Collection<? extends @Nullable Message> messages)
                throws TenantNotResolvedException {
            return messages.stream()
                           .map(this)
                           .filter(Optional::isPresent)
                           .reduce((a, b) -> {
                               if (a.equals(b)) {
                                   return a;
                               }
                               throw new TenantNotResolvedException(
                                       "Events in a single publish batch must all belong to the same tenant, but found mixed tenants: %s vs %s",
                                       a.map(TenantDescriptor::tenantId).orElse("<unresolved>"),
                                       b.map(TenantDescriptor::tenantId).orElse("<unresolved>")
                               );
                           })
                           .flatMap(opt -> opt);
        }
    }

    /**
     * Resolves the tenant from the current {@link ProcessingContext} by extracting the message stored on it and passing
     * it to the given {@code resolver}.
     * <p>
     * This is a convenience method for components that operate within an existing processing context (such as the event
     * store or snapshot store) and need to resolve the tenant from the context's message rather than from a directly
     * available message parameter.
     *
     * @param context the processing context containing the message
     * @param tenants the collection of known tenants
     * @return the resolved {@link TenantDescriptor}
     * @throws IllegalStateException if no message is found in the processing context
     */
    TenantDescriptor resolveTenant(ProcessingContext context, Collection<TenantDescriptor> tenants) {
        Message message = Optional.ofNullable(Message.fromContext(context))
                                  .orElseThrow(() -> new IllegalStateException(
                                          "Cannot resolve tenant: no message found in ProcessingContext"));

        return null; // resolveTenant(message, tenants);
    }
//
//    static Optional<TenantDescriptor> resolveTenant(TenantResolver tenantResolver,
//                                                    Te
//                                                    Collection<? extends Message> messages) {
//        return messages.stream()
//
//            .map(tenantResolver)
//                     .reduce((a, b) -> {
//        if (a.equals(b)) {
//            return a;
//        }
//        throw new TenantNotResolvedException(
//                "Events in a single publish batch must all belong to the same tenant, but found mixed tenants: %s vs %s",
//                a.map(TenantDescriptor::tenantId).orElse("<unresolved>"),
//                b.map(TenantDescriptor::tenantId).orElse("<unresolved>")
//        );
//    })
//            .flatMap(opt -> opt);
//}

    private MultiTenancyApiUtils() {
        // utility class
    }
}
