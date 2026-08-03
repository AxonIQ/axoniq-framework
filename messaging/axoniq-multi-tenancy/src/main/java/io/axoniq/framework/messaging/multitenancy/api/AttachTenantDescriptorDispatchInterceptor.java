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
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.MessageDispatchInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A {@link MessageDispatchInterceptor} that attaches the tenant of the dispatching {@link ProcessingContext} onto a
 * dispatched command or query, so it stays attributable to that tenant once it crosses a distributed command or
 * query bus and arrives in a fresh {@link ProcessingContext} on the receiving side.
 * <p>
 * The tenant is decided by the shared {@link TenantRouter}, the same way every other tenant-routing component
 * decides it. When the dispatching {@code context} carries no tenant, for example because the message is dispatched
 * outside of any handler, this interceptor does nothing.
 * <p>
 * The dispatch-side counterpart of {@link RegisterTenantDescriptorHandlerInterceptor}: that interceptor registers the
 * tenant of a handled message onto the {@link ProcessingContext}; this one attaches that same tenant onto a message
 * dispatched from within that handling, so a receiving component can resolve it without the message having named a
 * tenant of its own.
 *
 * @author Jakob Hatzl
 * @since 5.3.0
 */
@Internal
public class AttachTenantDescriptorDispatchInterceptor implements MessageDispatchInterceptor<Message> {

    private static final Logger logger = LoggerFactory.getLogger(AttachTenantDescriptorDispatchInterceptor.class);

    private final TenantRouter tenantRouter;

    /**
     * Constructs an {@code AttachTenantDescriptorDispatchInterceptor} attaching the tenant decided by the given
     * {@code tenantRouter} onto a dispatched command or query.
     *
     * @param tenantRouter the router deciding which tenant the dispatching {@link ProcessingContext} belongs to
     */
    public AttachTenantDescriptorDispatchInterceptor(TenantRouter tenantRouter) {
        this.tenantRouter = requireNonNull(tenantRouter, "tenantRouter must not be null");
    }

    @Override
    public MessageStream<?> interceptOnDispatch(Message message,
                                                @Nullable ProcessingContext context,
                                                MessageDispatchInterceptorChain<Message> interceptorChain) {
        if (message instanceof CommandMessage || message instanceof QueryMessage) {
            Optional<TenantDescriptor> tenant = tenantRouter.resolveFromContext(context);
            if (tenant.isPresent()) {
                return interceptorChain.proceed(tenantRouter.attachTenant(message, tenant.get()), context);
            }
            logger.warn("Tenant could not be resolved for message: {}. Proceeding without tenant context.", message);
        }
        return interceptorChain.proceed(message, context);
    }
}
