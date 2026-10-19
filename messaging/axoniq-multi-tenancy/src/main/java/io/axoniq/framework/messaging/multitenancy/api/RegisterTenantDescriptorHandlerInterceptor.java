/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.api;

import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A {@link MessageHandlerInterceptor} that registers the {@link TenantDescriptor} of the handled {@link Message} in the
 * {@link ProcessingContext}, so components further down the handling chain route to that tenant without inspecting the
 * message again.
 * <p>
 * The tenant is decided by the shared {@link TenantRouter}, the same way every other tenant-routing component decides
 * it. When the router cannot attribute the message to a known tenant, the interceptor logs a warning and proceeds
 * without registering a {@link TenantDescriptor}.
 *
 * @author Jan Galinski
 * @author Laura Devriendt
 * @since 5.3.0
 */
@Internal
public class RegisterTenantDescriptorHandlerInterceptor implements MessageHandlerInterceptor<Message> {

    private static final Logger logger = LoggerFactory.getLogger(RegisterTenantDescriptorHandlerInterceptor.class);

    private final TenantRouter tenantRouter;

    /**
     * Constructs a {@code RegisterTenantDescriptorHandlerInterceptor} registering the tenant decided by the given
     * {@code tenantRouter} onto the {@link ProcessingContext} of the handled {@link Message}.
     *
     * @param tenantRouter the router deciding which tenant the handled {@link Message} belongs to
     */
    public RegisterTenantDescriptorHandlerInterceptor(TenantRouter tenantRouter) {
        this.tenantRouter = requireNonNull(tenantRouter, "tenantRouter must not be null");
    }

    @Override
    public MessageStream<?> interceptOnHandle(Message message,
                                              ProcessingContext context,
                                              MessageHandlerInterceptorChain<Message> interceptorChain) {
        if (message instanceof CommandMessage || message instanceof QueryMessage) {
            Optional<TenantDescriptor> tenant = tenantRouter.resolveFromMessage(message);
            if (tenant.isPresent()) {
                ProcessingContext tenantContext = context.withResource(TenantDescriptor.RESOURCE_KEY, tenant.get());
                return interceptorChain.proceed(message, tenantContext);
            }
            logger.warn("Tenant could not be resolved for message: {}. Proceeding without tenant context.", message);
        }

        return interceptorChain.proceed(message, context);
    }
}
