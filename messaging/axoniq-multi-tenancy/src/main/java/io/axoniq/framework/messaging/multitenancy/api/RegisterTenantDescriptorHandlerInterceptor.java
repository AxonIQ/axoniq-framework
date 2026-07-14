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
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Collections;

/**
 * A {@link MessageHandlerInterceptor} that registers a {@link TenantDescriptor} in the {@link ProcessingContext}
 *
 * @param tenantResolver the {@link TenantResolver} to resolve the {@link TenantDescriptor} from the {@link Message}
 * @param tenantDescriptors optional list of known {@link TenantDescriptor}s to resolve the {@link TenantDescriptor} from the {@link Message}
 *
 * @author Jan Galinski
 * @since 5.3.0
 */
@Internal
public record RegisterTenantDescriptorHandlerInterceptor(
        TenantResolver<Message> tenantResolver,
        TenantDescriptors tenantDescriptors
) implements MessageHandlerInterceptor<Message> {

    /**
     * A {@code RegisterTenantDescriptorHandlerInterceptor} that registers a {@link TenantDescriptor}
     * in the {@link ProcessingContext} using the given {@code tenantResolver}.
     * Convenience constructor that initializes the {@code tenantDescriptors} to an empty list.
     *
     * @param tenantResolver the {@link TenantResolver} to resolve the {@link TenantDescriptor} from the {@link Message}
     */
    public RegisterTenantDescriptorHandlerInterceptor(TenantResolver<Message> tenantResolver) {
        this(tenantResolver, Collections::emptyList);
    }

    @Override
    public MessageStream<?> interceptOnHandle(Message message,
                                              ProcessingContext context,
                                              MessageHandlerInterceptorChain<Message> interceptorChain) {
        return interceptorChain.proceed(message, context.withResource(
                MultiTenancyApiUtils.TENANT_RESOURCE_KEY,
                tenantResolver.resolveTenant(message, tenantDescriptors.tenants())
        ));
    }
}
