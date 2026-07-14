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
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;

/**
 * Default implementation of {@link TenantResolverRegistry} that stores {@link ComponentBuilder} references
 * for a general and per-message-type {@link TenantResolver}.
 * <p>
 * Type-specific resolvers (command, event, query) take precedence over the general resolver.
 * When a type-specific resolver is not configured, the general resolver is returned as a fallback.
 * <p>
 * Each resolver is built once, on the first accessor call that needs it, and the same instance is returned on every
 * subsequent call. Registering a new builder replaces the previously built resolver of that bucket.
 * <p>
 * Internal, because users obtain this registry through {@link TenantResolverRegistry#create()} rather than
 * constructing it directly.
 *
 * @author Theo Emanuelsson
 * @since 5.3.0
 * @see TenantResolverRegistry
 */
@Internal
// Todo: Do we really need message type specific resolvers? Or is a single resolver enough?
class DefaultTenantResolverRegistry implements TenantResolverRegistry {

    private @Nullable ComponentBuilder<TenantResolver<Message>> generalBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super CommandMessage>> commandBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super EventMessage>> eventBuilder;
    private @Nullable ComponentBuilder<TenantResolver<? super QueryMessage>> queryBuilder;

    private @Nullable TenantResolver<Message> builtGeneralResolver;
    private @Nullable TenantResolver<Message> builtCommandResolver;
    private @Nullable TenantResolver<Message> builtEventResolver;
    private @Nullable TenantResolver<Message> builtQueryResolver;

    @Override
    public synchronized TenantResolverRegistry registerResolver(
            ComponentBuilder<TenantResolver<Message>> resolverBuilder
    ) {
        this.generalBuilder = resolverBuilder;
        this.builtGeneralResolver = null;
        return this;
    }

    @Override
    public synchronized TenantResolverRegistry registerCommandResolver(
            ComponentBuilder<TenantResolver<? super CommandMessage>> resolverBuilder
    ) {
        this.commandBuilder = resolverBuilder;
        this.builtCommandResolver = null;
        return this;
    }

    @Override
    public synchronized TenantResolverRegistry registerEventResolver(
            ComponentBuilder<TenantResolver<? super EventMessage>> resolverBuilder
    ) {
        this.eventBuilder = resolverBuilder;
        this.builtEventResolver = null;
        return this;
    }

    @Override
    public synchronized TenantResolverRegistry registerQueryResolver(
            ComponentBuilder<TenantResolver<? super QueryMessage>> resolverBuilder
    ) {
        this.queryBuilder = resolverBuilder;
        this.builtQueryResolver = null;
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized @Nullable TenantResolver<Message> commandResolver(Configuration config) {
        if (commandBuilder != null) {
            if (builtCommandResolver == null) {
                builtCommandResolver = (TenantResolver<Message>) (TenantResolver<?>) commandBuilder.build(config);
            }
            return builtCommandResolver;
        }
        return resolver(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized @Nullable TenantResolver<Message> eventResolver(Configuration config) {
        if (eventBuilder != null) {
            if (builtEventResolver == null) {
                builtEventResolver = (TenantResolver<Message>) (TenantResolver<?>) eventBuilder.build(config);
            }
            return builtEventResolver;
        }
        return resolver(config);
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized @Nullable TenantResolver<Message> queryResolver(Configuration config) {
        if (queryBuilder != null) {
            if (builtQueryResolver == null) {
                builtQueryResolver = (TenantResolver<Message>) (TenantResolver<?>) queryBuilder.build(config);
            }
            return builtQueryResolver;
        }
        return resolver(config);
    }

    @Override
    public synchronized @Nullable TenantResolver<Message> resolver(Configuration config) {
        if (generalBuilder == null) {
            return null;
        }
        if (builtGeneralResolver == null) {
            builtGeneralResolver = generalBuilder.build(config);
        }
        return builtGeneralResolver;
    }

    @Override
    public synchronized boolean hasResolver() {
        return generalBuilder != null
                || commandBuilder != null
                || eventBuilder != null
                || queryBuilder != null;
    }
}
