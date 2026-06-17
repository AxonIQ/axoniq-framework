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

import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantResolverRegistry;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;

/**
 * A registry of {@link TenantResolver TenantResolvers} for the multi-tenancy extension.
 * <p>
 * Provides operations to register a general {@link Message} resolver, as well as
 * {@link CommandMessage}-specific, {@link EventMessage}-specific, and {@link QueryMessage}-specific
 * resolvers. Type-specific resolvers take precedence over the general resolver when retrieving
 * via {@link #commandResolver(Configuration)}, {@link #eventResolver(Configuration)}, or
 * {@link #queryResolver(Configuration)}.
 * <p>
 * The general resolver registered via {@link #registerResolver(ComponentBuilder)} serves as a
 * fallback for all message types and can be retrieved directly via {@link #resolver(Configuration)}.
 *
 * @author Theo Emanuelsson
 * @since 5.2.0
 * @see TenantResolver
 * @see DefaultTenantResolverRegistry
 */
public interface TenantResolverRegistry {

    /**
     * Registers the given {@code resolverBuilder} as the general {@link TenantResolver} for all message types.
     * <p>
     * This resolver is used as a fallback when no type-specific resolver is registered for a given message type.
     * Registering a resolver via a {@link ComponentBuilder} ensures it is only built <b>once</b>.
     *
     * @param resolverBuilder the general {@link TenantResolver} builder to register
     * @return this registry, for fluent interfacing
     */
    TenantResolverRegistry registerResolver(
            ComponentBuilder<TenantResolver<Message>> resolverBuilder
    );

    /**
     * Registers the given {@code resolverBuilder} as the {@link CommandMessage}-specific {@link TenantResolver}.
     * <p>
     * When set, this resolver takes precedence over the general resolver for command messages.
     *
     * @param resolverBuilder the {@link CommandMessage}-specific {@link TenantResolver} builder to register
     * @return this registry, for fluent interfacing
     */
    TenantResolverRegistry registerCommandResolver(
            ComponentBuilder<TenantResolver<? super CommandMessage>> resolverBuilder
    );

    /**
     * Registers the given {@code resolverBuilder} as the {@link EventMessage}-specific {@link TenantResolver}.
     * <p>
     * When set, this resolver takes precedence over the general resolver for event messages.
     *
     * @param resolverBuilder the {@link EventMessage}-specific {@link TenantResolver} builder to register
     * @return this registry, for fluent interfacing
     */
    TenantResolverRegistry registerEventResolver(
            ComponentBuilder<TenantResolver<? super EventMessage>> resolverBuilder
    );

    /**
     * Registers the given {@code resolverBuilder} as the {@link QueryMessage}-specific {@link TenantResolver}.
     * <p>
     * When set, this resolver takes precedence over the general resolver for query messages.
     *
     * @param resolverBuilder the {@link QueryMessage}-specific {@link TenantResolver} builder to register
     * @return this registry, for fluent interfacing
     */
    TenantResolverRegistry registerQueryResolver(
            ComponentBuilder<TenantResolver<? super QueryMessage>> resolverBuilder
    );

    /**
     * Returns the {@link TenantResolver} for {@link CommandMessage CommandMessages}.
     * <p>
     * Returns the command-specific resolver if one was registered via
     * {@link #registerCommandResolver(ComponentBuilder)}, otherwise falls back to the general resolver.
     *
     * @param config the {@link Configuration} to build the resolver with
     * @return the command tenant resolver, or {@code null} if none is configured
     */
    @Nullable
    TenantResolver<Message> commandResolver(Configuration config);

    /**
     * Returns the {@link TenantResolver} for {@link EventMessage EventMessages}.
     * <p>
     * Returns the event-specific resolver if one was registered via
     * {@link #registerEventResolver(ComponentBuilder)}, otherwise falls back to the general resolver.
     *
     * @param config the {@link Configuration} to build the resolver with
     * @return the event tenant resolver, or {@code null} if none is configured
     */
    @Nullable
    TenantResolver<Message> eventResolver(Configuration config);

    /**
     * Returns the {@link TenantResolver} for {@link QueryMessage QueryMessages}.
     * <p>
     * Returns the query-specific resolver if one was registered via
     * {@link #registerQueryResolver(ComponentBuilder)}, otherwise falls back to the general resolver.
     *
     * @param config the {@link Configuration} to build the resolver with
     * @return the query tenant resolver, or {@code null} if none is configured
     */
    @Nullable
    TenantResolver<Message> queryResolver(Configuration config);

    /**
     * Returns the general {@link TenantResolver} registered via {@link #registerResolver(ComponentBuilder)}.
     * <p>
     * This method does <b>not</b> fall back to type-specific resolvers. It returns the general resolver
     * directly, which is useful for components that need a resolver without message-type specificity
     * (e.g., parameter resolver factories that extract tenant from any message).
     *
     * @param config the {@link Configuration} to build the resolver with
     * @return the general tenant resolver, or {@code null} if none is configured
     */
    @Nullable
    TenantResolver<Message> resolver(Configuration config);

    /**
     * Returns whether any resolver has been registered in this registry.
     *
     * @return {@code true} if at least one resolver (general or type-specific) has been registered
     */
    boolean hasResolver();
}
