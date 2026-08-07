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

package io.axoniq.framework.messaging.multitenancy.annotation;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.configuration.TenantComponentProviders;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Priority;
import org.axonframework.common.annotation.AnnotationUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException.tenantNotResolved;

/**
 * {@link ParameterResolverFactory} injecting tenant-scoped components into message-handling methods.
 * <p>
 * For each handler parameter it looks for a registered {@link TenantComponentProvider} whose
 * {@link TenantComponentProvider#componentType() component type} fits the parameter type. When one is found, the
 * resolved parameter value is that provider's instance for the tenant of the message being handled. The tenant is
 * derived from the current {@link ProcessingContext} (see {@link io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor}).
 * <p>
 * Registering several providers (one per component type) is supported: each parameter is matched to the provider for
 * its own type. A single exact type match settles the choice, even when assignable supertype candidates exist. When the
 * choice cannot be settled by an exact match, handler inspection fails with an {@link AxonConfigurationException},
 * since silently picking one of the candidates could hand the handler the wrong tenant-scoped resource.
 *
 * @author Theo Emanuelsson
 * @author Jan Galinski
 * @author Laura Devriendt
 * @author Jakob Hatzl
 * @see TenantComponentProvider
 * @since 5.3.0
 */
@Internal
@Priority(Priority.HIGH)
public class TenantComponentParameterResolverFactory implements ParameterResolverFactory {

    private final Configuration configuration;

    /**
     * Constructs a {@code TenantComponentParameterResolverFactory} for the given {@code configuration}.
     * <p>
     * The given {@code configuration} supplies the registered {@link TenantComponentProvider providers} matched against
     * handler parameters. The tenant itself is read from the {@link ProcessingContext} of the message being handled.
     *
     * @param configuration the configuration to look up the registered tenant-component providers, must not be
     *                      {@code null}
     */
    public TenantComponentParameterResolverFactory(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration, "The configuration must not be null");
    }

    @Override
    public @Nullable ParameterResolver<?> createInstance(Executable executable,
                                                         Parameter[] parameters,
                                                         int parameterIndex) {
        if (!AnnotationUtils.isAnnotationPresent(parameters[parameterIndex], TenantScoped.class)) {
            return null;
        }
        Class<?> parameterType = parameters[parameterIndex].getType();
        TenantComponentProvider<?> provider = findProviderFor(parameterType);
        if (provider == null) {
            return null;
        }
        return new TenantComponentParameterResolver(provider);
    }

    private @Nullable TenantComponentProvider<?> findProviderFor(Class<?> parameterType) {
        List<TenantComponentProvider<?>> exactMatches = new ArrayList<>();
        List<TenantComponentProvider<?>> assignableMatches = new ArrayList<>();
        for (TenantComponentProvider<?> provider : TenantComponentProviders.all(configuration)) {
            Class<?> componentType = provider.componentType();
            if (parameterType.equals(componentType)) {
                exactMatches.add(provider);
            } else if (parameterType.isAssignableFrom(componentType)) {
                assignableMatches.add(provider);
            }
        }
        if (exactMatches.size() > 1) {
            throw ambiguousProviders(parameterType, exactMatches);
        }
        if (!exactMatches.isEmpty()) {
            return exactMatches.getFirst();
        }
        if (assignableMatches.size() > 1) {
            throw ambiguousProviders(parameterType, assignableMatches);
        }
        return assignableMatches.isEmpty() ? null : assignableMatches.getFirst();
    }

    private static AxonConfigurationException ambiguousProviders(Class<?> parameterType,
                                                                 List<TenantComponentProvider<?>> candidates) {
        String componentTypes = candidates.stream()
                                          .map(provider -> provider.componentType().getName())
                                          .collect(Collectors.joining(", ", "[", "]"));
        return new AxonConfigurationException(
                "Multiple TenantComponentProviders match parameter type [" + parameterType.getName()
                        + "]: " + componentTypes
                        + ". Register a single provider per component type, or narrow the parameter type."
        );
    }

    /**
     * Resolves a handler parameter to the matched provider's component instance for the tenant of the message in the
     * {@link ProcessingContext}. Matches any message-carrying context: a message without a resolvable or registered
     * tenant fails at resolution time with a {@link TenantNotResolvedException}, rather than silently not matching.
     */
    private record TenantComponentParameterResolver(TenantComponentProvider<?> provider)
            implements ParameterResolver<Object> {

        @Override
        public CompletableFuture<Object> resolveParameterValue(ProcessingContext context) {
            Message message = Message.fromContext(context);
            if (message == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Cannot resolve a tenant-scoped component without a message in the ProcessingContext."
                ));
            }
            try {
                TenantDescriptor tenant = TenantDescriptor.fromContext(context)
                                                          .orElseThrow(tenantNotResolved(
                                                                  "No tenant descriptor found in processing context"));
                Object component = provider.componentFor(tenant);
                return CompletableFuture.completedFuture(component);
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public boolean matches(ProcessingContext context) {
            return Message.fromContext(context) != null;
        }
    }
}
