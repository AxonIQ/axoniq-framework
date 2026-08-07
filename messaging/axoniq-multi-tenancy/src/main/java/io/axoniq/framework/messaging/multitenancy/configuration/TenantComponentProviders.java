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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentLookup;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Finds {@link TenantComponentProvider TenantComponentProviders} registered in a configuration hierarchy.
 * <p>
 * Internal because providers are application components while this class only implements the framework's discovery
 * rules for infrastructure that consumes them.
 *
 * @author Jan Galinski
 * @since 5.3.1
 */
@Internal
public final class TenantComponentProviders {

    private TenantComponentProviders() {
        // Utility class
    }

    /**
     * Returns every tenant-component provider visible from the given configuration.
     *
     * @param configuration the configuration from which to resolve the root configuration
     * @return every provider registered in the root configuration and its modules
     */
    @SuppressWarnings("rawtypes")
    public static Iterable<TenantComponentProvider> all(Configuration configuration) {
        return rootConfiguration(configuration).getComponents(TenantComponentProvider.class).values();
    }

    /**
     * Finds the provider registered for the given component type.
     *
     * @param configuration the configuration from which to resolve providers
     * @param componentType the component type of the provider to find
     * @param <T>           the component type
     * @return the provider for the component type, if one is registered
     * @throws AxonConfigurationException if more than one provider exposes the component type
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static <T> Optional<TenantComponentProvider<T>> find(Configuration configuration, Class<T> componentType) {
        List<TenantComponentProvider> matches = new ArrayList<>();
        for (TenantComponentProvider provider : all(configuration)) {
            if (componentType.equals(provider.componentType())) {
                matches.add(provider);
            }
        }
        if (matches.size() > 1) {
            throw new AxonConfigurationException("Multiple TenantComponentProviders match component type ["
                                                         + componentType.getName()
                                                         + "]. Register a single provider for this type.");
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
    }

    /**
     * Returns a {@link TenantComponentLookup} that always returns the given {@code defaultComponent}, ignoring the
     * requested tenant.
     *
     * @param defaultComponent the default component instance to return for any tenant
     * @param <T>              the type of the component
     * @return a {@link TenantComponentLookup} that always returns the given {@code defaultComponent}
     */
    public static <T> TenantComponentLookup<T> defaultTenantComponentLookup(T defaultComponent) {
        T nonNullDefaultComponent = requireNonNull(defaultComponent, "The defaultComponent must not be null.");
        return unused -> nonNullDefaultComponent;
    }

    /**
     * Returns a {@link TenantComponentLookup} that uses the given {@code lookupFn} to resolve the component for a
     * tenant.
     *
     * @param lookupFn the function to resolve the component for a tenant
     * @param <T>      the type of the component
     * @return a {@link TenantComponentLookup} that uses the given {@code lookupFn} to resolve the component for a
     * tenant
     */
    public static <T> TenantComponentLookup<T> tenantComponentLookup(Function<TenantDescriptor, T> lookupFn) {
        requireNonNull(lookupFn, "The lookupFn must not be null.");
        return lookupFn::apply;
    }

    private static Configuration rootConfiguration(Configuration configuration) {
        Configuration root = configuration;
        Configuration parent;
        while ((parent = root.getParent()) != null) {
            root = parent;
        }
        return root;
    }
}
