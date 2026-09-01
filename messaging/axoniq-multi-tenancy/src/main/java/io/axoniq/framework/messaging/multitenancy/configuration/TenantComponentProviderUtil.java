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

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.TypeReference;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.AmbiguousComponentMatchException;
import org.axonframework.common.configuration.Component;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Finds {@link TenantComponentProvider TenantComponentProviders} registered in a configuration hierarchy.
 * <p>
 * Internal because providers are application components while this class only implements the framework's discovery
 * rules for infrastructure that consumes them.
 *
 * @author Jan Galinski
 * @since 5.3.1
 */
public final class TenantComponentProviderUtil {

    private TenantComponentProviderUtil() {
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
            throw new AmbiguousComponentMatchException(new Component.Identifier(new TypeReference<TenantComponentProvider<Converter>>() {
            }, null));
        }
        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.getFirst());
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
