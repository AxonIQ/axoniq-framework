/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.common.configuration;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;

import java.util.Collections;
import java.util.Objects;

/**
 * Implementation of {@link Component} and {@link ComponentDefinition} that wraps a pre-instantiated component.
 * <p>
 * For internal use only. Instead, use static methods on {@link ComponentDefinition} to instantiate definitions.
 *
 * @param <C> The declared type of the component.
 * @author Allard Buijze
 * @since 5.0.0
 */
@Internal
public class InstantiatedComponentDefinition<C> extends AbstractComponent<C, C> {

    private final C instance;

    /**
     * Create the definition for a component with given {@code identifier} and given {@code instance}.
     *
     * @param identifier The identifier of the component.
     * @param instance   The instance the components resolves to.
     */
    public InstantiatedComponentDefinition(Component.Identifier<C> identifier,
                                           C instance) {
        super(identifier, Collections.emptyList(), Collections.emptyList());
        this.instance = Objects.requireNonNull(instance, "The instance must not be null.");
    }

    @Override
    public C doResolve(Configuration configuration) {
        return instance;
    }

    @Override
    public boolean isInstantiated() {
        return true;
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        super.describeTo(descriptor);
        descriptor.describeProperty("instance", instance);
    }
}
