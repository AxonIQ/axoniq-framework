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

package org.axonframework.modelling.command;

import org.axonframework.common.ReflectionUtils;

import org.jspecify.annotations.Nullable;

/**
 * Default implementation of {@link CreationPolicyAggregateFactory} that invokes the default, no-arguments constructor
 * of the aggregate class {@code A}.
 *
 * @param <A> The aggregate type this factory constructs.
 * @author Stefan Andjelkovic
 * @since 4.6.0
 */
public class NoArgumentConstructorCreationPolicyAggregateFactory<A> implements CreationPolicyAggregateFactory<A> {

    private final Class<? extends A> aggregateClass;

    /**
     * Construct an instance of the {@link NoArgumentConstructorCreationPolicyAggregateFactory} for the given type.
     *
     * @param aggregateClass The aggregate type.
     */
    public NoArgumentConstructorCreationPolicyAggregateFactory(Class<? extends A> aggregateClass) {
        this.aggregateClass = aggregateClass;
    }

    /**
     * Creates the aggregate instance based on the previously provided type. Invokes the default, no-argument
     * constructor of the class.
     *
     * @param identifier The identifier to create the aggregate with. Not used by this factory.
     * @return An aggregate instance.
     */
    @SuppressWarnings("deprecation") // Suppressed ReflectionUtils#ensureAccessible
    @Override
    public A create(@Nullable Object identifier) {
        try {
            return ReflectionUtils.ensureAccessible(aggregateClass.getDeclaredConstructor()).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
