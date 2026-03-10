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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.engine.association;


import jakarta.annotation.Nonnull;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Comparison operator registry.
 *
 * @author Simon Zambrovski
 * @since 1.0.0
 */
public class ValueComparisonOperatorRegistry {

    private final Map<String, ValueComparisonOperator> operators = new ConcurrentHashMap<>();

    public ValueComparisonOperatorRegistry() {
        load();
    }

    /**
     * Performs service loading of operators.
     */
    public void load() {
        ServiceLoader<ValueComparisonOperator> loader = ServiceLoader.load(
                ValueComparisonOperator.class, getClass().getClassLoader()
        );
        loader.stream().forEach(p -> {
            var operator = p.get();
            operators.put(operator.name(), operator);
        });
    }

    /**
     * Retrieves a value comparison for given operator name.
     *
     * @param name operator name.
     * @return operator or throws exception if operator does not exist.
     */
    @Nonnull
    public ValueComparisonOperator get(@Nonnull String name) {
        Objects.requireNonNull(name, "Operator name must not be null.");
        if (!operators.containsKey(name)) {
            throw new IllegalArgumentException("Unknown operator used " + name);
        }
        return operators.get(name);
    }

    /**
     * Returns all operators of this registry.
     *
     * @return set of operators.
     */
    public Set<String> getOperatorNames() {
        return operators.keySet();
    }

}
