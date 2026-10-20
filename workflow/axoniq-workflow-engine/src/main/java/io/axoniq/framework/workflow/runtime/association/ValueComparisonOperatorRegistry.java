/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.association;


import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Comparison operator registry.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class ValueComparisonOperatorRegistry {

    private final Map<String, ValueComparisonOperator> operators = new ConcurrentHashMap<>();

    /**
     * Constructs a new registry and loads operators.
     */
    public ValueComparisonOperatorRegistry() {
        this(true);
    }

    /**
     * Constructs a new registry.
     *
     * @param loadOperators flag indicating if the Service loading of operators should be performed.
     */
    public ValueComparisonOperatorRegistry(boolean loadOperators) {
        if (loadOperators) {
            load();
        }
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
     * Retrieves a value comparison for a given operator name.
     *
     * @param name operator name.
     * @return operator or throws exception if an operator does not exist.
     */
    public ValueComparisonOperator get(String name) {
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
