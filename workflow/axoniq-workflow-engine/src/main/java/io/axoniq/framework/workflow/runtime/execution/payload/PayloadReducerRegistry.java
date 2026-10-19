/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.runtime.execution.payload;

import io.axoniq.framework.workflow.dsl.api.PayloadReducer;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of payload reducers defined by {@link PayloadReducer}.
 *
 * @author Simon Zambrovski
 * @since 5.4.0
 */
public class PayloadReducerRegistry {

    private final Map<String, PayloadReducer> reducers = new ConcurrentHashMap<>();

    /**
     * Constructs a new registry and loads reducers.
     */
    public PayloadReducerRegistry() {
        this(true, PayloadReducerRegistry.class.getClassLoader());
    }

    /**
     * Constructs a new registry and loads reducers.
     *
     * @param loadReducers flag indicating if the Service loading of reducers should be performed.
     * @param classLoader  class loader to use for loading reducers.
     */
    public PayloadReducerRegistry(boolean loadReducers, ClassLoader classLoader) {
        if (loadReducers) {
            load(classLoader);
        }
    }

    /**
     * Loads payload reducers via SPI.
     */
    void load(ClassLoader classLoader) {
        ServiceLoader<PayloadReducer> loader = ServiceLoader.load(
                PayloadReducer.class, classLoader
        );
        loader.stream().forEach(p -> {
            var reducer = p.get();
            reducers.put(reducer.name(), reducer);
        });
    }

    /**
     * Registers new payload reducer.
     *
     * @param reducer reducer to register.
     */
    public void register(PayloadReducer reducer) {
        this.reducers.put(reducer.name(), reducer);
    }

    /**
     * Retrieves a payload reducer by name.
     *
     * @param name name of the reducer.
     * @return payload reducer.
     */
    public Optional<PayloadReducer> get(String name) {
        return Optional.ofNullable(reducers.get(
                Objects.requireNonNull(name, "Reducer name must not be null")
        ));
    }
}
