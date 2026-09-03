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
package io.axoniq.workflow.runtime.execution.payload;

import io.axoniq.workflow.runtime.api.payload.PayloadReducer;

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
