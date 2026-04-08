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

package org.axonframework.modelling.command.inspection;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Specialized EntityModel that describes the capabilities and properties of an aggregate root of type {@code T}.
 *
 * @param <T> The type of the aggregate root
 */
public interface AggregateModel<T> extends EntityModel<T> {

    /**
     * Get the String representation of the modeled aggregate's type. This defaults to the simple name of the
     * aggregate's type.
     *
     * @return The type of the aggregate
     */
    String type();

    /**
     * Get the current version number of the given {@code aggregate}. For event sourced aggregates this is identical to
     * the sequence number of the last applied event.
     *
     * @param target The target aggregate root instance
     * @return The current version of the aggregate
     * @deprecated Aggregate versioning will completely be removed from 5.0.0, as it does not align with the adjusted
     * sequencing approach of a DCB-supporting event store at the moment.
     */
    @Deprecated(since = "5.0.0", forRemoval = true)
    Long getVersion(T target);

    /**
     * Gets the aggregate class based on given {@code declaredType}.
     *
     * @param declaredType the declared type of aggregate represented by {@link String}
     * @return the concrete aggregate class based on {@code declaredType}, if exists
     */
    default Optional<Class<?>> type(String declaredType) {
        return Optional.empty();
    }

    /**
     * Gets the declared aggregate type based on given class {@code type}.
     *
     * @param type the type of the aggregate
     * @return the declared aggregate type represented by {@link String}, if exists
     */
    default Optional<String> declaredType(Class<?> type) {
        return Optional.empty();
    }

    /**
     * Gets all types (concrete aggregate classes) of this aggregate.
     *
     * @return all types (concrete aggregate classes) of this aggregate
     */
    default Stream<Class<?>> types() {
        return Stream.empty();
    }
}
