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

package org.axonframework.messaging.core.unitofwork;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Factory for creating {@link UnitOfWork} instances. Useful to create units of work that are bound to some context,
 * such as a database transaction.
 *
 * @author Mateusz Nowak
 * @since 5.0.0
 */
@FunctionalInterface
public interface UnitOfWorkFactory {

    /**
     * Creates a new {@link UnitOfWork} with a randomly generated identifier.
     *
     * @return A new {@link UnitOfWork} instance.
     */
        default UnitOfWork create() {
        return create(UUID.randomUUID().toString(), UnaryOperator.identity());
    }

    /**
     * Creates a new {@link UnitOfWork} with the given identifier.
     *
     * @param identifier The identifier for the unit of work.
     * @return A new {@link UnitOfWork} instance.
     */
        default UnitOfWork create(String identifier) {
        return create(identifier, UnaryOperator.identity());
    }

    /**
     * Creates a new {@link UnitOfWork} with the given identifier and applies the provided customization to its
     * configuration.
     *
     * @param identifier    The identifier for the unit of work.
     * @param customization A function to customize the unit of work's configuration.
     * @return A new {@link UnitOfWork} instance.
     */
    UnitOfWork create(String identifier,
                      Function<UnitOfWorkConfiguration, UnitOfWorkConfiguration> customization);

    /**
     * Creates a new {@link UnitOfWork} with a random identifier and applies the provided customization to its
     * configuration.
     *
     * @param customization A function to customize the unit of work's configuration.
     * @return A new {@link UnitOfWork} instance.
     */
        default UnitOfWork create(Function<UnitOfWorkConfiguration, UnitOfWorkConfiguration> customization) {
        return create(UUID.randomUUID().toString(), customization);
    }
}
