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


/**
 * Provides a repository for given aggregate type.
 *
 * @author Milan Savic
 * @since 3.3
 */
@FunctionalInterface
public interface RepositoryProvider {

    /**
     * Provides a repository for given aggregate type.
     *
     * @param aggregateType type of the aggregate
     * @param <T>           type of the aggregate
     * @return repository given for aggregate type
     */
    <T> Repository<T> repositoryFor(Class<T> aggregateType);
}
