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

package org.axonframework.common.jdbc;

/**
 * The PersistenceExceptionResolver is used to find out if an exception is caused by  duplicate keys.
 *
 * @author Martin Tilma
 * @since 2.2
 */
public interface PersistenceExceptionResolver {

    /**
     * Indicates whether the given {@code exception} represents a duplicate key violation. Typically, duplicate key
     * violations indicates concurrent access to an entity in the application. Two users might be accessing the same
     * Aggregate, for example.
     *
     * @param exception The exception to evaluate
     * @return {@code true} if the given exception represents a Duplicate Key Violation, {@code false}
     *         otherwise.
     */
    boolean isDuplicateKeyViolation(Exception exception);
}
