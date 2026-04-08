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
 * Enumeration containing the possible creation policies for aggregates.
 *
 * @author Marc Gathier
 * @since 4.3
 */
public enum AggregateCreationPolicy {
    /**
     * Always create a new instance of the aggregate on invoking the method. Fail if already exists.
     */
    ALWAYS,
    /**
     * Create a new instance of the aggregate when it is not found.
     */
    CREATE_IF_MISSING,
    /**
     * Expect instance of the aggregate to exist. Fail if missing.
     */
    NEVER
}
