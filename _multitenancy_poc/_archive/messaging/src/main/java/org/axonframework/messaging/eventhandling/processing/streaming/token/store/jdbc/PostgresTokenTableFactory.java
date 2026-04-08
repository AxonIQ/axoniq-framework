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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc;

/**
 * Jdbc token entry table factory for Postgres databases.
 *
 * @author Rene de Waele
 */
public class PostgresTokenTableFactory extends GenericTokenTableFactory {

    /**
     * Creates a singleton reference the PostgresTokenTableFactory implementation.
     */
    public static final PostgresTokenTableFactory INSTANCE = new PostgresTokenTableFactory();

    protected PostgresTokenTableFactory() {
        super();
    }

    @Override
    protected String tokenType() {
        return "bytea";
    }
}
