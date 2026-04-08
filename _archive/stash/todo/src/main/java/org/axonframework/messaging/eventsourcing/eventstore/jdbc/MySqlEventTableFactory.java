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

package org.axonframework.messaging.eventsourcing.eventstore.jdbc;

/**
 * Jdbc event entry table factory for MySql databases.
 *
 * @author Rene de Waele
 * @since 3.0
 */
public class MySqlEventTableFactory extends AbstractEventTableFactory {

    /**
     * Singleton MySqlEventTableFactory instance
     */
    public static final MySqlEventTableFactory INSTANCE = new MySqlEventTableFactory();

    @Override
    protected String idColumnType() {
        return "BIGINT AUTO_INCREMENT";
    }

    @Override
    protected String payloadType() {
        return "blob";
    }
}
