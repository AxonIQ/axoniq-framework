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

import org.axonframework.common.annotation.Internal;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface towards a mechanism that provides access to a JDBC Connection.
 *
 * @author Allard Buijze
 * @since 2.2
 */
@Internal
@FunctionalInterface
public interface ConnectionProvider {

    /**
     * Returns a connection, ready for use.
     *
     * @return a new connection to use
     *
     * @throws SQLException when an error occurs obtaining the connection
     */
    Connection getConnection() throws SQLException;
}
