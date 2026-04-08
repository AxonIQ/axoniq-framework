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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionProvider implementation that obtains a connection from a given DataSource.
 *
 * @author Allard Buijze
 * @since 2.2
 */
public class DataSourceConnectionProvider implements ConnectionProvider {

    private final DataSource dataSource;

    /**
     * Initialize the Connection Provider, using given {@code dataSource} to obtain new connections.
     *
     * @param dataSource The DataSource to obtain connections from
     */
    public DataSourceConnectionProvider(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
