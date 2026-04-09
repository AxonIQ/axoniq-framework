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
package io.axoniq.framework.dataprotection.cryptoengine;

import io.axoniq.framework.dataprotection.utils.TestUtils;
import org.hsqldb.jdbc.JDBCDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Test class validating the {@link JdbcCryptoEngine}.
 */
class JDBCTest extends AbstractEngineTestSet {

    private static JdbcCryptoEngine cryptoEngine;

    static {
        JDBCDataSource jdbcDataSource = new JDBCDataSource();
        jdbcDataSource.setURL("jdbc:hsqldb:mem:mydb");
        cryptoEngine = new JdbcCryptoEngine(jdbcDataSource);
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());

        try (Connection connection = jdbcDataSource.getConnection()) {
            connection.prepareStatement(cryptoEngine.getCreateTableStatement()).execute();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected CryptoEngine getCryptoEngine() {
        return cryptoEngine;
    }
}
