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

package org.axonframework.extension.spring.util;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.*;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.MySQLContainer;

import javax.sql.DataSource;

/**
 * Junit 5 extension which fires up a Mysql test container before the test class. After all tests have run, it will tear
 * it down.
 * <p>
 * If you want behavior that creates a container before every individual test, this extension does not support that.
 */
public class MysqlTestContainerExtension extends MySQLContainer<MysqlTestContainerExtension>
        implements BeforeAllCallback, AfterAllCallback {

    private static MysqlTestContainerExtension container;

    public MysqlTestContainerExtension() {
        super("mysql:8.0");
    }

    public static MysqlTestContainerExtension getInstance() {
        if (container == null) {
            container = new MysqlTestContainerExtension();
        }
        return container;
    }

    public DataSource asDataSource() {
        DriverManagerDataSource driverManagerDataSource = new DriverManagerDataSource(container.getJdbcUrl(),
                                                                                      container.getUsername(),
                                                                                      container.getPassword());
        driverManagerDataSource.setDriverClassName(container.getDriverClassName());
        return driverManagerDataSource;
    }

    @Override
    public void beforeAll(@NonNull ExtensionContext extensionContext) throws Exception {
        MysqlTestContainerExtension.getInstance().start();
    }

    @Override
    public void afterAll(@NonNull ExtensionContext extensionContext) throws Exception {
        MysqlTestContainerExtension.getInstance().stop();
    }
}
