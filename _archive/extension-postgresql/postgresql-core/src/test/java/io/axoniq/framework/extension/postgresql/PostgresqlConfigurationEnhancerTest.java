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

package io.axoniq.framework.extension.postgresql;

import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link PostgresqlConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 */
class PostgresqlConfigurationEnhancerTest {

    private PostgresqlConfigurationEnhancer testSubject;

    @BeforeEach
    void setUp() {
        testSubject = new PostgresqlConfigurationEnhancer();
    }

    @Test
    void orderIsMinValuePlusTwenty() {
        assertThat(testSubject.order()).isEqualTo(Integer.MIN_VALUE + 20);
    }

    @Test
    void enhanceRegistersEventStorageEngine() throws SQLException {
        Statement mockedStatement = mock(Statement.class);
        Connection mockedConnection = mock(Connection.class);
        DataSource mockedDataSource = mock(DataSource.class);
        when(mockedDataSource.getConnection()).thenReturn(mockedConnection);
        when(mockedConnection.createStatement()).thenReturn(mockedStatement);

        Configuration result = EventSourcingConfigurer.create()
                                                      .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                                      .componentRegistry(cr -> cr.registerComponent(
                                                              DataSource.class, c -> mockedDataSource
                                                      ))
                                                      .componentRegistry(cr -> cr.registerComponent(
                                                              EntitlementManager.class, c -> mock(EntitlementManager.class)
                                                      ))
                                                      .componentRegistry(cr -> testSubject.enhance(cr))
                                                      .build();

        assertThat(result.getComponent(EventStorageEngine.class)).isInstanceOf(PostgresqlEventStorageEngine.class);
    }
}