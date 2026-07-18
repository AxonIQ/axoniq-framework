/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.postgresql;

import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine;
import org.axonframework.eventsourcing.snapshot.store.SnapshotStore;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link PostgresqlConfigurationEnhancer}.
 *
 * @author Steven van Beelen
 * @author John Hendrikx
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
        ResultSet mockedResultSet = mock(ResultSet.class);
        when(mockedDataSource.getConnection()).thenReturn(mockedConnection);
        when(mockedConnection.createStatement()).thenReturn(mockedStatement);
        when(mockedStatement.executeQuery(anyString())).thenReturn(mockedResultSet);

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

    @Test
    void enhanceRegistersSnapshotStoreAsTheSameInstanceAsTheEngine() throws SQLException {
        Statement mockedStatement = mock(Statement.class);
        Connection mockedConnection = mock(Connection.class);
        DataSource mockedDataSource = mock(DataSource.class);
        ResultSet mockedResultSet = mock(ResultSet.class);
        when(mockedDataSource.getConnection()).thenReturn(mockedConnection);
        when(mockedConnection.createStatement()).thenReturn(mockedStatement);
        when(mockedStatement.executeQuery(anyString())).thenReturn(mockedResultSet);

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

        // when
        EventStorageEngine engine = result.getComponent(EventStorageEngine.class);
        SnapshotStore snapshotStore = result.getComponent(SnapshotStore.class);

        // then
        assertThat(snapshotStore).isSameAs(engine);
    }

    @Test
    void enhance_whenEngineAlreadyRegistered_doesNotRegisterSnapshotStoreAndDoesNotThrow() {
        // given — a pre-existing engine that does not implement SnapshotStore
        EventStorageEngine preExistingEngine = mock(EventStorageEngine.class);
        boolean[] snapshotStorePresent = {true};

        // when / then — must not throw, and must not have registered a SnapshotStore
        assertThatNoException().isThrownBy(() ->
            EventSourcingConfigurer.create()
                                   .componentRegistry(ComponentRegistry::disableEnhancerScanning)
                                   .componentRegistry(cr -> cr.registerComponent(
                                           EventStorageEngine.class, c -> preExistingEngine
                                   ))
                                   .componentRegistry(cr -> {
                                       testSubject.enhance(cr);
                                       snapshotStorePresent[0] = cr.hasComponent(SnapshotStore.class);
                                   })
                                   .build()
        );

        assertThat(snapshotStorePresent[0]).isFalse();
    }
}