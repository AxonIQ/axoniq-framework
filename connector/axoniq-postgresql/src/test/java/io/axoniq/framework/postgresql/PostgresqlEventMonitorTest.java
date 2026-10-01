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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.common.Registration;
import org.axonframework.messaging.core.MessageStream;
import org.junit.jupiter.api.*;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Test class validating the {@code PostgresqlEventMonitor}.
 *
 * @author John Hendrikx
 */
class PostgresqlEventMonitorTest {

    private static PostgreSQLContainer postgresContainer;
    private static HikariDataSource dataSource;

    private PostgresqlEventMonitor testSubject;

    @BeforeAll
    @SuppressWarnings("resource")
    static void startContainer() {
        postgresContainer = new PostgreSQLContainer("postgres:16.2-alpine")
                .withDatabaseName("testdb")
                .withUsername("test")
                .withPassword("test");
        postgresContainer.start();

        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(postgresContainer.getJdbcUrl());
        config.setUsername(postgresContainer.getUsername());
        config.setPassword(postgresContainer.getPassword());
        config.setAutoCommit(true);

        dataSource = new HikariDataSource(config);

        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE events (global_index INT8 PRIMARY KEY)");
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterAll
    static void stopContainer() {
        dataSource.close();

        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }

    @BeforeEach
    void createMonitor() throws SQLException {
        try (
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()
        ) {
            statement.execute("DELETE FROM events");
        }

        testSubject = new PostgresqlEventMonitor(dataSource);
    }

    @AfterEach
    void closeMonitor() {
        testSubject.close();
    }

    @Nested
    class CallbackNotification {

        @Test
        void updateHighestKnownGlobalIndex_withHigherIndex_notifiesRegisteredCallback() {
            AtomicInteger callCount = new AtomicInteger();
            testSubject.registerCallback(dummyStream(), callCount::incrementAndGet);

            testSubject.updateHighestKnownGlobalIndex(5);

            assertThat(callCount.get()).isEqualTo(1);
        }

        @Test
        void updateHighestKnownGlobalIndex_withSameOrLowerIndex_doesNotNotify() {
            testSubject.updateHighestKnownGlobalIndex(5);

            AtomicInteger callCount = new AtomicInteger();

            testSubject.registerCallback(dummyStream(), callCount::incrementAndGet);

            testSubject.updateHighestKnownGlobalIndex(5);
            testSubject.updateHighestKnownGlobalIndex(3);

            assertThat(callCount.get()).isZero();
        }

        @Test
        void updateHighestKnownGlobalIndex_notifiesAllRegisteredCallbacks() {
            AtomicInteger callCount1 = new AtomicInteger();
            AtomicInteger callCount2 = new AtomicInteger();

            testSubject.registerCallback(dummyStream(), callCount1::incrementAndGet);
            testSubject.registerCallback(dummyStream(), callCount2::incrementAndGet);

            testSubject.updateHighestKnownGlobalIndex(1);

            assertThat(callCount1.get()).isEqualTo(1);
            assertThat(callCount2.get()).isEqualTo(1);
        }

        @Test
        void cancellingRegistration_stopsFurtherNotifications() {
            AtomicInteger callCount = new AtomicInteger();
            Registration registration = testSubject.registerCallback(dummyStream(), callCount::incrementAndGet);

            boolean cancelled = registration.cancel();

            testSubject.updateHighestKnownGlobalIndex(10);

            assertThat(cancelled).isTrue();
            assertThat(callCount.get()).isZero();
        }
    }

    @Nested
    class RealPostgresNotifications {

        @Test
        void notifyFromAnotherConnection_eventuallyNotifiesRegisteredCallback() {
            AtomicInteger callCount = new AtomicInteger();

            testSubject.registerCallback(dummyStream(), callCount::incrementAndGet);

            // Retries the notification until the monitor's background thread has established its
            // LISTEN - a notification sent before that happens is not queued, so a single attempt
            // would be flaky.
            await().untilAsserted(() -> {
                sendNotification(1);

                assertThat(callCount.get()).isGreaterThan(0);
            });
        }

        private void sendNotification(long globalIndex) throws SQLException {
            try (
                Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()
            ) {
                statement.execute("SELECT pg_notify('events_channel', '" + globalIndex + "')");
            }
        }
    }

    private static MessageStream<?> dummyStream() {
        return MessageStream.fromIterable(List.of());
    }
}
