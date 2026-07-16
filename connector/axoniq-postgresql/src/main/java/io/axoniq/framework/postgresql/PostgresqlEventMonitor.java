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

import org.axonframework.common.Registration;
import org.axonframework.common.annotation.Internal;
import org.axonframework.messaging.core.MessageStream;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

/**
 * Watches for the arrival of new events appended to the underlying PostgreSQL database via Postgres's
 * {@code LISTEN}/{@code NOTIFY} mechanism, and notifies registered callbacks so streams can resume
 * without waiting for their next scheduled poll.
 * <p>
 * Used internally by {@link PostgresqlEventStorageEngine}, which owns the single instance backing
 * its {@link org.axonframework.eventsourcing.eventstore.EventStorageEngine#stream(
 * org.axonframework.messaging.eventstreaming.StreamingCondition)} method and forwards its own
 * finalization results here too, so callbacks also fire promptly for events finalized by this same
 * process, without waiting on a round trip through Postgres notifications.
 *
 * @author John Hendrikx
 * @since 5.2.0
 */
@Internal
final class PostgresqlEventMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlEventMonitor.class);

    /**
     * Finds the lowest global index strictly greater than the largest known value. Returns
     * a single row with the result.
     */
    private static final String EVENTS_FIND_NEXT_AVAILABLE_GLOBAL_INDEX =
        """
        SELECT COALESCE(MAX(global_index) + 1, 1) FROM events
        """;

    /**
     * Tracks runnables for callbacks attached to streams for when new events may have become available.
     */
    private final Map<Object, Runnable> streamCallbacks = new ConcurrentHashMap<>();

    /**
     * The thread used to monitor for the arrival of new events inserted
     * by another instance of this class running in a different process.
     */
    private final Thread eventMonitoringThread;

    /**
     * Synchronized field (via {@link #streamCallbacks}). Tracks the highest known global index,
     * updated either by this monitor's own notification loop or via {@link #updateHighestKnownGlobalIndex}
     * being called directly by the owning engine after a local finalization.
     */
    private long highestKnownGlobalIndex;

    /**
     * Constructs a new instance, immediately starting its monitoring thread.
     *
     * @param dataSource a data source to connect to PostgreSQL, cannot be {@code null}
     */
    PostgresqlEventMonitor(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource");

        this.eventMonitoringThread = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try (Connection connection = dataSource.getConnection()) {
                    monitorForNewEvents(connection);
                }
                catch (SQLException e) {

                    /*
                     * The Postgres driver will wrap InterruptedExceptions in an SQLException.
                     * To check whether the exception here was meant to terminate the monitoring
                     * thread, the interrupted flag is checked:
                     */

                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }

                    LOGGER.warn("Exception while accessing DataSource (retry in 5 seconds): " + dataSource, e);

                    try {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException ie) {
                        break;  // exit thread when asked to terminate during retry delay
                    }
                }
            }

            LOGGER.info("Event Monitoring Thread terminated");
        });
    }

    void close() {  // for testing purposes, to avoid junk exceptions
        eventMonitoringThread.interrupt();

        try {
            eventMonitoringThread.join(10_000);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Registers a callback to be run whenever new events may have become available.
     *
     * @param messageStream the stream to register the callback for, used as the key for later removal, cannot be {@code null}
     * @param callback the callback to run, cannot be {@code null}
     * @return a {@link Registration} that removes the callback when cancelled, never {@code null}
     */
    Registration registerCallback(MessageStream<?> messageStream, Runnable callback) {
        streamCallbacks.put(
            Objects.requireNonNull(messageStream, "messageStream"),
            Objects.requireNonNull(callback, "callback")
        );

        return () -> streamCallbacks.remove(messageStream) != null;
    }

    /**
     * Updates the highest known global index, if higher than what is already known, and notifies
     * all registered callbacks if it was.
     *
     * @param globalIndex the global index to update to, if higher than the current value
     */
    void updateHighestKnownGlobalIndex(long globalIndex) {
        synchronized (streamCallbacks) {
            if (globalIndex <= highestKnownGlobalIndex) {  // checks if notification can be skipped
                return;
            }

            highestKnownGlobalIndex = globalIndex;
        }

        /*
         * ContinuousMessageStream already guarantees that the callbacks do not
         * throw exceptions, and per MessageStream documentation they must not
         * block or do any significant amount of work in the callback. This may
         * block or stop the monitor thread otherwise.
         *
         * The callbacks should still preferably be run outside the
         * synchronization block.
         */

        for (Runnable callback : streamCallbacks.values()) {
            callback.run();
        }
    }

    private void monitorForNewEvents(Connection c) throws SQLException {
        PGConnection pgConnection = c.unwrap(PGConnection.class);

        c.setAutoCommit(true);  // required to have no transaction for receiving notifications

        /*
         * First set up a listener for global index updates:
         */

        try (PreparedStatement ps = c.prepareStatement("LISTEN events_channel")) {
            ps.execute();
        }

        /*
         * It's possible some notifications were missed, so perform a direct query once
         * to find the current highest global index:
         */

        try (
            PreparedStatement ps = c.prepareStatement(EVENTS_FIND_NEXT_AVAILABLE_GLOBAL_INDEX);
            ResultSet resultSet = ps.executeQuery();
        ) {
            resultSet.next();

            updateHighestKnownGlobalIndex(resultSet.getLong(1) - 1);
        }

        /*
         * Loop and process incoming notifications, until interrupted. Note that
         * there is no InterruptedException that can occur here, as the Postgres
         * driver hides this fact and wraps it in a normal SQLException.
         */

        while (!Thread.currentThread().isInterrupted()) {
            PGNotification[] notifications = pgConnection.getNotifications(60000);
            long globalIndex = 0;

            for (PGNotification notification : notifications) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Received notification from PID " + notification.getPID() + " on " + notification.getName() + " with " + notification.getParameter());
                }

                if (notification.getName().equals("events_channel")) {
                    globalIndex = Math.max(Long.parseLong(notification.getParameter()), globalIndex);
                }
            }

            if (globalIndex > 0) {
                updateHighestKnownGlobalIndex(globalIndex);
            }
        }
    }
}
