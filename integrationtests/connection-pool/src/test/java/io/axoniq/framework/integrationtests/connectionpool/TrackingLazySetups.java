/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.extension.spring.jdbc.TrackingLazyConnectionDataSource;

/**
 * Setups on top of the framework's {@link TrackingLazyConnectionDataSource}.
 *
 * @author John Hendrikx
 */
final class TrackingLazySetups {

    /**
     * The common Spring setup, where the transaction manager, the connection provider and the engine all use a
     * {@link TrackingLazyConnectionDataSource}, with nothing but what the framework itself does on top of it.
     */
    static final TransactionSetup TRACKING_LAZY = (database, userConnections, autoCommit) -> {
        HikariDataSource pool = TransactionSetup.pool(database, userConnections, autoCommit, "tracking-lazy");
        TrackingLazyConnectionDataSource tracking = new TrackingLazyConnectionDataSource(pool);

        return SpringJdbcSetups.spring(pool, tracking, tracking, tracking);
    };

    private TrackingLazySetups() {
    }
}
