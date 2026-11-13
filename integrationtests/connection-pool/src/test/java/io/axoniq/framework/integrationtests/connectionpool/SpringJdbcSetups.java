/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import com.zaxxer.hikari.HikariDataSource;
import org.axonframework.extension.spring.jdbc.SpringDataSourceConnectionProvider;
import org.axonframework.extension.spring.messaging.unitofwork.SpringTransactionManager;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import javax.sql.DataSource;

/**
 * Transaction setups based on a Spring {@link DataSourceTransactionManager}.
 *
 * @author John Hendrikx
 */
final class SpringJdbcSetups {

    /**
     * The common setup: a plain pool, a {@link DataSourceTransactionManager} and the {@link SpringTransactionManager}
     * on the same {@link DataSource}.
     */
    static final TransactionSetup PLAIN = (database, userConnections, autoCommit) -> {
        HikariDataSource pool = TransactionSetup.pool(database, userConnections, autoCommit, "plain");

        return spring(pool, pool, pool, pool);
    };

    /**
     * As {@link #PLAIN}, but every component goes through a {@link LazyConnectionDataSourceProxy}, so a real
     * connection is only taken when the first statement runs.
     */
    static final TransactionSetup LAZY_PROXY = (database, userConnections, autoCommit) -> {
        HikariDataSource pool = TransactionSetup.pool(database, userConnections, autoCommit, "lazy");
        DataSource lazy = new LazyConnectionDataSourceProxy(pool);

        return spring(pool, lazy, lazy, lazy);
    };

    private SpringJdbcSetups() {
    }

    static Environment spring(HikariDataSource enginePool,
                                      DataSource transactionManagerDataSource,
                                      DataSource connectionProviderDataSource,
                                      DataSource engineDataSource) {
        DataSourceTransactionManager springTransactionManager =
                new DataSourceTransactionManager(transactionManagerDataSource);
        SpringTransactionManager axonTransactionManager = new SpringTransactionManager(
                springTransactionManager,
                null,
                new SpringDataSourceConnectionProvider(connectionProviderDataSource)
        );
        JdbcTemplate jdbcTemplate = new JdbcTemplate(transactionManagerDataSource);

        return new BasicEnvironment(
                engineDataSource,
                () -> enginePool.getHikariPoolMXBean().getActiveConnections(),
                axonTransactionManager,
                (sql, args) -> jdbcTemplate.update(sql, args),
                work -> new TransactionTemplate(springTransactionManager).executeWithoutResult(status -> work.run()),
                List.of(enginePool)
        );
    }
}
