/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.connectionpool;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Query;
import org.axonframework.extension.spring.jdbc.SpringDataSourceConnectionProvider;
import org.axonframework.extension.spring.jdbc.TrackingLazyConnectionDataSource;
import org.axonframework.extension.spring.messaging.unitofwork.SpringTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.orm.jpa.persistenceunit.PersistenceManagedTypes;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;

/**
 * Transaction setups based on a Spring {@link JpaTransactionManager} with Hibernate, the way many enterprise
 * applications are configured.
 *
 * @author John Hendrikx
 */
final class JpaSetups {

    /**
     * A plain pool, shared by Hibernate and the connection provider, with a {@link JpaTransactionManager} that is
     * told about the {@code DataSource} so that it exposes the connection of its transaction.
     */
    static final TransactionSetup JPA_TRANSACTION_MANAGER = (database, userConnections, autoCommit) -> {
        HikariDataSource pool = TransactionSetup.pool(database, userConnections, autoCommit, "jpa");

        return jpa(pool, pool);
    };

    /**
     * As {@link #JPA_TRANSACTION_MANAGER}, but Hibernate, the transaction manager, the connection provider and the
     * engine all use a {@link TrackingLazyConnectionDataSource}, so a physical connection is only taken once
     * something needs one.
     */
    static final TransactionSetup JPA_TRACKING_LAZY = (database, userConnections, autoCommit) -> {
        HikariDataSource pool = TransactionSetup.pool(database, userConnections, autoCommit, "jpa-tracking");

        return jpa(pool, new TrackingLazyConnectionDataSource(pool));
    };

    private JpaSetups() {
    }

    private static Environment jpa(HikariDataSource pool, DataSource dataSource) throws Exception {
        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();

        factoryBean.setDataSource(dataSource);
        factoryBean.setManagedTypes(PersistenceManagedTypes.of(UniqueEmailEntity.class.getName()));
        factoryBean.setPersistenceUnitName("connection-pool-test");
        factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

        Properties properties = new Properties();

        properties.setProperty("hibernate.hbm2ddl.auto", "none");
        factoryBean.setJpaProperties(properties);
        factoryBean.afterPropertiesSet();

        EntityManagerFactory entityManagerFactory = factoryBean.getObject();
        JpaTransactionManager springTransactionManager = new JpaTransactionManager(entityManagerFactory);

        springTransactionManager.setDataSource(dataSource);
        springTransactionManager.afterPropertiesSet();

        SpringTransactionManager axonTransactionManager = new SpringTransactionManager(
                springTransactionManager, null, new SpringDataSourceConnectionProvider(dataSource)
        );
        EntityManager sharedEntityManager = SharedEntityManagerCreator.createSharedEntityManager(entityManagerFactory);

        return new BasicEnvironment(
                dataSource,
                () -> pool.getHikariPoolMXBean().getActiveConnections(),
                axonTransactionManager,
                (sql, args) -> {
                    Query query = sharedEntityManager.createNativeQuery(positional(sql));

                    for (int i = 0; i < args.length; i++) {
                        query.setParameter(i + 1, args[i]);
                    }

                    query.executeUpdate();
                },
                work -> new TransactionTemplate(springTransactionManager).executeWithoutResult(status -> work.run()),
                List.of(entityManagerFactory::close, pool)
        );
    }

    private static String positional(String sql) {
        StringBuilder result = new StringBuilder();
        int index = 0;

        for (char c : sql.toCharArray()) {
            result.append(c);

            if (c == '?') {
                result.append(++index);
            }
        }

        return result.toString();
    }
}
