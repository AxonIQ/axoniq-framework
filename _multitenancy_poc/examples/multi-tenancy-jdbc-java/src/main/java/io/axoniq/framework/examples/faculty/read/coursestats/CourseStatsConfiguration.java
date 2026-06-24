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

package io.axoniq.framework.examples.faculty.read.coursestats;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.MultiTenantEventProcessorModule;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.pooled.MultiTenantPooledStreamingEventProcessorModule;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.TenantTokenStoreFactory;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.streaming.token.store.jdbc.JdbcTenantTokenStoreFactory;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.JdbcTokenStoreConfiguration;
import org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc.TokenSchema;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction.TenantTransactionManagerFactory;
import io.axoniq.framework.messaging.multitenancy.eventhandling.processing.transaction.jdbc.JdbcTenantTransactionManager;
import org.axonframework.conversion.Converter;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.h2.jdbcx.JdbcDataSource;
import org.axonframework.messaging.queryhandling.configuration.QueryHandlingModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public enum CourseStatsConfiguration {
    ;

    public static final Logger logger = LoggerFactory.getLogger(CourseStatsConfiguration.class);

    private static final String PROJECTION_PROCESSOR = "Projection_CourseStats_Processor";
    private static final JdbcTokenStoreConfiguration TOKEN_STORE_CONFIGURATION = JdbcTokenStoreConfiguration.DEFAULT
            .schema(TokenSchema.builder().setTokenTable("token_entry").build());

    public static EventSourcingConfigurer configure(EventSourcingConfigurer configurer) {
        MultiTenantPooledStreamingEventProcessorModule projectionProcessor = MultiTenantEventProcessorModule
                .pooledStreaming(PROJECTION_PROCESSOR)
                .eventHandlingComponents(
                        c -> c.autodetected(cfg -> new CoursesStatsProjection())
                )
                .notCustomized();

        QueryHandlingModule getCourseStatsByIdQueryHandler = QueryHandlingModule.named("get-course-stats-by-id")
                .queryHandlers()
                .autodetectedQueryHandlingComponent(cfg -> new GetCourseStatsByIdQueryHandler())
                .build();

        QueryHandlingModule getAllCourseStatsQueryHandler = QueryHandlingModule.named("get-all-course-stats")
                .queryHandlers()
                .autodetectedQueryHandlingComponent(cfg -> new GetAllCourseStatsQueryHandler())
                .build();

        return configurer
                .componentRegistry(cr -> {
                    cr.registerComponent(TenantTokenStoreFactory.class, cfg ->
                            new JdbcTenantTokenStoreFactory(CourseStatsConfiguration::tenantDataSource,
                                                            cfg.getComponent(Converter.class),
                                                            TOKEN_STORE_CONFIGURATION)
                    );
                    cr.registerComponent(TenantTransactionManagerFactory.class, cfg ->
                            tenant -> new JdbcTenantTransactionManager(tenantDataSource(tenant))
                    );
                    cr.registerComponent(TenantComponentRegistry.class, cfg ->
                            new DefaultTenantComponentRegistry<>(
                                    CourseStatsRepository.class,
                                    tenant -> new JdbcCourseStatsRepository(tenantDataSource(tenant), tenant.tenantId())
                            )
                    );
                    cr.registerModule(projectionProcessor);
                })
                .registerQueryHandlingModule(getCourseStatsByIdQueryHandler)
                .registerQueryHandlingModule(getAllCourseStatsQueryHandler);
    }

    private static DataSource tenantDataSource(TenantDescriptor tenantDescriptor) {
        try {
            Path databaseDirectory = Path.of("target", "h2", tenantDescriptor.tenantId());
            Files.createDirectories(databaseDirectory);

            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:file:" + databaseDirectory.resolve("course-stats").toAbsolutePath()
                                                                        .toString()
                                                                        .replace('\\', '/')
                              + ";AUTO_SERVER=FALSE;DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            initializeTenantSchema(dataSource);
            return dataSource;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create file-based H2 datasource for tenant "
                                            + tenantDescriptor.tenantId(), e);
        }
    }

    private static void initializeTenantSchema(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS course_stats (
                        course_id VARCHAR(255) PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        capacity INT NOT NULL,
                        subscribed_students INT NOT NULL
                    )
                    """);
            statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS token_entry (
                        processorName VARCHAR(255) NOT NULL,
                        segment INTEGER NOT NULL,
                        mask INTEGER NOT NULL,
                        token BLOB NULL,
                        tokenType VARCHAR(255) NULL,
                        timestamp VARCHAR(255) NULL,
                        owner VARCHAR(255) NULL,
                        PRIMARY KEY (processorName, segment)
                    )
                    """);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize tenant schema", e);
        }
    }
}
