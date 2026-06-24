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

import io.axoniq.framework.examples.shared.ids.CourseId;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.configuration.DefaultTenantComponentRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcCourseStatsRepositoryTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("foo-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("foo-b");

    @Test
    void keepsTenantDatabasesSeparated() {
        TenantComponentRegistry<CourseStatsRepository> registry = new DefaultTenantComponentRegistry<>(
                CourseStatsRepository.class,
                tenant -> new JdbcCourseStatsRepository(tenantDataSource(tenant), tenant.tenantId())
        );

        CourseStatsRepository tenantARepository = registry.getComponent(TENANT_A);
        CourseStatsRepository tenantBRepository = registry.getComponent(TENANT_B);

        tenantARepository.save(new CoursesStatsReadModel(CourseId.of("course-a"), "Alpha", 30, 1));
        tenantBRepository.save(new CoursesStatsReadModel(CourseId.of("course-b"), "Beta", 40, 2));

        assertThat(tenantARepository.findAll()).extracting(CoursesStatsReadModel::courseId)
                                              .containsExactly(CourseId.of("course-a"));
        assertThat(tenantBRepository.findAll()).extracting(CoursesStatsReadModel::courseId)
                                              .containsExactly(CourseId.of("course-b"));
        assertThat(tenantARepository.findById(CourseId.of("course-b"))).isEmpty();
        assertThat(tenantBRepository.findById(CourseId.of("course-a"))).isEmpty();

        assertThat(tenantDatabaseFiles(TENANT_A)).isGreaterThan(0);
        assertThat(tenantDatabaseFiles(TENANT_B)).isGreaterThan(0);
        assertThat(tableExists(tenantDataSource(TENANT_A), "course_stats")).isTrue();
        assertThat(tableExists(tenantDataSource(TENANT_A), "token_entry")).isTrue();
        assertThat(tableExists(tenantDataSource(TENANT_B), "course_stats")).isTrue();
        assertThat(tableExists(tenantDataSource(TENANT_B), "token_entry")).isTrue();
    }

    private static DataSource tenantDataSource(TenantDescriptor tenantDescriptor) {
        try {
            Path databaseFile = databaseFile(tenantDescriptor);
            Files.createDirectories(databaseFile.getParent());

            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:file:" + databaseFile.toAbsolutePath().toString().replace('\\', '/')
                              + ";AUTO_SERVER=FALSE;DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            dataSource.setPassword("");
            initializeTenantSchema(dataSource);
            return dataSource;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create tenant data source", e);
        }
    }

    private static Path databaseFile(TenantDescriptor tenantDescriptor) {
        return Path.of("target", "h2-test", tenantDescriptor.tenantId(), "course-stats");
    }

    private static long tenantDatabaseFiles(TenantDescriptor tenantDescriptor) {
        Path tenantDirectory = databaseFile(tenantDescriptor).getParent();
        try (Stream<Path> files = Files.list(tenantDirectory)) {
            return files.filter(path -> path.getFileName().toString().startsWith("course-stats"))
                        .count();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to inspect tenant database directory", e);
        }
    }

    private static boolean tableExists(DataSource dataSource, String tableName) {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables = metadata.getTables(null, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    if (tableName.equalsIgnoreCase(tables.getString("TABLE_NAME"))) {
                        return true;
                    }
                }
            }
            return false;
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("Failed to inspect tenant database schema", e);
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
