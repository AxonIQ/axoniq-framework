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
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.axonframework.messaging.eventhandling.replay.annotation.ResetHandler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.axoniq.framework.examples.faculty.read.coursestats.CourseStatsConfiguration.logger;

class JdbcCourseStatsRepository implements CourseStatsRepository {

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS course_stats (
                course_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                capacity INT NOT NULL,
                subscribed_students INT NOT NULL
            )
            """;

    private static final String UPSERT = """
            MERGE INTO course_stats (course_id, name, capacity, subscribed_students)
            KEY(course_id)
            VALUES (?, ?, ?, ?)
            """;

    private static final String SELECT_BY_ID = """
            SELECT course_id, name, capacity, subscribed_students
            FROM course_stats
            WHERE course_id = ?
            """;

    private static final String SELECT_ALL = """
            SELECT course_id, name, capacity, subscribed_students
            FROM course_stats
            ORDER BY course_id
            """;

    private static final String DELETE_ALL = "DELETE FROM course_stats";

    private final DataSource dataSource;
    private final String tenantId;

    JdbcCourseStatsRepository(DataSource dataSource, String tenantId) {
        this.dataSource = dataSource;
        this.tenantId = tenantId;
        initializeSchema();
        logger.info("Creating JDBC course stats repository for tenant {}", tenantId);
    }

    @Override
    public CoursesStatsReadModel save(CoursesStatsReadModel stats) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(UPSERT)) {
            statement.setString(1, stats.courseId().raw());
            statement.setString(2, stats.name());
            statement.setInt(3, stats.capacity());
            statement.setInt(4, stats.subscribedStudents());
            statement.executeUpdate();
            return stats;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to save course stats for tenant " + tenantId, e);
        }
    }

    @Override
    public Optional<CoursesStatsReadModel> findById(CourseId courseId) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_BY_ID)) {
            statement.setString(1, courseId.raw());
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next()
                        ? Optional.of(map(resultSet))
                        : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read course stats for tenant " + tenantId, e);
        }
    }

    @Override
    public List<CoursesStatsReadModel> findAll() {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_ALL);
             ResultSet resultSet = statement.executeQuery()) {
            List<CoursesStatsReadModel> results = new ArrayList<>();
            while (resultSet.next()) {
                results.add(map(resultSet));
            }
            return results;
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to read all course stats for tenant " + tenantId, e);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tenantId", tenantId)
                .append("dataSource", dataSource.getClass().getSimpleName())
                .toString();
    }

    @ResetHandler
    public void reset() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(DELETE_ALL);
            logger.info("Resetting JDBC course stats repository for tenant {}", tenantId);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to reset course stats for tenant " + tenantId, e);
        }
    }

    private void initializeSchema() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(CREATE_TABLE);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize course stats schema for tenant " + tenantId, e);
        }
    }

    private CoursesStatsReadModel map(ResultSet resultSet) throws SQLException {
        return new CoursesStatsReadModel(
                CourseId.of(resultSet.getString("course_id")),
                resultSet.getString("name"),
                resultSet.getInt("capacity"),
                resultSet.getInt("subscribed_students")
        );
    }
}
