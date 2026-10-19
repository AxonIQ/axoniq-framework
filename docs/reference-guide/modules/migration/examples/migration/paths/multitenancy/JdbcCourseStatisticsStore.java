/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package migration.paths.multitenancy;

import javax.sql.DataSource;

/**
 * Stands in for the JDBC-backed read model of a single tenant, built on that tenant's {@link DataSource}.
 */
public class JdbcCourseStatisticsStore implements CourseStatisticsStore {

    private final DataSource dataSource;

    public JdbcCourseStatisticsStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(CourseStatistics statistics) {
        // Writes to this tenant's database through dataSource.
    }

    @Override
    public void close() {
        // Releases this tenant's resources.
    }
}
