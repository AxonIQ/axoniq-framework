/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.tenantscopedcomponents;

import javax.sql.DataSource;

public class JdbcCourseStatisticsStore implements CourseStatisticsStore {

    public JdbcCourseStatisticsStore(DataSource dataSource) {
        // Holds on to the tenant's datasource.
    }

    @Override
    public void save(CourseStatistics statistics) {
        // Writes through the tenant's datasource.
    }

    @Override
    public void flush() {
        // Writes pending changes.
    }

    @Override
    public void close() {
        // Releases the datasource.
    }
}
