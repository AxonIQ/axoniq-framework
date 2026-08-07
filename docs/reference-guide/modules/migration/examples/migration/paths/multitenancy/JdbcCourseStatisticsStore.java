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
