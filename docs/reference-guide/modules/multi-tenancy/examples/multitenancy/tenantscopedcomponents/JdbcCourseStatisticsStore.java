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
