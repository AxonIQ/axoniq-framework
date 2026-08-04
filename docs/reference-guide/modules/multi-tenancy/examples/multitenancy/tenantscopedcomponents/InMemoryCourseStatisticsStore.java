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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCourseStatisticsStore implements CourseStatisticsStore {

    private final Map<String, CourseStatistics> statistics = new ConcurrentHashMap<>();

    public InMemoryCourseStatisticsStore(String tenantId) {
        // One store per tenant, named after the tenant it serves.
    }

    @Override
    public void save(CourseStatistics courseStatistics) {
        statistics.put(courseStatistics.courseId(), courseStatistics);
    }

    @Override
    public void flush() {
        // Writes pending changes.
    }

    @Override
    public void close() {
        statistics.clear();
    }
}
