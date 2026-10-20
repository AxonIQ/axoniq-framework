/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
