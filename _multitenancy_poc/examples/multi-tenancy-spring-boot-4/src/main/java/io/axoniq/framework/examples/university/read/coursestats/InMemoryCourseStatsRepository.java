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

package io.axoniq.framework.examples.university.read.coursestats;

import io.axoniq.framework.examples.shared.CourseId;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory tenant-specific course repository.
 */
public class InMemoryCourseStatsRepository implements CourseStatsRepository {

    private final ConcurrentHashMap<CourseId, CourseStats> stats = new ConcurrentHashMap<>();
    private final String tenantId;

    public InMemoryCourseStatsRepository(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public CourseStats save(CourseStats stats) {
        this.stats.put(stats.courseId(), stats);
        return stats;
    }

    @Override
    public Optional<CourseStats> findById(CourseId courseId) {
        return Optional.ofNullable(stats.get(courseId));
    }

    @Override
    public List<CourseStats> findAll() {
        return stats.values().stream().toList();
    }

    @Override
    public String toString() {
        return "InMemoryCourseStatsRepository{" +
               "tenantId='" + tenantId + '\'' +
               ", stats=" + stats +
               '}';
    }
}
