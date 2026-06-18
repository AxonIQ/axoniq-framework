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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.axoniq.framework.examples.faculty.read.coursestats.CourseStatsConfiguration.logger;

public interface CourseStatsRepository {
    CoursesStatsReadModel save(CoursesStatsReadModel stats);

    Optional<CoursesStatsReadModel> findById(CourseId courseId);

    default CoursesStatsReadModel findByIdOrThrow(CourseId courseId) {
        return findById(courseId).orElseThrow(() -> new RuntimeException("Course with id " + courseId + " does not exist!"));
    }

    List<CoursesStatsReadModel> findAll();
}

class InMemoryCourseStatsRepository implements CourseStatsRepository {

    private final ConcurrentHashMap<CourseId, CoursesStatsReadModel> stats = new ConcurrentHashMap<>();
    private final String tenantId;

    public InMemoryCourseStatsRepository(String tenantId) {
        this.tenantId = tenantId;

        logger.info("Creating in-memory course stats repository for tenant {}", tenantId);
    }

    @Override
    public CoursesStatsReadModel save(CoursesStatsReadModel stats) {
        this.stats.put(stats.courseId(), stats);
        return stats;
    }

    @Override
    public Optional<CoursesStatsReadModel> findById(CourseId courseId) {
        return Optional.ofNullable(stats.get(courseId));
    }

    @Override
    public List<CoursesStatsReadModel> findAll() {
        return stats.values().stream().toList();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("tenantId", tenantId)
                .append("stats", stats)
                .toString();
    }

    @ResetHandler
    public void reset() {
        logger.info("Resetting course stats repository for tenant {}", tenantId);
        stats.clear();
    }
}
