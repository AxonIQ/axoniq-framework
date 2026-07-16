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

package io.axoniq.framework.examples.university.web;

import io.axoniq.framework.examples.shared.CourseId;
import io.axoniq.framework.examples.university.read.coursestats.CourseStats;
import io.axoniq.framework.examples.university.read.coursestats.CoursesQueryResult;
import io.axoniq.framework.examples.university.read.coursestats.GetAllCourses;
import io.axoniq.framework.examples.university.read.coursestats.GetCourseStatsById;
import io.axoniq.framework.examples.university.write.createcourse.CreateCourse;
import io.axoniq.framework.messaging.multitenancy.configuration.MetadataBasedTenantResolver;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.gateway.QueryGateway;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Minimal REST API that routes commands and queries by tenant id.
 */
@RestController
@RequestMapping("/tenants")
public class CourseController {

    private final CommandGateway commandGateway;
    private final QueryGateway queryGateway;

    public CourseController(CommandGateway commandGateway, QueryGateway queryGateway) {
        this.commandGateway = commandGateway;
        this.queryGateway = queryGateway;
    }

    @PostMapping("/{tenantId}/courses")
    public ResponseEntity<CourseStats> createCourse(@PathVariable("tenantId") String tenantId,
                                                    @RequestBody CreateCourseRequest request) {
        CourseId courseId = request.courseId() == null || request.courseId().isBlank()
                ? CourseId.random()
                : CourseId.of(request.courseId());
        CreateCourse command = new CreateCourse(courseId, request.name(), request.capacity());
        commandGateway.sendAndWait(
                new org.axonframework.messaging.commandhandling.GenericCommandMessage(
                        new MessageType(CreateCourse.class.getName()),
                        command
                ).andMetadata(tenantMetadata(tenantId))
        );
        return ResponseEntity.status(HttpStatus.CREATED)
                             .body(new CourseStats(courseId, request.name(), request.capacity()));
    }

    @GetMapping("/{tenantId}/courses")
    public List<CourseStats> getCourses(@PathVariable("tenantId") String tenantId) {
        CoursesQueryResult result = queryGateway.query(
                new GenericQueryMessage(
                        new MessageType(GetAllCourses.class.getName()),
                        new GetAllCourses()
                ).andMetadata(tenantMetadata(tenantId)),
                CoursesQueryResult.class
        ).join();
        return result.courses();
    }

    @GetMapping("/{tenantId}/courses/{courseId}")
    public CourseStats getCourse(@PathVariable("tenantId") String tenantId,
                                 @PathVariable("courseId") String courseId) {
        return queryGateway.query(
                new GenericQueryMessage(
                        new MessageType(GetCourseStatsById.class.getName()),
                        new GetCourseStatsById(CourseId.of(courseId))
                ).andMetadata(tenantMetadata(tenantId)),
                CourseStats.class
        ).join();
    }

    private Metadata tenantMetadata(String tenantId) {
        return Metadata.with(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, tenantId);
    }

    /**
     * Request payload for creating a course.
     */
    public record CreateCourseRequest(String courseId, String name, int capacity) {
    }
}
