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

package io.axoniq.framework.examples.cli;

import io.axoniq.framework.examples.faculty.read.coursestats.GetAllCourseStats;
import io.axoniq.framework.examples.faculty.read.coursestats.GetCourseStatsById;
import io.axoniq.framework.examples.shared.ids.CourseId;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;

import java.util.Objects;
import java.util.function.Function;

/**
 * List of available queries for easier use in REPL.
 */
public enum Queries implements Function<String, GenericQueryMessage> {
    /**
     * Gets the details of one course, see {@link GetCourseStatsById} for more details.
     */
    ID(p -> new GenericQueryMessage(
            new MessageType(GetCourseStatsById.class.getName()),
                                    new GetCourseStatsById(
                                            CourseId.of(Objects.requireNonNull(p, "QueryById needs id")
                                                                              .trim())
                                    )), GetCourseStatsById.Result.class),
    /**
     * List all courses, see {@link GetAllCourseStats} for more details.
     */
    ALL(p -> new GenericQueryMessage(
            new MessageType(GetAllCourseStats.class),
            GetAllCourseStats.ALL), GetAllCourseStats.Result.class
    ),
    ;

    private final Function<String, GenericQueryMessage> fn;
    private final Class<?> responseType;

    Queries(Function<String, GenericQueryMessage> fn, Class<?> responseType) {
        this.fn = fn;
        this.responseType = responseType;
    }

    @Override
    public GenericQueryMessage apply(String s) {
        return fn.apply(s);
    }

    /**
     * The type of the response expected for this query, used by dynmic query handling.
     * @return the type of the response expected for this query
     */
    public Class<?> responseType() {
        return responseType;
    }
}
