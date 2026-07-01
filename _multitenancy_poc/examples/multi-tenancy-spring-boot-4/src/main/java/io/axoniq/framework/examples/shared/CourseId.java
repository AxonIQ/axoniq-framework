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

package io.axoniq.framework.examples.shared;

import java.util.UUID;

/**
 * Identifier for a course.
 */
public record CourseId(String raw) {

    private static final String ENTITY_TYPE = "Course";

    public CourseId {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Course ID cannot be null or empty");
        }
        raw = withType(raw);
    }

    public static CourseId of(String raw) {
        return new CourseId(raw);
    }

    public static CourseId random() {
        return new CourseId(UUID.randomUUID().toString());
    }

    @Override
    public String toString() {
        return raw;
    }

    private static String withType(String id) {
        return id.startsWith(ENTITY_TYPE + ":") ? id : ENTITY_TYPE + ":" + id;
    }
}
