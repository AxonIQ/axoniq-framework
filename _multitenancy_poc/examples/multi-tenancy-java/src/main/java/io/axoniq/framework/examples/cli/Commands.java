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

import io.axoniq.framework.examples.faculty.write.createcourse.CreateCourse;
import io.axoniq.framework.examples.shared.ids.CourseId;

import java.util.function.Function;

/**
 * A list of supported commands with their corresponding payload parsers. This allows easier creation in the REPL.
 */
public enum Commands implements Function<String, Object> {
    /**
     * Creates a new course. See {@link CreateCourse} for more details.
     */
    CREATE_COURSE(
            p -> {
                var s = p.split(",", 3);
                return new CreateCourse(
                        CourseId.of(
                                s[0].trim()),
                        s[1].trim(),
                        Integer.parseInt(s[2].trim()));
            }
    ),
    ;

    private final Function<String, Object> fn;

    Commands(Function<String, Object> fn) {
        this.fn = fn;
    }

    @Override
    public Object apply(String payloadCsv) {
        return fn.apply(payloadCsv);
    }
}
