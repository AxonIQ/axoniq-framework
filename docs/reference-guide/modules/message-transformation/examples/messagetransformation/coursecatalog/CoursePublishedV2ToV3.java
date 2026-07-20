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

package messagetransformation.coursecatalog;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

public final class CoursePublishedV2ToV3 {

    // tag::typed-event[]
    private static final MessageType FROM = new MessageType("coursecatalog.CoursePublished", "2.0.0");
    private static final MessageType TO   = new MessageType("coursecatalog.CoursePublished", "3.0.0");

    public static EventTransformation build() {
        return EventTransformation.from(FROM)
                                  .to(TO)
                                  .transform(V2Schema.class, CoursePublishedV2ToV3::map); // <1>
    }

    private static CoursePublished map(V2Schema v2) { // <2>
        return new CoursePublished(
                new CatalogId(v2.catalogId().value()),
                new CourseId(v2.courseId().value()),
                v2.name(),
                new CapacityRange(v2.minCapacity(), v2.maxCapacity())
        );
    }

    // Mirrors how a v2 CoursePublished was stored: capacity split into a min and a max.
    record V2Schema(Id catalogId, Id courseId, String name, int minCapacity, int maxCapacity) { } // <3>

    // Stored id value object: a single value string.
    record Id(String value) { }
    // end::typed-event[]
}
