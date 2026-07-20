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

// tag::single-transformation[]
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;
import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

public final class CoursePublishedV1ToV2 {

    private static final MessageType FROM = new MessageType("coursecatalog.CoursePublished", "1.0.0");
    private static final MessageType TO   = new MessageType("coursecatalog.CoursePublished", "2.0.0");

    public static EventTransformation build() {
        return EventTransformation.from(FROM)                                             // <1>
                                  .to(TO)                                                 // <2>
                                  .transform(JsonNode.class, CoursePublishedV1ToV2::map); // <3>
    }

    private static JsonNode map(JsonNode v1) { // <4>
        int capacity = v1.get("capacity").asInt();
        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
        v2.set("catalogId", v1.get("catalogId"));
        v2.set("courseId",  v1.get("courseId"));
        v2.set("name",      v1.get("name"));
        v2.put("minCapacity", capacity);
        v2.put("maxCapacity", capacity);
        return v2;
    }
}
// end::single-transformation[]
