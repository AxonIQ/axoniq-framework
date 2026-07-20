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

package messagetransformation.index;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/**
 * Supporting stand-in for the transformation whose full definition lives on the defining-transformations
 * page, providing the {@code map} method the overview snippet references as a method handle so the overview
 * snippet stays focused on the shape of a single transformation.
 */
final class CoursePublishedV1ToV2 {

    private CoursePublishedV1ToV2() {
    }

    static JsonNode map(JsonNode v1) {
        int capacity = v1.get("capacity").asInt();
        ObjectNode v2 = JsonNodeFactory.instance.objectNode();
        v2.set("catalogId", v1.get("catalogId"));
        v2.set("courseId", v1.get("courseId"));
        v2.set("name", v1.get("name"));
        v2.put("minCapacity", capacity);
        v2.put("maxCapacity", capacity);
        return v2;
    }
}
