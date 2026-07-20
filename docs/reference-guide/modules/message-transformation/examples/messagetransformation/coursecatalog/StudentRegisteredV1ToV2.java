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
import org.axonframework.common.TypeReference;
import org.axonframework.messaging.core.MessageType;

import java.util.LinkedHashMap;
import java.util.Map;

public final class StudentRegisteredV1ToV2 {

    private static final MessageType FROM = new MessageType("coursecatalog.StudentRegistered", "1.0.0");
    private static final MessageType TO = new MessageType("coursecatalog.StudentRegistered", "2.0.0");

    // tag::generic-payloads[]
    private static final TypeReference<Map<String, Object>> INPUT_TYPE = new TypeReference<>() { // <1>
    };

    public static EventTransformation build() {
        return EventTransformation.from(FROM)
                                  .to(TO)
                                  .transform(INPUT_TYPE, StudentRegisteredV1ToV2::map); // <2>
    }

    private static Map<String, Object> map(Map<String, Object> v1) {
        Map<String, Object> v2 = new LinkedHashMap<>();
        v2.put("catalogId", v1.get("catalogId"));
        v2.put("studentId", v1.get("studentId"));
        v2.put("fullName", combine(v1.get("firstName"), v1.get("lastName"))); // <3>
        return v2;
    }
    // end::generic-payloads[]

    private static Object combine(Object first, Object last) {
        return first + " " + last;
    }
}
