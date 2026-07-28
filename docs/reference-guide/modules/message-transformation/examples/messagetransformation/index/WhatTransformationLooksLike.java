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

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;
import tools.jackson.databind.JsonNode;

final class WhatTransformationLooksLike {

    private WhatTransformationLooksLike() {
    }

    static EventTransformation define() {
        // tag::single-transformation-overview[]
        EventTransformation transformation =
                EventTransformation.from(new MessageType("coursecatalog.CoursePublished", "1.0.0")) // <1>
                                   .to(new MessageType("coursecatalog.CoursePublished", "2.0.0"))   // <2>
                                   .transform(JsonNode.class, CoursePublishedV1ToV2::map);          // <3>
        // end::single-transformation-overview[]
        return transformation;
    }
}
