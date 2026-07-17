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
