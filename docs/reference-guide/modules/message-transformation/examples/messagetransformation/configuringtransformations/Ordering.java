package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import messagetransformation.coursecatalog.CoursePublishedV1ToV2;
import messagetransformation.coursecatalog.CoursePublishedV2ToV3;

final class Ordering {

    private Ordering() {
    }

    static EventTransformerChain chain() {
        return EventTransformerChain.builder()
                                    // tag::ordering[]
                                    .register(CoursePublishedV1ToV2.build())   // 1.0.0 -> 2.0.0
                                    .register(CoursePublishedV2ToV3.build())   // 2.0.0 -> 3.0.0
                                    // end::ordering[]
                                    .build();
    }
}
