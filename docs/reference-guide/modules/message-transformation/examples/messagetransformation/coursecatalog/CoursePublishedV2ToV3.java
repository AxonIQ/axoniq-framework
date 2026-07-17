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
