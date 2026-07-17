package messagetransformation.definingtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

public final class RenameCourseOffered {

    // tag::rename-event[]
    private static final MessageType FROM = new MessageType("coursecatalog.CourseOffered", "1.0.0");
    private static final MessageType TO   = new MessageType("coursecatalog.CoursePublished", "1.0.0");

    public static EventTransformation build() {
        return EventTransformation.rename(FROM, TO); // <1>
    }
    // end::rename-event[]
}
