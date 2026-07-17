package messagetransformation.coursecatalog;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;

public final class SystemHeartbeatDrop {

    // tag::drop-event[]
    private static final MessageType FROM = new MessageType("coursecatalog.SystemHeartbeat", "1.0.0");

    public static EventTransformation build() {
        return EventTransformation.drop(FROM); // <1>
    }
    // end::drop-event[]
}
