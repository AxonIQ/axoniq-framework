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
