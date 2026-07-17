package messagetransformation.coursecatalog;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;
import tools.jackson.databind.JsonNode;

/**
 * Supporting example transformation referenced by {@link CourseCatalogTransformations}. Lifts a legacy
 * system announcement onto its current version so the sample chain reads like a realistic migration.
 */
public final class SystemAnnouncementLegacyUplift {

    private static final MessageType FROM = new MessageType("coursecatalog.SystemAnnouncement", "1.0.0");
    private static final MessageType TO = new MessageType("coursecatalog.SystemAnnouncement", "2.0.0");

    private SystemAnnouncementLegacyUplift() {
    }

    /**
     * Builds the uplift transformation.
     *
     * @return the transformation lifting a legacy system announcement onto its current version
     */
    public static EventTransformation build() {
        return EventTransformation.from(FROM)
                                  .to(TO)
                                  .transform(JsonNode.class, payload -> payload);
    }
}
