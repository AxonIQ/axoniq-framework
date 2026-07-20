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
