/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

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
