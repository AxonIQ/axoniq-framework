/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

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
