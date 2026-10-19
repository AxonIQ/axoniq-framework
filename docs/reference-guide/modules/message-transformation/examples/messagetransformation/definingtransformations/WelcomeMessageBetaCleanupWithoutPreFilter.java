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
import tools.jackson.databind.JsonNode;

import java.util.function.Predicate;

public final class WelcomeMessageBetaCleanupWithoutPreFilter {

    // tag::predicate-no-prefilter[]
    private static final Predicate<MessageType> FROM_PREDICATE =
            type -> "coursecatalog.WelcomeMessageSent".equals(type.qualifiedName().name()) // <1>
                    && type.version().startsWith("0.");

    public static EventTransformation build() {
        return EventTransformation.from(FROM_PREDICATE) // <2>
                                  .to(new MessageType("coursecatalog.WelcomeMessageSent", "1.0.0"))
                                  .transform(JsonNode.class, WelcomeMessageBetaCleanupWithoutPreFilter::map);
    }
    // end::predicate-no-prefilter[]

    private static JsonNode map(JsonNode payload) {
        return payload;
    }
}
