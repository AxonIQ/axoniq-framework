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
import org.axonframework.messaging.core.QualifiedName;
import tools.jackson.databind.JsonNode;

import java.util.function.Predicate;

public final class WelcomeMessageBetaCleanup {

    // tag::predicate-prefilter[]
    private static final Predicate<MessageType> BETA_VERSION =
            type -> type.version().startsWith("0."); // <1>

    private static final QualifiedName FROM_NAME = new QualifiedName("coursecatalog.WelcomeMessageSent");
    private static final MessageType TO = new MessageType(FROM_NAME, "1.0.0");

    public static EventTransformation build() {
        return EventTransformation.from(BETA_VERSION)            // <2>
                                  .declaringFromTypes(FROM_NAME) // <3>
                                  .to(TO)
                                  .transform(JsonNode.class, WelcomeMessageBetaCleanup::map);
    }
    // end::predicate-prefilter[]

    /**
     * Folds every {@code 0.x} beta payload up to the {@code 1.0.0} shape. Public so the overlap-resolution
     * sample on the configuring page can reference it as a method handle.
     *
     * @param payload the stored beta payload
     * @return the cleaned-up {@code 1.0.0} payload
     */
    public static JsonNode map(JsonNode payload) {
        return payload;
    }
}
