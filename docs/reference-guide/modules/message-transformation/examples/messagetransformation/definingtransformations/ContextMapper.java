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

package messagetransformation.definingtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * Illustrates the context-taking {@code transform(...)} overload from the defining-transformations page:
 * a mapper that also receives the surrounding {@link ProcessingContext} for read-only access.
 */
final class ContextMapper {

    private static final MessageType FROM = new MessageType("coursecatalog.CoursePublished", "1.0.0");
    private static final MessageType TO = new MessageType("coursecatalog.CoursePublished", "2.0.0");

    private ContextMapper() {
    }

    static EventTransformation build() {
        return EventTransformation.from(FROM)
                                  .to(TO)
                                  // tag::processing-context[]
                                  .transform(JsonNode.class, (payload, context) -> map(payload, context)); // <1>
                                  // end::processing-context[]
    }

    private static JsonNode map(JsonNode payload, @Nullable ProcessingContext context) {
        return payload;
    }
}
