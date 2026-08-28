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

package messagetransformation.configuringtransformations;

import io.axoniq.framework.messaging.transformation.events.EventTransformation;
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;
import messagetransformation.coursecatalog.WelcomeMessage090ToV1;
import messagetransformation.coursecatalog.WelcomeMessageBetaCleanup;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;
import tools.jackson.databind.JsonNode;

final class OverlapResolution {

    private OverlapResolution() {
    }

    static void register() {
        // tag::matching-precedence[]
        QualifiedName welcomeMessageSent = new QualifiedName("coursecatalog.WelcomeMessageSent");
        MessageType v1 = new MessageType(welcomeMessageSent, "1.0.0");

        EventTransformation betaCleanup =                                       // <1>
                EventTransformation.from(type -> type.version().startsWith("0."))
                                   .declaringFromTypes(welcomeMessageSent)
                                   .to(v1)
                                   .transform(JsonNode.class, WelcomeMessageBetaCleanup::map);

        EventTransformation beta090ToV1 =                                       // <2>
                EventTransformation.from(new MessageType(welcomeMessageSent, "0.9.0"))
                                   .to(v1)
                                   .transform(JsonNode.class, WelcomeMessage090ToV1::map);

        EventTransformerChain.builder()
                             .register(betaCleanup)
                             .register(beta090ToV1)
                             .build();
        // end::matching-precedence[]
    }
}
