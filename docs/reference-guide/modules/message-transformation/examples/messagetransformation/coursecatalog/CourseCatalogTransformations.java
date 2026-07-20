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

// tag::register-chain[]
import io.axoniq.framework.messaging.transformation.events.EventTransformerChain;

public final class CourseCatalogTransformations {

    public static EventTransformerChain chain() {
        return EventTransformerChain.builder()                               // <1>
                                    .register(SystemAnnouncementLegacyUplift.build())
                                    .register(CoursePublishedV1ToV2.build()) // <2>
                                    .register(CoursePublishedV2ToV3.build())
                                    .register(StudentRegisteredV1ToV2.build())
                                    .register(WelcomeMessageBetaCleanup.build())
                                    .register(SystemHeartbeatDrop.build())   // <3>
                                    .build();                                // <4>
    }
}
// end::register-chain[]
