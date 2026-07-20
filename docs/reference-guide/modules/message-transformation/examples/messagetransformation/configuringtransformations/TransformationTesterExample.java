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

import messagetransformation.coursecatalog.CoursePublishedV1ToV2;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

final class TransformationTesterExample {

    private static final QualifiedName COURSE_PUBLISHED = new QualifiedName("coursecatalog.CoursePublished");

    private TransformationTesterExample() {
    }

    static void verify() {
        // tag::testing-transformation[]
        TransformationTester.forTransformation(CoursePublishedV1ToV2.build())        // <1>
                            .given()
                            .messageType(COURSE_PUBLISHED, "1.0.0")                  // <2>
                            .payloadFromResource("/transformations/coursepublished/v1.json")
                            .when()
                            .then()
                            .success()
                            .outputType(new MessageType(COURSE_PUBLISHED, "2.0.0"))  // <3>
                            .outputPayloadFromResource("/transformations/coursepublished/v2.json");
        // end::testing-transformation[]
    }
}
