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

import tools.jackson.databind.JsonNode;

/**
 * Supporting example mapper referenced by the overlap-resolution sample on the configuring page. Maps the
 * historic {@code 0.9.0} beta welcome message onto version {@code 1.0.0} with its own dedicated rule.
 */
public final class WelcomeMessage090ToV1 {

    private WelcomeMessage090ToV1() {
    }

    /**
     * Maps the {@code 0.9.0} beta payload onto the {@code 1.0.0} shape.
     *
     * @param payload the stored {@code 0.9.0} payload
     * @return the {@code 1.0.0} payload
     */
    public static JsonNode map(JsonNode payload) {
        return payload;
    }
}
