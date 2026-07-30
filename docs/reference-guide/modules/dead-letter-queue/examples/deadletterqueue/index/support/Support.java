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

package deadletterqueue.index.support;

/**
 * Supporting stubs for the dead-letter-queue documentation samples: a minimal logger and a projection update
 * method, referenced by the "detect dead letter" handler snippet through static imports so the snippet itself
 * stays focused on the framework API.
 */
public final class Support {

    public static final Log log = new Log();

    private Support() {
    }

    public static void updateProjection(Object event) {
    }

    /**
     * Minimal logger stub exposing the {@code warn} shape used by the handler snippet.
     */
    public static final class Log {

        public void warn(String format, Object... arguments) {
        }
    }
}
