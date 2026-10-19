/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
