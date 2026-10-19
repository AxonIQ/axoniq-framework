/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.test.saga;

/**
 * The failures reported by the parts of the Axon Framework 4 saga fixture that are declared but not yet backed by
 * anything.
 * <p>
 * The declarations are kept so an Axon Framework 4 test suite still compiles, and so the day the event scheduler is
 * ported the change is to the bodies rather than to the API. Each of those bodies holds the Axon Framework 4 source as
 * a comment.
 *
 * @author Mateusz Nowak
 * @since 5.4.0
 */
final class NotPorted {

    private NotPorted() {
        // Utility class, not meant to be instantiated.
    }

    /**
     * Reports that the given {@code method} depends on the event scheduler, which {@code axoniq-legacy} does not carry
     * yet.
     *
     * @param method the name of the method that was called
     * @return the exception to throw
     */
    static UnsupportedOperationException eventScheduler(String method) {
        return new UnsupportedOperationException(
                "[" + method + "] is not supported: the event scheduler has not been ported into axoniq-legacy yet, "
                        + "see AxonIQ/AxonFramework#3104. Everything else the Axon Framework 4 saga fixture offered, "
                        + "deadlines included, works."
        );
    }
}
