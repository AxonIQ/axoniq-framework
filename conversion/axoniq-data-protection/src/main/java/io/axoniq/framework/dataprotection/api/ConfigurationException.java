/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */
package io.axoniq.framework.dataprotection.api;

import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;

/**
 * Runtime exception which indicates that there is some problem with the configuration. This
 * may have to do with the set up or capabilities of the {@link CryptoEngine},
 * or with the placement of the various Axon Data Protection Module annotations on application data classes.
 * <p>
 * In most cases, there is no way to recover from this exception without fixing the application code
 * or environment.
 *
 * @author Frans van Buul
 */
public final class ConfigurationException extends RuntimeException {

    /** Constructs a new configuration exception with the specified detail message.
     * The cause is not initialized, and may subsequently be initialized by a
     * call to {@link #initCause}.
     *
     * @param   message   the detail message. The detail message is saved for
     *          later retrieval by the {@link #getMessage()} method.
     */
    public ConfigurationException(String message) {
        super(message);
    }

    /**
     * Constructs a new configuration exception with the specified detail message and
     * cause.  <p>Note that the detail message associated with
     * {@code cause} is <i>not</i> automatically incorporated in
     * this configuration exception's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <code>null</code> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
