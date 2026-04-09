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
package io.axoniq.framework.extension.dataprotection.cryptoengine;

/**
 * Enum used to describe key type/length. Currently, the module supports AES at all available key lengths.
 *
 * @author Frans van Buul
 */
public enum KeyType {

    /**
     * Represents a 256-bit AES key.
     */
    AES_256,

    /**
     * Represents a 192-bit AES key.
     */
    AES_192,

    /**
     * Represents a 128-bit AES key.
     */
    AES_128,
    ;
}
