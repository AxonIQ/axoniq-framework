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
package io.axoniq.framework.extension.dataprotection.api;

/**
 * Enum that determines the scope of one of the other Axon Data Protection Module annotations. On regular fields, the
 * value will always be <code>DEFAULT</code>. The reason that this enum exists, is that scope may be different
 * on {@link java.util.Map} fields - in this case, an annotation may apply to the key, value or both sides
 * of the {@link java.util.Map}.
 *
 * @author Frans van Buul
 */
public enum Scope {

    /**
     * Used for all non-{@link java.util.Map} fields.
     */
    DEFAULT,

    /**
     * Indicates that an annotation applies to the keys in the {@link java.util.Map}.
     */
    KEY,

    /**
     * Indicates that an annotation applies to the values in the {@link java.util.Map}.
     */
    VALUE,

    /**
     * Indicates that an annotation applies to both the keys and the values in the {@link java.util.Map}.
     */
    BOTH,
}
