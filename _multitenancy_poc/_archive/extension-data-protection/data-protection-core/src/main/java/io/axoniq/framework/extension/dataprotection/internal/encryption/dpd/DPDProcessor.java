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
package io.axoniq.framework.extension.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.internal.encryption.core.Operation;

/**
 * Interface for Deep Personal Data processors. Handles encryption operations on objects
 * that contain fields marked with {@link DeepPersonalData}.
 *
 * @param <T> the type of data to process
 * @author Frans van Buul
 */
public interface DPDProcessor<T> {

    /**
     * Processes the given input object, applying encryption/decryption operations.
     * <p>
     * For mutable objects, this modifies the object in-place and returns the same instance.
     * For immutable objects (like Java records), this creates and returns a new instance with modified fields.
     *
     * @param input the object to process
     * @param context the encryption context
     * @param operation the operation to perform (ENCRYPT, DECRYPT, or REPLACE)
     * @return the processed object (same instance for mutable objects, new instance for immutable objects)
     */
    T process(T input, EncryptionContext context, Operation operation);

}
