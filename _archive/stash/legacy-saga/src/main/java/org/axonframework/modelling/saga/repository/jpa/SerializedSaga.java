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

package org.axonframework.modelling.saga.repository.jpa;

import org.axonframework.conversion.SimpleSerializedObject;

/**
 * Specialization of the SerializedObject for Sagas represented as byte array.
 *
 * @author Allard Buijze
 * @since 2.0
 * @deprecated By shifting from the {@link Serializer} to the {@link Converter}, this class becomes obsolete.
 */
@Deprecated(forRemoval = true, since = "5.0.0")
public class SerializedSaga extends SimpleSerializedObject<byte[]> {

    /**
     * Initialize a SerializedSaga instance with given {@code data}, of given {@code type} and
     * {@code revision}.
     *
     * @param data     The binary data of the Saga
     * @param type     The type of saga
     * @param revision The revision of the serialized version
     */
    public SerializedSaga(byte[] data, String type, String revision) {
        super(data, byte[].class, type, revision);
    }
}
