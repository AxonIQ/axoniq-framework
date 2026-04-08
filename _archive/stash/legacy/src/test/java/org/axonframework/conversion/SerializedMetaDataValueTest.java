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

package org.axonframework.conversion;

import org.axonframework.messaging.core.Metadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Allard Buijze
 */
class SerializedMetadataValueTest {

    @Test
    void serializeMetadata() {
        byte[] stubData = new byte[]{};
        SerializedMetadata<byte[]> serializedMetadata = new SerializedMetadata<>(stubData, byte[].class);
        assertEquals(stubData, serializedMetadata.getData());
        assertEquals(byte[].class, serializedMetadata.getContentType());
        assertNull(serializedMetadata.getType().getRevision());
        assertEquals(Metadata.class.getName(), serializedMetadata.getType().getName());
    }

    @Test
    void isSerializedMetadata() {
        SerializedMetadata<byte[]> serializedMetadata = new SerializedMetadata<>(new byte[]{}, byte[].class);
        assertTrue(SerializedMetadata.isSerializedMetadata(serializedMetadata));
        assertFalse(SerializedMetadata.isSerializedMetadata(
                new SimpleSerializedObject<>("test", String.class, "type", "rev")));
    }

}
