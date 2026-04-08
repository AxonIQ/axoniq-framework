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

package org.axonframework.messaging.eventhandling.processing.streaming.token.store.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TokenSchemaTest {

    @Test
    void defaultNamesAreCorrect() {
        TokenSchema tokenSchema = TokenSchema.builder()
            .build();

        assertEquals("TokenEntry", tokenSchema.tokenTable());
        assertEquals("processorName", tokenSchema.processorNameColumn());
        assertEquals("segment", tokenSchema.segmentColumn());
        assertEquals("token", tokenSchema.tokenColumn());
        assertEquals("tokenType", tokenSchema.tokenTypeColumn());
        assertEquals("timestamp", tokenSchema.timestampColumn());
        assertEquals("owner", tokenSchema.ownerColumn());
    }

    @Test
    void modifiedNamesAreCorrect() {
        TokenSchema tokenSchema = TokenSchema.builder()
            .setTokenTable("TokenEntryModified")
            .setProcessorNameColumn("processorNameModified")
            .setSegmentColumn("segmentModified")
            .setTokenColumn("tokenModified")
            .setTokenTypeColumn("tokenTypeModified")
            .setTimestampColumn("timestampModified")
            .setOwnerColumn("ownerModified")
            .build();

        assertEquals("TokenEntryModified", tokenSchema.tokenTable());
        assertEquals("processorNameModified", tokenSchema.processorNameColumn());
        assertEquals("segmentModified", tokenSchema.segmentColumn());
        assertEquals("tokenModified", tokenSchema.tokenColumn());
        assertEquals("tokenTypeModified", tokenSchema.tokenTypeColumn());
        assertEquals("timestampModified", tokenSchema.timestampColumn());
        assertEquals("ownerModified", tokenSchema.ownerColumn());
    }

}