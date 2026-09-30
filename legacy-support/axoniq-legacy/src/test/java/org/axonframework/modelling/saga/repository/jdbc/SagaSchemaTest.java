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

package org.axonframework.modelling.saga.repository.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SagaSchemaTest {

    @Test
    void defaultNamesAreCorrect() {
        SagaSchema sagaSchema = SagaSchema.builder()
            .build();

        assertEquals("SagaEntry", sagaSchema.sagaEntryTable());
        assertEquals("revision", sagaSchema.revisionColumn());
        assertEquals("serializedSaga", sagaSchema.serializedSagaColumn());
        assertEquals("AssociationValueEntry", sagaSchema.associationValueEntryTable());
        assertEquals("associationKey", sagaSchema.associationKeyColumn());
        assertEquals("associationValue", sagaSchema.associationValueColumn());
        assertEquals("sagaId", sagaSchema.sagaIdColumn());
        assertEquals("sagaType", sagaSchema.sagaTypeColumn());
    }

    @Test
    void modifiedNamesAreCorrect() {
        SagaSchema sagaSchema = SagaSchema.builder()
            .sagaEntryTable("SagaEntryModified")
            .revisionColumn("revisionModified")
            .serializedSagaColumn("serializedSagaModified")
            .associationValueEntryTable("AssociationValueEntryModified")
            .associationKeyColumn("associationKeyModified")
            .associationValueColumn("associationValueModified")
            .sagaIdColumn("sagaIdModified")
            .sagaTypeColumn("sagaTypeModified")
            .build();

        assertEquals("SagaEntryModified", sagaSchema.sagaEntryTable());
        assertEquals("revisionModified", sagaSchema.revisionColumn());
        assertEquals("serializedSagaModified", sagaSchema.serializedSagaColumn());
        assertEquals("AssociationValueEntryModified", sagaSchema.associationValueEntryTable());
        assertEquals("associationKeyModified", sagaSchema.associationKeyColumn());
        assertEquals("associationValueModified", sagaSchema.associationValueColumn());
        assertEquals("sagaIdModified", sagaSchema.sagaIdColumn());
        assertEquals("sagaTypeModified", sagaSchema.sagaTypeColumn());
    }

}