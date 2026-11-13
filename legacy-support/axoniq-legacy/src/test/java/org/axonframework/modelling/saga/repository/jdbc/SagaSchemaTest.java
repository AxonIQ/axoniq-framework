/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.modelling.saga.repository.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SagaSchemaTest {

    @Test
    void defaultNamesAreCorrect() {
        SagaSchema sagaSchema = SagaSchema.builder()
            .build();

        assertThat(sagaSchema.sagaEntryTable()).isEqualTo("SagaEntry");
        assertThat(sagaSchema.revisionColumn()).isEqualTo("revision");
        assertThat(sagaSchema.serializedSagaColumn()).isEqualTo("serializedSaga");
        assertThat(sagaSchema.associationValueEntryTable()).isEqualTo("AssociationValueEntry");
        assertThat(sagaSchema.associationKeyColumn()).isEqualTo("associationKey");
        assertThat(sagaSchema.associationValueColumn()).isEqualTo("associationValue");
        assertThat(sagaSchema.sagaIdColumn()).isEqualTo("sagaId");
        assertThat(sagaSchema.sagaTypeColumn()).isEqualTo("sagaType");
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

        assertThat(sagaSchema.sagaEntryTable()).isEqualTo("SagaEntryModified");
        assertThat(sagaSchema.revisionColumn()).isEqualTo("revisionModified");
        assertThat(sagaSchema.serializedSagaColumn()).isEqualTo("serializedSagaModified");
        assertThat(sagaSchema.associationValueEntryTable()).isEqualTo("AssociationValueEntryModified");
        assertThat(sagaSchema.associationKeyColumn()).isEqualTo("associationKeyModified");
        assertThat(sagaSchema.associationValueColumn()).isEqualTo("associationValueModified");
        assertThat(sagaSchema.sagaIdColumn()).isEqualTo("sagaIdModified");
        assertThat(sagaSchema.sagaTypeColumn()).isEqualTo("sagaTypeModified");
    }

}