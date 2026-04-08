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

package org.axonframework.modelling.entity.child;

import org.axonframework.modelling.entity.child.mock.RecordingChildEntity;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class GetterSetterChildEntityFieldDefinitionTest {

    private final GetterSetterChildEntityFieldDefinition<ParentEntity, RecordingChildEntity> testSubject = new GetterSetterChildEntityFieldDefinition<>(
            ParentEntity::getRecordingChildEntity,
            ParentEntity::setRecordingChildEntity
    );

    @Test
    void canRetrieveChildEntityForMutableEntity() {
        RecordingChildEntity childEntity = new RecordingChildEntity("7826736");
        ParentEntity parentEntity = new ParentEntity();
        parentEntity.setRecordingChildEntity(childEntity);

        assertEquals(childEntity, testSubject.getChildValue(parentEntity));
    }

    @Test
    void canEvolveParentEntityForMutableEntity() {
        RecordingChildEntity childEntity = new RecordingChildEntity("2323802");
        ParentEntity parentEntity = new ParentEntity();
        parentEntity.setRecordingChildEntity(childEntity);

        RecordingChildEntity newChildEntity = new RecordingChildEntity("1234567");
        ParentEntity evolvedParentEntity = testSubject.evolveParentBasedOnChildInput(parentEntity,
                                                                                     newChildEntity);

        assertEquals(newChildEntity, evolvedParentEntity.getRecordingChildEntity());
    }

    @Test
    void canNotCreateWithNullGetter() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new GetterSetterChildEntityFieldDefinition<>(
                null,
                ParentEntity::setRecordingChildEntity
        ));
    }

    @Test
    void canNotCreateWithNullSetter() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new GetterSetterChildEntityFieldDefinition<>(
                ParentEntity::getRecordingChildEntity,
                null
        ));
    }


    private static class ParentEntity {

        private RecordingChildEntity childEntity;

        public RecordingChildEntity getRecordingChildEntity() {
            return childEntity;
        }

        public void setRecordingChildEntity(RecordingChildEntity childEntity) {
            this.childEntity = childEntity;
        }
    }
}