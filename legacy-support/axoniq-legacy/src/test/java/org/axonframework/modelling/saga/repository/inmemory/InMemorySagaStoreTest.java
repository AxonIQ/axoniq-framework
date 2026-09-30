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

package org.axonframework.modelling.saga.repository.inmemory;

import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValuesImpl;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.SagaStoreTestSuite;
import org.axonframework.modelling.saga.repository.StubSaga;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link InMemorySagaStore}.
 *
 * @author Mateusz Nowak
 */
class InMemorySagaStoreTest extends SagaStoreTestSuite {

    private final InMemorySagaStore testSubject = new InMemorySagaStore();

    @Override
    protected SagaStore<Object> testSubject() {
        return testSubject;
    }

    /**
     * Pins behaviour that differs per implementation, and is therefore not part of {@link SagaStoreTestSuite}. This
     * store creates the saga; {@code JdbcSagaStore} changes nothing and {@code JpaSagaStore} applies only the
     * association changes.
     */
    @Test
    void updatingAnAbsentSagaCreatesIt() {
        // given no saga stored
        AssociationValue orderId = new AssociationValue("orderId", "order-1");

        // when
        testSubject.updateSaga(
                StubSaga.class, "saga-1", new StubSaga(), new AssociationValuesImpl(singleton(orderId)));

        // then
        assertThat(testSubject.loadSaga(StubSaga.class, "saga-1")).isNotNull();
        assertThat(testSubject.findSagas(StubSaga.class, orderId)).containsExactly("saga-1");
    }

    @Test
    void sizeReflectsTheNumberOfStoredSagas() {
        // given
        assertThat(testSubject.size()).isZero();

        // when
        testSubject.insertSaga(StubSaga.class,
                               "saga-1",
                               new StubSaga(),
                               singleton(new AssociationValue("orderId", "order-1")));

        // then
        assertThat(testSubject.size()).isEqualTo(1);

        // and when the saga is deleted again
        testSubject.deleteSaga(StubSaga.class, "saga-1", singleton(new AssociationValue("orderId", "order-1")));

        // then
        assertThat(testSubject.size()).isZero();
    }
}
