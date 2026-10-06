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

package org.axonframework.modelling.saga.repository.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.modelling.saga.repository.Af4XStreamSupport;
import org.axonframework.modelling.saga.repository.SagaStore;
import org.axonframework.modelling.saga.repository.StubSaga;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import com.thoughtworks.xstream.XStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link JpaSagaStore}, configured with an {@link XStreamConverter}, reads a saga row exactly as an
 * Axon Framework 4 node's {@code XStreamSerializer} left it.
 */
class JpaSagaStoreXStreamAf4CompatibilityTest {

    private static final String SAGA_ID = "saga-1";

    private EntityManagerFactory entityManagerFactory;
    private EntityManager entityManager;
    private JpaSagaStore testSubject;

    @BeforeEach
    void setUp() {
        entityManagerFactory = Persistence.createEntityManagerFactory("jpaSagaStorePersistenceUnit");
        entityManager = entityManagerFactory.createEntityManager();

        XStream xStream = new XStream();
        xStream.allowTypesByWildcard(new String[]{"org.axonframework.**"});
        testSubject = JpaSagaStore.builder()
                                  .entityManagerProvider(new SimpleEntityManagerProvider(entityManager))
                                  .converter(new XStreamConverter(xStream))
                                  .build();
    }

    @AfterEach
    void tearDown() {
        entityManager.close();
        entityManagerFactory.close();
    }

    @Test
    void aSagaWrittenByAxonFramework4WithXStreamIsReadBackThroughXStreamConverter() throws Exception {
        // given a saga row holding real Axon Framework 4 XStreamSerializer XML
        StubSaga saga = new StubSaga();
        saga.handled("OrderPlaced");
        saga.handled("OrderPaid");
        String af4Xml = Af4XStreamSupport.withAf4ClassLoader(
                classLoader -> Af4XStreamSupport.af4XStream(classLoader).toXML(saga)
        );
        insertAf4Saga(SAGA_ID, StubSaga.class.getName(), af4Xml);

        // when
        SagaStore.Entry<StubSaga> entry = testSubject.loadSaga(StubSaga.class, SAGA_ID);

        // then
        assertThat(entry).isNotNull();
        assertThat(entry.saga().getHandledEvents()).containsExactly("OrderPlaced", "OrderPaid");
    }

    private void insertAf4Saga(String sagaId, String sagaType, String serializedSaga) {
        entityManager.getTransaction().begin();
        entityManager.createNativeQuery(
                             "INSERT INTO SagaEntry (sagaId, revision, sagaType, serializedSaga) VALUES (?, ?, ?, ?)")
                     .setParameter(1, sagaId)
                     .setParameter(2, (String) null)
                     .setParameter(3, sagaType)
                     .setParameter(4, serializedSaga.getBytes(StandardCharsets.UTF_8))
                     .executeUpdate();
        entityManager.getTransaction().commit();
    }
}
