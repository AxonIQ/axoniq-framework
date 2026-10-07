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

package org.axonframework.modelling.saga.repository;

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.ConversionException;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.messaging.core.annotation.Namespace;
import org.axonframework.modelling.saga.AssociationValue;
import org.axonframework.modelling.saga.AssociationValuesImpl;
import org.axonframework.modelling.saga.repository.jpa.SagaEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract that a persistent {@link SagaStore} is expected to satisfy against a saga table written by Axon Framework 4.
 * <p>
 * Rows are seeded by hand in the Axon Framework 4 column layout, with the serialized saga spelled out as JSON rather
 * than produced by the store's own converter, so that a change to either the column layout or the stored representation
 * fails here rather than passing by construction.
 * <p>
 * Subclasses supply the store under test and the three fixture operations that differ per backend. Seeding is not done
 * for them, since it has to happen after their own setup: call {@link #seedAf4Rows()} at the end of it.
 *
 * @author Mateusz Nowak
 */
public abstract class Af4CompatibilityTestSuite {

    /**
     * Association value carried by {@link #SAGA_WITHOUT_REVISION}.
     */
    protected static final AssociationValue ORDER_1 = new AssociationValue("orderId", "order-1");
    /**
     * Association value carried by {@link #SAGA_WITH_REVISION}.
     */
    protected static final AssociationValue ORDER_2 = new AssociationValue("orderId", "order-2");

    /**
     * Identifier of the seeded saga whose {@code revision} column is {@code null}, as Axon Framework 4 left it for a
     * saga class without {@code @Revision}.
     */
    protected static final String SAGA_WITHOUT_REVISION = "saga-no-revision";
    /**
     * Identifier of the seeded saga whose {@code revision} column holds a value written by Axon Framework 4.
     */
    protected static final String SAGA_WITH_REVISION = "saga-with-revision";
    /**
     * Identifier of the seeded saga whose {@code serializedSaga} column holds XML an Axon Framework 4 node's
     * XStream-based {@code Serializer} wrote, rather than JSON.
     */
    protected static final String SAGA_WITH_XSTREAM_SERIALIZATION = "saga-xstream-serialized";

    /**
     * Returns the {@link SagaStore} under test.
     *
     * @return the store to verify
     */
    protected abstract SagaStore<Object> testSubject();

    /**
     * Inserts a saga row directly, bypassing the store, in the Axon Framework 4 column layout.
     *
     * @param sagaId         the value for the saga identifier column
     * @param sagaType       the value for the saga type column
     * @param revision       the value for the revision column, or {@code null} to leave it unset
     * @param serializedSaga the value for the serialized saga column
     */
    protected abstract void insertAf4Saga(String sagaId, String sagaType, String revision, String serializedSaga);

    /**
     * Inserts an association row directly, bypassing the store, in the Axon Framework 4 column layout.
     *
     * @param sagaId           the value for the saga identifier column
     * @param sagaType         the value for the saga type column
     * @param associationValue the association to record for the saga
     */
    protected abstract void insertAf4Association(String sagaId, String sagaType, AssociationValue associationValue);

    /**
     * Reads the given {@code column} of the saga row with the given {@code sagaId} directly, bypassing the store.
     *
     * @param column the name of the column to read
     * @param sagaId the identifier of the saga row to read
     * @return the column value, which may be {@code null}
     */
    protected abstract String columnOf(String column, String sagaId);

    /**
     * Reads the raw {@code serializedSaga} bytes for the given {@code sagaId} directly, bypassing the store's
     * {@link Converter}.
     *
     * @param sagaId the identifier of the saga row to read
     * @return the stored {@code serializedSaga} bytes
     */
    protected abstract byte[] serializedSagaOf(String sagaId);

    /**
     * Returns a {@link SagaStore} under test, backed by this suite's table or persistence context like
     * {@link #testSubject()}, configured with the given {@code converter}.
     *
     * @param converter the {@link Converter} the returned store uses
     * @return a store under test configured with {@code converter}
     */
    protected abstract SagaStore<Object> storeWith(Converter converter);

    /**
     * Runs the given {@code operation} against the store. Overridden by subclasses that need the call to happen inside
     * a transaction; by default it simply runs.
     *
     * @param operation the operation to run
     */
    protected void inTransaction(Runnable operation) {
        operation.run();
    }

    /**
     * Seeds the rows every test here expects, exactly as Axon Framework 4 would have left them. Subclasses call this at
     * the end of their own setup, since it needs their store and fixture to be in place.
     */
    protected final void seedAf4Rows() {
        String sagaType = StubSaga.class.getName();
        inTransaction(() -> {
            insertAf4Saga(SAGA_WITHOUT_REVISION, sagaType, null, "{\"handledEvents\":[\"OrderPlaced\"]}");
            insertAf4Saga(SAGA_WITH_REVISION, sagaType, "2", "{\"handledEvents\":[\"OrderPlaced\",\"OrderPaid\"]}");
            insertAf4Saga(SAGA_WITH_XSTREAM_SERIALIZATION, sagaType, null, """
                    <org.axonframework.modelling.saga.repository.StubSaga>
                      <handledEvents>
                        <string>OrderPlaced</string>
                      </handledEvents>
                    </org.axonframework.modelling.saga.repository.StubSaga>""");
            insertAf4Association(SAGA_WITHOUT_REVISION, sagaType, ORDER_1);
            insertAf4Association(SAGA_WITH_REVISION, sagaType, ORDER_2);
        });
    }

    private String revisionOf(String sagaId) {
        return columnOf("revision", sagaId);
    }

    @Nested
    class ReadingAxonFramework4Rows {

        @Test
        void aSagaWrittenByAxonFramework4IsReadBack() {
            // given a row written by Axon Framework 4 / when
            SagaStore.Entry<StubSaga> entry = testSubject().loadSaga(StubSaga.class, SAGA_WITHOUT_REVISION);

            // then
            assertThat(entry).isNotNull();
            assertThat(entry.saga().getHandledEvents()).containsExactly("OrderPlaced");
            assertThat(entry.associationValues()).containsExactly(ORDER_1);
        }

        @Test
        void aRevisionWrittenByAxonFramework4DoesNotAffectReadingTheSagaBack() {
            // given a row whose revision column is set / when
            SagaStore.Entry<StubSaga> entry = testSubject().loadSaga(StubSaga.class, SAGA_WITH_REVISION);

            // then the revision plays no part in resolving or converting the saga
            assertThat(entry).isNotNull();
            assertThat(entry.saga().getHandledEvents()).containsExactly("OrderPlaced", "OrderPaid");
            assertThat(entry.associationValues()).containsExactly(ORDER_2);
        }

        @Test
        void aSagaIsFoundByAnAssociationWrittenByAxonFramework4() {
            // given / when / then
            assertThat(testSubject().findSagas(StubSaga.class, ORDER_1)).containsExactly(SAGA_WITHOUT_REVISION);
            assertThat(testSubject().findSagas(StubSaga.class, ORDER_2)).containsExactly(SAGA_WITH_REVISION);
        }

        @Test
        void rowsWhoseSagaTypeIsNotTheClassNameAreNotReachable() {
            // given rows written by an Axon Framework 4 application whose Serializer mapped the saga class to a type
            // name other than its fully qualified class name, an XStream alias being the usual way to get one
            String alias = "order-saga";
            inTransaction(() -> {
                insertAf4Saga("aliased-saga", alias, null, "{\"handledEvents\":[\"OrderPlaced\"]}");
                insertAf4Association("aliased-saga", alias, ORDER_1);
            });

            // when / then the saga type column is matched literally against the class name, so the row is invisible.
            // Axon Framework 4 resolved it through the same Serializer that wrote it; this store has no such mapping,
            // and such a table needs its saga type columns rewritten to the class name before it can be used here.
            assertThat(testSubject().findSagas(StubSaga.class, ORDER_1)).containsExactly(SAGA_WITHOUT_REVISION);

            // the saga row itself is keyed only by identifier, so it still loads, but without its associations
            SagaStore.Entry<StubSaga> entry = testSubject().loadSaga(StubSaga.class, "aliased-saga");
            assertThat(entry).isNotNull();
            assertThat(entry.associationValues()).isEmpty();
        }

        @Test
        void xStreamSerializedSagaFailsWhenXStreamConverterIsNotUsed() {
            // given a row whose serializedSaga column is XML, written by an Axon Framework 4 node whose Serializer
            // defaulted to XStream, read here by a store without an XStreamConverter / when / then
            assertThatThrownBy(() -> testSubject().loadSaga(StubSaga.class, SAGA_WITH_XSTREAM_SERIALIZATION))
                    .isInstanceOf(ConversionException.class)
                    .hasMessageContaining(StubSaga.class.getName());
        }
    }

    @Nested
    class WritingToAnAxonFramework4Table {

        @Test
        void updatingASagaLeavesARevisionWrittenByAxonFramework4Untouched() {
            // given a saga whose revision column holds "2"
            assertThat(revisionOf(SAGA_WITH_REVISION)).isEqualTo("2");
            StubSaga updated = new StubSaga();
            updated.handled("OrderShipped");

            // when
            inTransaction(() -> testSubject().updateSaga(StubSaga.class,
                                                         SAGA_WITH_REVISION,
                                                         updated,
                                                         new AssociationValuesImpl(singleton(ORDER_2))));

            // then the state was replaced but the revision survived, so an Axon Framework 4 reader still sees its value
            SagaStore.Entry<StubSaga> entry = testSubject().loadSaga(StubSaga.class, SAGA_WITH_REVISION);
            assertThat(entry).isNotNull();
            assertThat(entry.saga().getHandledEvents()).containsExactly("OrderShipped");
            assertThat(revisionOf(SAGA_WITH_REVISION)).isEqualTo("2");
        }

        @Test
        void insertingASagaMarksTheRowAsWrittenByThisModule() {
            // given / when
            inTransaction(() -> testSubject().insertSaga(
                    StubSaga.class, "saga-new", new StubSaga(), singleton(ORDER_1)));

            // then the row is distinguishable from one Axon Framework 4 left without a revision
            assertThat(revisionOf("saga-new")).isEqualTo(SagaEntry.LEGACY_REVISION);
        }

        @Test
        void updatingASagaInsertedHereKeepsItsMarker() {
            // given a saga inserted by this module
            inTransaction(() -> testSubject().insertSaga(
                    StubSaga.class, "saga-new", new StubSaga(), singleton(ORDER_1)));

            // when
            inTransaction(() -> testSubject().updateSaga(StubSaga.class,
                                                         "saga-new",
                                                         new StubSaga(),
                                                         new AssociationValuesImpl(singleton(ORDER_1))));

            // then the update left the column alone, so the marker is still there
            assertThat(revisionOf("saga-new")).isEqualTo(SagaEntry.LEGACY_REVISION);
        }

        @Test
        void theSagaTypeColumnHoldsTheSagaClassName() {
            // given / when
            inTransaction(() -> testSubject().insertSaga(
                    StubSaga.class, "saga-new", new StubSaga(), singleton(ORDER_1)));

            // then, which is what Axon Framework 4 wrote for any saga without a custom type mapping
            assertThat(columnOf("sagaType", "saga-new")).isEqualTo(StubSaga.class.getName());
        }

        @Test
        void namespaceDoesNotChangeThePersistedSagaType() {
            // given / when
            inTransaction(() -> testSubject().insertSaga(
                    NamespacedSaga.class, "namespaced-saga", new NamespacedSaga("created"), singleton(ORDER_1)));

            // then @Namespace remains an event-processing and message-naming concern
            assertThat(columnOf("sagaType", "namespaced-saga")).isEqualTo(NamespacedSaga.class.getName());
        }
    }

    @Nested
    class XStreamConverterCompatibility {

        private SagaStore<Object> xStreamStore;

        @BeforeEach
        void setUp() {
            xStreamStore = storeWith(new XStreamConverter(new XStream()));
        }

        @Test
        void aSagaWrittenByAxonFramework4WithXStreamIsReadBackThroughXStreamConverter() {
            // given the row seeded by seedAf4Rows() holding real Axon Framework 4 XStreamSerializer XML / when
            SagaStore.Entry<StubSaga> entry =
                    xStreamStore.loadSaga(StubSaga.class, SAGA_WITH_XSTREAM_SERIALIZATION);

            // then
            assertThat(entry).isNotNull();
            assertThat(entry.saga().getHandledEvents()).containsExactly("OrderPlaced");
        }

        @Test
        void aSagaUpdatedThroughXStreamConverterIsStillReadableByAxonFramework4XStreamSerializer() throws Exception {
            // given a saga this store updates through the XStreamConverter, as an Axon Framework 5 node would while
            // an Axon Framework 4 node still runs alongside it during a rolling upgrade
            StubSaga updated = new StubSaga();
            updated.handled("OrderShipped");

            // when
            inTransaction(() -> xStreamStore.updateSaga(StubSaga.class,
                                                         SAGA_WITH_XSTREAM_SERIALIZATION,
                                                         updated,
                                                         new AssociationValuesImpl(Collections.emptySet())));

            // then an Axon Framework 4 node's own XStreamSerializer must still be able to read what was written
            byte[] serializedSaga = serializedSagaOf(SAGA_WITH_XSTREAM_SERIALIZATION);
            StubSaga fromAf4 = Af4ClassLoaderSupport.withAf4ClassLoader(
                    classLoader -> (StubSaga) Af4ClassLoaderSupport.af4XStream(classLoader)
                                                                    .fromXML(new String(serializedSaga,
                                                                                         StandardCharsets.UTF_8))
            );
            assertThat(fromAf4.getHandledEvents()).containsExactly("OrderShipped");
        }
    }

    @Namespace("orders")
    private record NamespacedSaga(String state) {
    }
}
