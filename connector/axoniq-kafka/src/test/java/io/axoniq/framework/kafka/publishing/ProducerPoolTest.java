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

package io.axoniq.framework.kafka.publishing;

import io.axoniq.framework.kafka.utils.RecordingKafkaClientFactory;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ProducerPoolTest {

    private final RecordingKafkaClientFactory clientFactory = new RecordingKafkaClientFactory(true);
    private final ProducerPool<byte[]> testSubject =
            new ProducerPool<>(clientFactory, Map.of(), new ByteArraySerializer(), "parcels-");

    private List<Object> transactionalIdentifiers() {
        return clientFactory.producerProperties().stream()
                            .map(properties -> properties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
                            .toList();
    }

    @Nested
    class Identifiers {

        @Test
        void reusesTheLowestFreeIdentifierFirst() {
            // given - three live producers, of which the middle one is discarded
            Producer<String, byte[]> first = testSubject.borrow();
            Producer<String, byte[]> second = testSubject.borrow();
            Producer<String, byte[]> third = testSubject.borrow();
            testSubject.discard(third);
            testSubject.discard(second);

            // when
            testSubject.borrow();
            testSubject.borrow();

            // then
            assertThat(first).isNotNull();
            assertThat(transactionalIdentifiers()).containsExactly("parcels-0", "parcels-1", "parcels-2",
                                                                   "parcels-1", "parcels-2");
            // a producer that takes over an identifier also takes over the client identifier that goes with it
            assertThat(clientFactory.producerClientNames()).containsExactly("tx-0", "tx-1", "tx-2", "tx-1", "tx-2");
        }

        @Test
        void failingToInitializeTransactionsFreesTheIdentifier() {
            // given - the first producer fails to initialize its transactions
            AtomicBoolean failed = new AtomicBoolean();
            clientFactory.prepareProducers(producer -> {
                if (failed.compareAndSet(false, true)) {
                    producer.initTransactionException = new KafkaException("init failed");
                }
            });
            assertThatThrownBy(testSubject::borrow).hasMessage("init failed");

            // when
            testSubject.borrow();

            // then
            assertThat(transactionalIdentifiers()).containsExactly("parcels-0", "parcels-0");
            assertThat(clientFactory.producers().getFirst().closed()).isTrue();
        }

        @Test
        void releasedProducerIsHandedOutAgain() {
            // given
            Producer<String, byte[]> producer = testSubject.borrow();
            testSubject.release(producer);

            // when
            Producer<String, byte[]> again = testSubject.borrow();

            // then
            assertThat(again).isSameAs(producer);
            assertThat(clientFactory.producers()).hasSize(1);
        }
    }

    @Nested
    class Closing {

        @Test
        void closesIdleProducersRightAway() {
            // given
            testSubject.release(testSubject.borrow());

            // when
            testSubject.close(Duration.ZERO);

            // then - a pool closed without waiting closes the idle producer on a thread of its own
            assertThat(clientFactory.producers()).hasSize(1);
            await().atMost(Duration.ofSeconds(5)).until(() -> clientFactory.producers().getFirst().closed());
        }

        @Test
        void closesIdleProducersInParallelWithinOneTimeout() {
            // given - three idle producers whose close is held up, as while the broker is down
            CompletableFuture<Void> gate = new CompletableFuture<>();
            clientFactory.holdUpClosingUntil(gate);
            List<Producer<String, byte[]>> producers =
                    List.of(testSubject.borrow(), testSubject.borrow(), testSubject.borrow());
            producers.forEach(testSubject::release);
            long start = System.nanoTime();

            // when
            testSubject.close(Duration.ofMillis(100));

            // then - at most one timeout passes, rather than one per producer
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(3));
            gate.complete(null);
            await().atMost(Duration.ofSeconds(5)).until(
                    () -> clientFactory.producers().stream().allMatch(MockProducer::closed)
            );
        }

        @Test
        void closesAProducerInUseWhenItIsReleased() {
            // given
            Producer<String, byte[]> inUse = testSubject.borrow();
            testSubject.close(Duration.ZERO);
            MockProducer<String, byte[]> mock = clientFactory.<byte[]>producers().getFirst();
            assertThat(mock.closed()).isFalse();

            // when
            testSubject.release(inUse);

            // then
            assertThat(mock.closed()).isTrue();
        }

        @Test
        void refusesToHandOutProducersOnceClosed() {
            // given
            testSubject.close(Duration.ZERO);

            // when / then
            assertThatThrownBy(testSubject::borrow).isInstanceOf(IllegalStateException.class);
        }
    }
}
