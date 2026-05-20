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

package io.axoniq.framework.axonserver.connector.event;

import io.axoniq.axonserver.connector.AxonServerConnection;
import io.axoniq.axonserver.connector.event.EventChannel;
import io.axoniq.axonserver.connector.event.PersistentStream;
import io.axoniq.axonserver.connector.event.PersistentStreamProperties;
import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.axonserver.connector.api.AxonServerConnectionManager;
import org.axonframework.common.Registration;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.DelegatingEventConverter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PersistentStreamMessageSourceTest {

    private static ScheduledExecutorService TEST_SCHEDULER;
    private static ExecutorService CONCURRENT_TEST_EXECUTOR;
    private static final int THREAD_COUNT = 10;

    @Mock
    private BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> eventConsumer;

    private PersistentStreamMessageSource messageSource;

    @BeforeAll
    static void beforeAll() {
        TEST_SCHEDULER = Executors.newSingleThreadScheduledExecutor();
        CONCURRENT_TEST_EXECUTOR = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    @AfterAll
    static void afterAll() {
        TEST_SCHEDULER.shutdown();
        CONCURRENT_TEST_EXECUTOR.shutdown();
    }

    @BeforeEach
    void setUp() {
        // Use a mock connection manager so the source never actually connects to Axon Server
        AxonServerConnectionManager mockConnectionManager = mock(AxonServerConnectionManager.class);
        AxonServerConnection mockConnection = mock(AxonServerConnection.class);
        EventChannel mockEventChannel = mock(EventChannel.class);
        when(mockEventChannel.openPersistentStream(anyString(), anyInt(), anyInt(), any(), any()))
                .thenReturn(mock(PersistentStream.class));
        when(mockConnection.eventChannel()).thenReturn(mockEventChannel);
        when(mockConnectionManager.getConnection(anyString())).thenReturn(mockConnection);

        String streamName = UUID.randomUUID().toString();
        messageSource = new PersistentStreamMessageSource(
                streamName,
                mockConnectionManager,
                new AxonServerConfiguration(),
                new DelegatingEventConverter(new JacksonConverter()),
                new PersistentStreamProperties(streamName, 1, "example", Collections.emptyList(), "HEAD", null),
                TEST_SCHEDULER,
                UnitOfWorkTestUtils.SIMPLE_FACTORY,
                1
        );
    }

    @Test
    void subscribeShouldReturnValidRegistration() {
        // when
        Registration registration = messageSource.subscribe(eventConsumer);

        // then
        assertThat(registration).isNotNull();
        assertThat(registration.cancel()).isTrue();
    }

    @Test
    void subscribingTwiceWithSameConsumerShouldNotThrowException() {
        // given
        messageSource.subscribe(eventConsumer);

        // when / then
        assertThatCode(() -> messageSource.subscribe(eventConsumer)).doesNotThrowAnyException();
    }

    @Test
    void subscribingWithDifferentConsumerShouldThrowException() {
        // given
        messageSource.subscribe(eventConsumer);
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> anotherConsumer =
                mock(BiFunction.class);

        // when / then
        assertThatThrownBy(() -> messageSource.subscribe(anotherConsumer))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void cancellingRegistrationShouldAllowNewSubscription() {
        // given
        Registration registration = messageSource.subscribe(eventConsumer);
        registration.cancel();
        BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>> newConsumer =
                mock(BiFunction.class);

        // when / then
        assertThatCode(() -> messageSource.subscribe(newConsumer)).doesNotThrowAnyException();
    }

    @Test
    void registrationCancelShouldBeIdempotent() {
        // given
        Registration registration = messageSource.subscribe(eventConsumer);

        // when
        boolean firstCancel = registration.cancel();
        boolean secondCancel = registration.cancel();

        // then
        assertThat(firstCancel).isTrue();
        assertThat(secondCancel).isTrue();
    }

    @Nested
    class ThreadSafety {

        @Test
        void concurrentSubscribeWithSameConsumerShouldBeThreadSafe() throws InterruptedException {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(THREAD_COUNT);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

            IntStream.range(0, THREAD_COUNT)
                     .forEach(i -> CONCURRENT_TEST_EXECUTOR.submit(() -> {
                         try {
                             startLatch.await();
                             messageSource.subscribe(eventConsumer);
                         } catch (Exception e) {
                             exceptions.add(e);
                         } finally {
                             completionLatch.countDown();
                         }
                     }));

            startLatch.countDown();
            completionLatch.await(5, TimeUnit.SECONDS);

            assertThat(exceptions).isEmpty();
        }

        @Test
        void concurrentSubscribeWithDifferentConsumersShouldBeThreadSafe() throws InterruptedException {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completionLatch = new CountDownLatch(THREAD_COUNT);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
            AtomicInteger successfulSubscriptions = new AtomicInteger(0);

            IntStream.range(0, THREAD_COUNT)
                     .forEach(i -> CONCURRENT_TEST_EXECUTOR.submit(() -> {
                         try {
                             startLatch.await();
                             BiFunction<List<? extends EventMessage>, ProcessingContext, CompletableFuture<?>>
                                     consumer = mock(BiFunction.class);
                             messageSource.subscribe(consumer);
                             successfulSubscriptions.incrementAndGet();
                         } catch (IllegalStateException e) {
                             // expected for all but one subscription
                         } catch (Exception e) {
                             exceptions.add(e);
                         } finally {
                             completionLatch.countDown();
                         }
                     }));

            startLatch.countDown();
            completionLatch.await(5, TimeUnit.SECONDS);

            assertThat(exceptions).isEmpty();
            assertThat(successfulSubscriptions.get()).isEqualTo(1);
        }

        @Test
        void concurrentSubscribeAndCancelShouldBeThreadSafe() throws InterruptedException {
            int iterationCount = 100;
            CountDownLatch completionLatch = new CountDownLatch(iterationCount * 2);
            ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();

            IntStream.range(0, iterationCount).forEach(i -> {
                CONCURRENT_TEST_EXECUTOR.submit(() -> {
                    try {
                        messageSource.subscribe(eventConsumer);
                        completionLatch.countDown();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                });

                CONCURRENT_TEST_EXECUTOR.submit(() -> {
                    try {
                        Registration registration = messageSource.subscribe(eventConsumer);
                        if (registration != null) {
                            registration.cancel();
                        }
                        completionLatch.countDown();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                });
            });

            completionLatch.await(10, TimeUnit.SECONDS);

            assertThat(exceptions).isEmpty();
        }

        @Test
        void subscribeWhileRegistrationCancellationInProgressShouldBeThreadSafe() throws InterruptedException {
            Registration initialRegistration = messageSource.subscribe(eventConsumer);
            CountDownLatch cancellationStarted = new CountDownLatch(1);
            CountDownLatch cancellationCompleted = new CountDownLatch(1);
            CountDownLatch subscriptionAttempted = new CountDownLatch(1);

            CONCURRENT_TEST_EXECUTOR.submit(() -> {
                try {
                    cancellationStarted.countDown();
                    initialRegistration.cancel();
                } finally {
                    cancellationCompleted.countDown();
                }
            });

            CONCURRENT_TEST_EXECUTOR.submit(() -> {
                try {
                    cancellationStarted.await();
                    messageSource.subscribe(mock(BiFunction.class));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    subscriptionAttempted.countDown();
                }
            });

            assertThat(cancellationCompleted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(subscriptionAttempted.await(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
