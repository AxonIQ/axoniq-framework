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

package io.axoniq.framework.springcloud.transport;

import io.axoniq.framework.springcloud.util.RecordingQueryHandler;
import org.axonframework.messaging.core.MessageType;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests how {@link SpringCloudQueryController} keeps an otherwise idle subscription alive.
 * <p>
 * The writes themselves are covered by {@link SseQueryResponseSinkFramingTest}, which reads back what came out on the
 * wire. What is pinned here is when they are attempted and on whose thread, because a keep-alive is a blocking write
 * to one subscriber while the scheduler that decides it is due carries every other subscription and every dispatched
 * query's deadline besides.
 *
 * @author Allard Buijze
 */
class SpringCloudQueryControllerTest {

    private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
    private static final Duration BEAT_OFTEN = Duration.ofMillis(50);
    private static final Duration NEVER_REACHED = Duration.ofHours(1);

    private IncomingQueryInvoker invoker;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() {
        invoker = new IncomingQueryInvoker(() -> "node-b", null);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private SpringCloudQueryController controller(Duration keepAliveInterval, Executor keepAliveExecutor) {
        return new SpringCloudQueryController(
                invoker,
                scheduler,
                keepAliveExecutor,
                SpringCloudQueryControllerConfiguration.DEFAULT.keepAliveInterval(keepAliveInterval)
        );
    }

    private static SubscriptionQueryRequest request() {
        return new SubscriptionQueryRequest("query-1",
                                            FIND_COURSE_TYPE.toString(),
                                            "{\"id\":\"course-1\"}",
                                            Map.of(),
                                            null,
                                            16);
    }

    /**
     * An {@link Executor} recording what it was handed, and either running it on a thread of its own or holding on to
     * it, so that a test can leave a keep-alive in mid-write.
     */
    private static final class RecordingExecutor implements Executor {

        private final List<Runnable> handed = new CopyOnWriteArrayList<>();
        private final @Nullable ExecutorService delegate;

        private RecordingExecutor(@Nullable ExecutorService delegate) {
            this.delegate = delegate;
        }

        private static RecordingExecutor running() {
            return new RecordingExecutor(Executors.newVirtualThreadPerTaskExecutor());
        }

        /**
         * Records what it is handed and never runs it, leaving every keep-alive perpetually mid-write.
         */
        private static RecordingExecutor holding() {
            return new RecordingExecutor(null);
        }

        @Override
        public void execute(Runnable command) {
            handed.add(command);
            if (delegate != null) {
                delegate.execute(command);
            }
        }

        private int handedOff() {
            return handed.size();
        }

        private void shutdown() {
            if (delegate != null) {
                delegate.shutdownNow();
            }
        }
    }

    @Nested
    class KeepingASubscriptionAlive {

        @Test
        void writesTheFirstKeepAliveWithoutTheExecutor() {
            // given a bound handler, so that the subscription is registered rather than refused
            invoker.bind(new RecordingQueryHandler());
            RecordingExecutor executor = RecordingExecutor.holding();

            // when the subscription is received
            controller(NEVER_REACHED, executor).receiveSubscriptionQuery(request());

            // then the first beat went out on the request's own thread. A stream nothing has been written to does not
            // reach the subscribing member at all, and that member asks for the initial result on the strength of it
            assertThat(executor.handedOff()).isZero();
        }

        @Test
        void handsTheBeatsThatFollowToTheExecutor() {
            // given
            invoker.bind(new RecordingQueryHandler());
            RecordingExecutor executor = RecordingExecutor.running();
            try {
                // when
                controller(BEAT_OFTEN, executor).receiveSubscriptionQuery(request());

                // then the scheduler only ever decides a beat is due. Writing one blocks on a subscriber that has
                // stopped reading, and the scheduler carries every other subscription and query deadline besides
                Awaitility.await()
                          .atMost(Duration.ofSeconds(5))
                          .until(() -> executor.handedOff() >= 2);
            } finally {
                executor.shutdown();
            }
        }

        @Test
        void skipsABeatWhileThePreviousOneIsStillBeingWritten() {
            // given an executor that never finishes a keep-alive, as a subscriber that stopped reading leaves one
            invoker.bind(new RecordingQueryHandler());
            RecordingExecutor executor = RecordingExecutor.holding();

            // when
            controller(BEAT_OFTEN, executor).receiveSubscriptionQuery(request());

            // then a stalled subscriber collects one thread rather than one per interval, however many intervals
            // pass, which is what handing the write elsewhere gives up of the scheduler's own guarantee that a run
            // never overlaps its predecessor
            Awaitility.await()
                      .atMost(Duration.ofSeconds(5))
                      .until(() -> executor.handedOff() == 1);
            Awaitility.await()
                      .during(Duration.ofMillis(300))
                      .atMost(Duration.ofSeconds(1))
                      .until(() -> executor.handedOff() == 1);
        }

        @Test
        void stopsBeatingWhenTheFirstKeepAliveCannotBeWritten() {
            // given no handler bound, so the invoker reports the subscription unhandled and ends the stream before
            // the first keep-alive is attempted
            RecordingExecutor executor = RecordingExecutor.running();
            try {
                // when
                controller(BEAT_OFTEN, executor).receiveSubscriptionQuery(request());

                // then a beat that fails on its very first write has to have something to cancel, or it goes on
                // writing to a stream nobody reads for as long as the application lives
                Awaitility.await()
                          .during(Duration.ofMillis(500))
                          .atMost(Duration.ofSeconds(1))
                          .until(() -> executor.handedOff() == 0);
            } finally {
                executor.shutdown();
            }
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsAMissingKeepAliveExecutor() {
            assertThatThrownBy(() -> controller(BEAT_OFTEN, null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("keepAliveExecutor");
        }

        @Test
        void rejectsAMissingConfiguration() {
            assertThatThrownBy(() -> new SpringCloudQueryController(invoker,
                                                                    scheduler,
                                                                    RecordingExecutor.holding(),
                                                                    null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("configuration");
        }
    }
}
