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

package org.axonframework.deadline;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.dbscheduler.DbSchedulerDeadlineManager;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.common.util.DbSchedulerTestUtil.getScheduler;
import static org.axonframework.common.util.DbSchedulerTestUtil.reCreateTable;

/**
 * Test class validating that the {@link DeadlineManagerLifecycleConfigurationEnhancer} starts and shuts down the
 * deadline managers of a configuration with the application.
 *
 * @author Jakob Hatzl
 */
class DeadlineManagerLifecycleConfigurationEnhancerTest {

    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final RecordingDeadlineManager deadlineManager = new RecordingDeadlineManager("manager", timeline);

    @Test
    void aDeadlineManagerOfTheConfigurationIsShutDownWithTheApplication() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                 c -> deadlineManager))
                                   .start();
        assertThat(timeline).isEmpty();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).containsExactly("manager:shutdown");
    }

    @Test
    void aDeadlineManagerRegisteredUnderItsImplementationTypeIsShutDownToo() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(RecordingDeadlineManager.class,
                                                                                 c -> deadlineManager))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).containsExactly("manager:shutdown");
    }

    @Test
    void anApplicationDisablingTheEnhancerShutsDownItsDeadlineManagerItself() {
        // given
        AxonConfiguration configuration =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                 c -> deadlineManager)
                                                              .disableEnhancer(
                                                                      DeadlineManagerLifecycleConfigurationEnhancer.class
                                                              ))
                                   .start();

        // when
        configuration.shutdown();

        // then
        assertThat(timeline).isEmpty();
    }

    @Nested
    class DbScheduler {

        private Scheduler scheduler;

        @BeforeEach
        void setUp() {
            JDBCDataSource dataSource = new JDBCDataSource();
            dataSource.setUrl("jdbc:hsqldb:mem:lifecycleEnhancerTest");
            dataSource.setUser("sa");
            reCreateTable(dataSource);
            scheduler = getScheduler(dataSource, DbSchedulerDeadlineManager.binaryTask(() -> null));
        }

        @AfterEach
        void tearDown() {
            scheduler.stop();
        }

        @Test
        void theSchedulerStartsWithTheApplication() {
            // given
            DbSchedulerDeadlineManager deadlineManager = builder().build();

            // when
            AxonConfiguration configuration = start(deadlineManager);

            // then
            assertThat(scheduler.getSchedulerState().isStarted()).isTrue();
            configuration.shutdown();
        }

        @Test
        void theSchedulerIsLeftToTheApplicationWithoutStartScheduler() {
            // given
            DbSchedulerDeadlineManager deadlineManager = builder().startScheduler(false)
                                                                  .stopScheduler(false)
                                                                  .build();

            // when
            AxonConfiguration configuration = start(deadlineManager);

            // then
            assertThat(scheduler.getSchedulerState().isStarted()).isFalse();
            configuration.shutdown();
        }

        /**
         * A decorator that wraps the manager, ordered before the default order, would hide the
         * {@link DbSchedulerDeadlineManager} from the lifecycle decorator if that ran after it.
         */
        @Test
        void theSchedulerStartsWithTheApplicationWhenAnotherDecoratorWrapsTheManager() {
            // given
            DbSchedulerDeadlineManager deadlineManager = builder().build();

            DecoratorDefinition<DeadlineManager, WrappingDeadlineManager> wrapping =
                    DecoratorDefinition.forType(DeadlineManager.class)
                                       .with((config, name, manager) -> new WrappingDeadlineManager(manager))
                                       .order(-1);

            // when
            AxonConfiguration configuration =
                    MessagingConfigurer.create()
                                       .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                     c -> deadlineManager)
                                                                  .registerDecorator(wrapping))
                                       .start();

            // then
            assertThat(scheduler.getSchedulerState().isStarted()).isTrue();
            configuration.shutdown();
        }

        private DbSchedulerDeadlineManager.Builder builder() {
            return DbSchedulerDeadlineManager.builder()
                                             .scheduler(scheduler)
                                             .scopeAwareProvider(scope -> Stream.empty())
                                             .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                             .converter(new JacksonConverter());
        }

        private static AxonConfiguration start(DeadlineManager deadlineManager) {
            return MessagingConfigurer.create()
                                      .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class,
                                                                                    c -> deadlineManager))
                                      .start();
        }
    }

    /**
     * A {@link DeadlineManager} delegating every call to another, as a decorator wrapping the configured manager does.
     */
    private record WrappingDeadlineManager(DeadlineManager delegate) implements DeadlineManager {

        @Override
        public String schedule(Instant triggerDateTime,
                               String deadlineName,
                               @Nullable Object messageOrPayload,
                               ScopeDescriptor deadlineScope) {
            return delegate.schedule(triggerDateTime, deadlineName, messageOrPayload, deadlineScope);
        }

        @Override
        public void cancelSchedule(String deadlineName, String scheduleId) {
            delegate.cancelSchedule(deadlineName, scheduleId);
        }

        @Override
        public void cancelAll(String deadlineName) {
            delegate.cancelAll(deadlineName);
        }

        @Override
        public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
            delegate.cancelAllWithinScope(deadlineName, scope);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }
    }
}
