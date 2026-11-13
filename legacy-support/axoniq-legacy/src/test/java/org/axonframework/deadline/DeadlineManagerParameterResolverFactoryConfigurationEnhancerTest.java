/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule;
import org.axonframework.messaging.commandhandling.gateway.CommandGateway;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.annotation.EventHandler;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;

/**
 * Test class validating that the {@link DeadlineManagerParameterResolverFactoryConfigurationEnhancer} makes a
 * {@link DeadlineManager} handler parameter defer its calls to the {@link ProcessingContext} of the handler, as Axon
 * Framework 4 deferred them to the current unit of work, and otherwise resolves the parameter to the configured
 * component itself.
 */
class DeadlineManagerParameterResolverFactoryConfigurationEnhancerTest {

    private static final ScopeDescriptor SCOPE = () -> "scope";
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final RecordingDeadlineManager deadlineManager = new RecordingDeadlineManager();
    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

    @Nested
    class BoundToTheHandlerContext {

        @Test
        void aDeadlineManagerParameterDefersItsCallsUntilTheContextOfTheHandlerPreparesItsCommit() {
            // given
            ParameterResolver<?> resolver = resolverFor(
                    "handle", registry -> registry.registerComponent(DeadlineManager.class, c -> deadlineManager)
            );
            AtomicInteger scheduledDuringInvocation = new AtomicInteger(-1);

            // when
            runInUnitOfWork(context -> {
                DeadlineManager parameter = resolve(resolver, context);
                parameter.schedule(Instant.now(), "deadlineName", "payload", SCOPE);
                scheduledDuringInvocation.set(deadlineManager.scheduled.size());
            });

            // then
            assertThat(scheduledDuringInvocation).hasValue(0);
            assertThat(deadlineManager.scheduled).hasSize(1);
        }

        @Test
        void anEventHandlerComponentDefersItsDeadlineCallsToTheContextOfTheEvent() {
            // given
            SchedulingEventHandler handler = new SchedulingEventHandler(deadlineManager);
            configuration = MessagingConfigurer
                    .create()
                    .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class, c -> deadlineManager))
                    .eventProcessing(processing -> processing.subscribing(
                            subscribing -> subscribing.defaultProcessor(
                                    "processor", components -> components.autodetected("handler", c -> handler)
                            )
                    ))
                    .start();

            // when
            FutureUtils.joinAndUnwrap(
                    configuration.getComponent(EventSink.class)
                                 .publish(null, List.of(asEventMessage(new SomethingHappened("id")))),
                    TIMEOUT
            );

            // then
            assertThat(handler.scheduledDuringInvocation).hasValue(0);
            assertThat(deadlineManager.scheduled).hasSize(1);
        }

        /**
         * Axon Framework 4 deferred the call to the prepare-commit phase of the command's unit of work, so a command
         * handler that failed after scheduling never stored its deadline.
         */
        @Test
        void aCommandHandlerComponentFailingAfterSchedulingSchedulesNoDeadline() {
            // given
            FailingCommandHandler handler = new FailingCommandHandler(deadlineManager);
            configuration = MessagingConfigurer
                    .create()
                    .componentRegistry(cr -> cr.registerComponent(DeadlineManager.class, c -> deadlineManager))
                    .registerCommandHandlingModule(
                            CommandHandlingModule.named("commandHandling")
                                                 .commandHandlers()
                                                 .autodetectedCommandHandlingComponent(c -> handler)
                    )
                    .start();

            // when
            CompletableFuture<?> result = configuration.getComponent(CommandGateway.class)
                                                       .send(new ScheduleAndFail())
                                                       .getResultMessage();

            // then
            assertThat(result).failsWithin(TIMEOUT);
            assertThat(handler.scheduledDuringInvocation).hasValue(0);
            assertThat(deadlineManager.scheduled).isEmpty();
        }
    }

    @Nested
    class ResolvedToTheComponentItself {

        /**
         * A {@link DeadlineManager} that does not extend {@link AbstractDeadlineManager}, such as the stub of the test
         * fixture, has no deferral to bind a context to.
         */
        @Test
        void aDeadlineManagerWithoutDeferralIsResolvedItself() {
            // given
            DeadlineManager stub = new StubDeadlineManager();
            ParameterResolver<?> resolver = resolverFor(
                    "handle", registry -> registry.registerComponent(DeadlineManager.class, c -> stub)
            );

            // when
            Object resolved = resolveInUnitOfWork(resolver);

            // then
            assertThat(resolved).isSameAs(stub);
        }

        @Test
        void aParameterOfTheImplementationTypeIsResolvedToTheManagerItself() {
            // given
            ParameterResolver<?> resolver = resolverFor(
                    "handleWithImplementation",
                    registry -> registry.registerComponent(RecordingDeadlineManager.class, c -> deadlineManager)
            );

            // when
            Object resolved = resolveInUnitOfWork(resolver);

            // then
            assertThat(resolved).isSameAs(deadlineManager);
        }

        @Test
        void anApplicationDisablingTheEnhancerGetsTheManagerItself() {
            // given
            ParameterResolver<?> resolver = resolverFor(
                    "handle",
                    registry -> registry.registerComponent(DeadlineManager.class, c -> deadlineManager)
                                        .disableEnhancer(
                                                DeadlineManagerParameterResolverFactoryConfigurationEnhancer.class
                                        )
            );

            // when
            Object resolved = resolveInUnitOfWork(resolver);

            // then
            assertThat(resolved).isSameAs(deadlineManager);
        }

        @Test
        void withoutADeadlineManagerTheParameterIsNotResolved() {
            // given
            configuration = MessagingConfigurer.create().start();

            // when
            ParameterResolver<?> resolver = createResolver("handle");

            // then
            assertThat(resolver).isNull();
        }
    }

    private ParameterResolver<?> resolverFor(String methodName, Consumer<ComponentRegistry> registration) {
        configuration = MessagingConfigurer.create().componentRegistry(registration::accept).start();
        ParameterResolver<?> resolver = createResolver(methodName);
        assertThat(resolver).isNotNull();
        return resolver;
    }

    private @Nullable ParameterResolver<?> createResolver(String methodName) {
        Method method = Arrays.stream(Handlers.class.getDeclaredMethods())
                              .filter(candidate -> candidate.getName().equals(methodName))
                              .findFirst()
                              .orElseThrow();
        return configuration.getComponent(ParameterResolverFactory.class)
                            .createInstance(method, method.getParameters(), 1);
    }

    private static Object resolveInUnitOfWork(ParameterResolver<?> resolver) {
        return UnitOfWorkTestUtils.aUnitOfWork()
                                  .executeWithResult(context -> resolver.resolveParameterValue(context)
                                                                        .thenApply(Object.class::cast))
                                  .orTimeout(1, TimeUnit.SECONDS)
                                  .join();
    }

    private static DeadlineManager resolve(ParameterResolver<?> resolver, ProcessingContext context) {
        return (DeadlineManager) resolver.resolveParameterValue(context).orTimeout(1, TimeUnit.SECONDS).join();
    }

    private static void runInUnitOfWork(Consumer<ProcessingContext> invocation) {
        UnitOfWorkTestUtils.aUnitOfWork()
                           .executeWithResult(context -> {
                               invocation.accept(context);
                               return CompletableFuture.completedFuture(null);
                           })
                           .orTimeout(1, TimeUnit.SECONDS)
                           .join();
    }

    @SuppressWarnings("unused")
    private static final class Handlers {

        void handle(String payload, DeadlineManager deadlineManager) {
        }

        void handleWithImplementation(String payload, RecordingDeadlineManager deadlineManager) {
        }
    }

    private record SomethingHappened(String id) {

    }

    private record ScheduleAndFail() {

    }

    private static final class SchedulingEventHandler {

        private final RecordingDeadlineManager recorder;
        private final AtomicInteger scheduledDuringInvocation = new AtomicInteger(-1);

        private SchedulingEventHandler(RecordingDeadlineManager recorder) {
            this.recorder = recorder;
        }

        @EventHandler
        void on(SomethingHappened event, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Instant.now(), "deadlineName", "payload", SCOPE);
            scheduledDuringInvocation.set(recorder.scheduled.size());
        }
    }

    private static final class FailingCommandHandler {

        private final RecordingDeadlineManager recorder;
        private final AtomicInteger scheduledDuringInvocation = new AtomicInteger(-1);

        private FailingCommandHandler(RecordingDeadlineManager recorder) {
            this.recorder = recorder;
        }

        @CommandHandler
        void handle(ScheduleAndFail command, DeadlineManager deadlineManager) {
            deadlineManager.schedule(Instant.now(), "deadlineName", "payload", SCOPE);
            scheduledDuringInvocation.set(recorder.scheduled.size());
            throw new IllegalStateException("The command fails after scheduling a deadline");
        }
    }

    private static final class StubDeadlineManager implements DeadlineManager {

        @Override
        public String schedule(Instant triggerDateTime,
                               String deadlineName,
                               @Nullable Object messageOrPayload,
                               ScopeDescriptor deadlineScope) {
            return "scheduleId";
        }

        @Override
        public void cancelSchedule(String deadlineName, String scheduleId) {
        }

        @Override
        public void cancelAll(String deadlineName) {
        }

        @Override
        public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        }
    }
}
