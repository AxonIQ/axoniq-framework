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

import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link DeadlineManagerParameterResolverFactoryConfigurationEnhancer} makes a
 * {@link DeadlineManager} handler parameter defer its calls to the {@link ProcessingContext} of the handler, as Axon
 * Framework 4 deferred them to the current unit of work, and otherwise resolves the parameter as before.
 */
class DeadlineManagerParameterResolverFactoryConfigurationEnhancerTest {

    private static final ScopeDescriptor SCOPE = () -> "scope";

    private final RecordingDeadlineManager deadlineManager = new RecordingDeadlineManager();
    private AxonConfiguration configuration;

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
    }

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
                                    .disableEnhancer(DeadlineManagerParameterResolverFactoryConfigurationEnhancer.class)
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
