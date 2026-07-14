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

package io.axoniq.framework.tracing.integration;

import io.axoniq.framework.axonserver.connector.configuration.AxonServerConfigurationEnhancer;
import org.axonframework.messaging.tracing.SpanFactory;
import org.axonframework.messaging.commandhandling.tracing.TracingCommandBus;
import org.axonframework.modelling.repository.tracing.TracingRepository;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.modelling.StateManager;
import org.axonframework.modelling.repository.ManagedEntity;
import org.axonframework.modelling.repository.Repository;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the tracing decoration-order convention through the real configuration: the
 * {@code *TracingConfigurationEnhancer}s register their decorators at near-maximal
 * {@code TRACING_DECORATOR_ORDER}, so the tracing wrapper must be the <em>outermost</em> layer of every
 * registry-built component — even when another decorator registers at the default order {@code 0} (which itself
 * already sits above AxonFramework's own innermost {@code Intercepting*} decorators at
 * {@code Integer.MIN_VALUE + 100}). This outermost guarantee is what makes the
 * {@code TracingStateManager#register} already-traced {@code instanceof} guard sound, which the last test verifies
 * end-to-end by asserting a re-registered traced repository produces exactly one span per operation.
 */
class TracingDecoratorOrderingIntegrationTest {

    private MicrometerTracingTestSetup tracing;
    private InMemorySpanExporter spanExporter;
    private SpanFactory spanFactory;
    private @Nullable AxonConfiguration configuration;

    @BeforeEach
    void setUp() {
        tracing = MicrometerTracingTestSetup.create();
        spanExporter = tracing.spanExporter();
        spanFactory = tracing.spanFactory();
    }

    @AfterEach
    void tearDown() {
        if (configuration != null) {
            configuration.shutdown();
        }
        tracing.close();
    }

    @Test
    void tracingIsTheOutermostCommandBusDecoratorEvenAgainstACompetingDefaultOrderDecorator() {
        // given a competing CommandBus decorator registered at the DEFAULT order (0)
        AtomicBoolean competingDecoratorApplied = new AtomicBoolean(false);
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(registry -> registry
                                                   .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                   .registerComponent(SpanFactory.class, c -> spanFactory)
                                                   .registerDecorator(CommandBus.class, 0, (c, name, delegate) -> {
                                                       competingDecoratorApplied.set(true);
                                                       return new CompetingCommandBus(delegate);
                                                   })
                                           )
                                           .start();

        // when the CommandBus component is resolved
        CommandBus commandBus = configuration.getComponent(CommandBus.class);

        // then the competing decorator was applied, yet tracing is the outermost wrapper —
        // an outermost instanceof check is sufficient to detect a traced component
        assertThat(competingDecoratorApplied).isTrue();
        assertThat(commandBus).isInstanceOf(TracingCommandBus.class);
    }

    @Test
    void tracingIsTheOutermostRepositoryDecoratorEvenAgainstACompetingDefaultOrderDecorator() {
        // given a root-registered repository and a competing Repository decorator at the DEFAULT order (0)
        AtomicBoolean competingDecoratorApplied = new AtomicBoolean(false);
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(registry -> registry
                                                   .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                   .registerComponent(SpanFactory.class, c -> spanFactory)
                                                   .registerComponent(Repository.class, c -> new StubRepository())
                                                   .registerDecorator(Repository.class, 0, (c, name, delegate) -> {
                                                       competingDecoratorApplied.set(true);
                                                       return new CompetingRepository(
                                                               (Repository.LifecycleManagement<String, Booking>) delegate);
                                                   })
                                           )
                                           .start();

        // when
        Repository<?, ?> repository = configuration.getComponent(Repository.class);

        // then
        assertThat(competingDecoratorApplied).isTrue();
        assertThat(repository).isInstanceOf(TracingRepository.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void anAlreadyTracedRootRepositoryRegisteredOnTheStateManagerProducesExactlyOneSpanPerLoad() {
        // given a root-registered repository — traced (outermost) by the registry — re-registered on the
        // (traced) StateManager, which must detect the existing TracingRepository and not wrap again
        configuration = EventSourcingConfigurer.create()
                                               .componentRegistry(registry -> registry
                                                       .disableEnhancer(AxonServerConfigurationEnhancer.class)
                                                       .registerComponent(SpanFactory.class, c -> spanFactory)
                                                       .registerComponent(Repository.class, c -> new StubRepository())
                                               )
                                               .start();
        Repository<String, Booking> tracedRepository =
                (Repository<String, Booking>) configuration.getComponent(Repository.class);
        assertThat(tracedRepository).isInstanceOf(TracingRepository.class);
        StateManager stateManager = configuration.getComponent(StateManager.class);

        // when the already-traced repository is registered and an entity is loaded through it
        stateManager.register(tracedRepository);
        stateManager.repository(Booking.class, String.class)
                    .load("room-42", new StubProcessingContext())
                    .join();

        // then exactly ONE load span is exported — a double wrap would produce two
        List<String> loadSpans = spanExporter.getFinishedSpanItems().stream()
                                             .map(SpanData::getName)
                                             .filter(name -> name.equals("Repository.load Booking"))
                                             .toList();
        assertThat(loadSpans).hasSize(1);
    }

    /**
     * Competing decorator a third party might register at the default order — must end up INSIDE tracing.
     */
    private record CompetingCommandBus(CommandBus delegate) implements CommandBus {

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                                @Nullable ProcessingContext processingContext) {
            return delegate.dispatch(command, processingContext);
        }

        @Override
        public CommandBus subscribe(QualifiedName name, CommandHandler commandHandler) {
            delegate.subscribe(name, commandHandler);
            return this;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    /**
     * Competing repository decorator a third party might register at the default order.
     */
    private record CompetingRepository(Repository.LifecycleManagement<String, Booking> delegate)
            implements Repository.LifecycleManagement<String, Booking> {

        @Override
        public Class<Booking> entityType() {
            return delegate.entityType();
        }

        @Override
        public Class<String> idType() {
            return delegate.idType();
        }

        @Override
        public CompletableFuture<ManagedEntity<String, Booking>> load(String identifier,
                                                                      ProcessingContext processingContext) {
            return delegate.load(identifier, processingContext);
        }

        @Override
        public CompletableFuture<ManagedEntity<String, Booking>> loadOrCreate(String identifier,
                                                                              ProcessingContext processingContext) {
            return delegate.loadOrCreate(identifier, processingContext);
        }

        @Override
        public ManagedEntity<String, Booking> persist(String identifier,
                                                      Booking entity,
                                                      ProcessingContext processingContext) {
            return delegate.persist(identifier, entity, processingContext);
        }

        @Override
        public ManagedEntity<String, Booking> attach(ManagedEntity<String, Booking> entity,
                                                     ProcessingContext processingContext) {
            return delegate.attach(entity, processingContext);
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            descriptor.describeWrapperOf(delegate);
        }
    }

    /**
     * Minimal root repository.
     */
    private static final class StubRepository implements Repository.LifecycleManagement<String, Booking> {

        @Override
        public Class<Booking> entityType() {
            return Booking.class;
        }

        @Override
        public Class<String> idType() {
            return String.class;
        }

        @Override
        public CompletableFuture<ManagedEntity<String, Booking>> load(String identifier,
                                                                      ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ManagedEntity<String, Booking>> loadOrCreate(String identifier,
                                                                              ProcessingContext processingContext) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public ManagedEntity<String, Booking> persist(String identifier,
                                                      Booking entity,
                                                      ProcessingContext processingContext) {
            return null;
        }

        @Override
        public ManagedEntity<String, Booking> attach(ManagedEntity<String, Booking> entity,
                                                     ProcessingContext processingContext) {
            return entity;
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
        }
    }

    static final class Booking {

    }
}
