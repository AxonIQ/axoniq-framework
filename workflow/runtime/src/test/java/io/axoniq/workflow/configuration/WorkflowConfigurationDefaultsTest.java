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
package io.axoniq.workflow.configuration;

import io.axoniq.workflow.runtime.execution.WorkflowEventTagResolver;
import jakarta.annotation.Nonnull;
import org.axonframework.common.configuration.Component;
import org.axonframework.common.configuration.ComponentDefinition;
import org.axonframework.common.configuration.ComponentFactory;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.common.configuration.DecoratorDefinition;
import org.axonframework.common.configuration.LifecycleHandler;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.common.configuration.Module;
import org.axonframework.common.configuration.OverridePolicy;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.lifecycle.Phase;
import org.axonframework.eventsourcing.eventstore.MultiTagResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link WorkflowConfigurationDefaults}.
 *
 * @author Simon Zambrovski
 */
class WorkflowConfigurationDefaultsTest {

    private final WorkflowConfigurationDefaults subject = new WorkflowConfigurationDefaults();

    @Test
    void orderReturnsWorkflowDefaultsEnhancerOrder() {
        assertThat(subject.order()).isEqualTo(WorkflowConfigurationDefaults.WORKFLOW_DEFAULTS_ENHANCER_ORDER);
    }

    @Test
    void decorateTagResolverRegistersTagResolverDecoratorAtOrderZero() {
        var registry = new CapturingComponentRegistry();

        subject.decorateTagResolver(registry);

        assertThat(registry.decoratorDefinition).isNotNull()
                                                .isInstanceOf(DecoratorDefinition.CompletedDecoratorDefinition.class);

        var decorator = registry.decoratorDefinition;
        assertThat(decorator.order()).isZero();
        assertThat(decorator.matches(new Component.Identifier<>(TagResolver.class, "tagResolver"))).isTrue();
        assertThat(decorator.matches(new Component.Identifier<>(Configuration.class, "tagResolver"))).isFalse();
    }

    @Test
    void decorateTagResolverRegistersMultiTagResolverWithWorkflowEventTagResolverDelegate() {
        var registry = new CapturingComponentRegistry();
        subject.decorateTagResolver(registry);

        TagResolver delegate = eventMessage -> java.util.Set.of();
        TagResolver decorated = registry.decorate(delegate);

        assertThat(decorated).isInstanceOf(MultiTagResolver.class);
        assertThat(extractDelegates((MultiTagResolver) decorated))
                .hasSize(2)
                .satisfies(delegates -> {
                    assertThat(delegates.getFirst()).isSameAs(delegate);
                    assertThat(delegates.get(1)).isInstanceOf(WorkflowEventTagResolver.class);
                });
    }

    /**
     * The engine's shutdown drops every workflow execution, which is exactly what
     * {@code hasPendingCheckpointWork(segment)} counts. Running it in the same phase as the event processor's shutdown
     * makes the two race: same-phase shutdown handlers are launched together and joined, so the engine can empty the
     * repository before the processor drains. The drain then finds nothing pending and stores a token covering events
     * whose wake was never applied, and no later claim redelivers them.
     * <p>
     * Shutdown handlers run from the highest phase down, so the engine must sit strictly below the processor's phase
     * to run after it, and strictly above {@link Phase#LOCAL_MESSAGE_HANDLER_REGISTRATIONS}, where the processor's
     * coordinator and worker executors are torn down.
     */
    @Test
    void workflowEngineShutsDownAfterTheEventProcessorHasDrained() {
        var registry = new CapturingComponentRegistry();

        subject.registerWorkflowEngine(registry);

        // --- precondition evidence -------------------------------------------------------------------------------
        assertThat(registry.componentDefinition).as("the engine must be registered").isNotNull();
        var shutdownPhases = registry.capturedShutdownPhases();
        assertThat(shutdownPhases).as("the engine registers exactly one shutdown handler").hasSize(1);

        // --- oracle ----------------------------------------------------------------------------------------------
        assertThat(shutdownPhases.getFirst())
                .as("""
                    The engine's shutdown phase is %s and the PooledStreamingEventProcessor shuts down at %s. \
                    Shutdown runs the highest phase first, so the engine must be strictly lower to run after the \
                    processor's drain. At the same phase the two are launched together and joined, and clearing the \
                    execution repository first makes the drain store a token whose wakes were never applied.""",
                    shutdownPhases.getFirst(), Phase.INBOUND_EVENT_CONNECTORS)
                .isLessThan(Phase.INBOUND_EVENT_CONNECTORS)
                .as("but still above the phase that tears down the processor's executors")
                .isGreaterThan(Phase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS);
    }

    private static final class CapturingComponentRegistry implements ComponentRegistry {

        private DecoratorDefinition.CompletedDecoratorDefinition<TagResolver, ? extends TagResolver> decoratorDefinition;
        private ComponentDefinition<?> componentDefinition;

        /**
         * Phases the captured definition registers shutdown handlers at. The component is only resolved from inside
         * the handler, so initializing the lifecycle never builds a {@code WorkflowEngine}.
         */
        private List<Integer> capturedShutdownPhases() {
            var phases = new ArrayList<Integer>();
            ((ComponentDefinition.ComponentCreator<?>) componentDefinition)
                    .createComponent()
                    .initLifecycle(mock(Configuration.class), new LifecycleRegistry() {
                        @Override
                        public LifecycleRegistry registerLifecyclePhaseTimeout(long timeout, TimeUnit timeUnit) {
                            return this;
                        }

                        @Override
                        public LifecycleRegistry onStart(int phase, LifecycleHandler startHandler) {
                            return this;
                        }

                        @Override
                        public LifecycleRegistry onShutdown(int phase, LifecycleHandler shutdownHandler) {
                            phases.add(phase);
                            return this;
                        }
                    });
            return phases;
        }

        @Override
        public <C> ComponentRegistry registerDecorator(DecoratorDefinition<C, ? extends C> decoratorDefinition) {
            this.decoratorDefinition =
                    (DecoratorDefinition.CompletedDecoratorDefinition<TagResolver, ? extends TagResolver>) decoratorDefinition;
            return this;
        }

        private TagResolver decorate(TagResolver delegate) {
            var component = new StaticComponent<>(new Component.Identifier<>(TagResolver.class, "tagResolver"), delegate);
            return decoratorDefinition.decorate(component).resolve(mock(Configuration.class));
        }

        @Override
        public <C> ComponentRegistry registerComponent(ComponentDefinition<? extends C> componentDefinition) {
            this.componentDefinition = componentDefinition;
            return this;
        }

        @Override
        public boolean hasComponent(Class<?> componentType, String name, SearchScope searchScope) {
            return false;
        }

        @Override
        public ComponentRegistry registerEnhancer(ConfigurationEnhancer enhancer) {
            return this;
        }

        @Override
        public ComponentRegistry registerModule(Module module) {
            return this;
        }

        @Override
        public <C> ComponentRegistry registerFactory(ComponentFactory<C> factory) {
            return this;
        }

        @Override
        public ComponentRegistry setOverridePolicy(OverridePolicy overridePolicy) {
            return this;
        }

        @Override
        public ComponentRegistry disableEnhancerScanning() {
            return this;
        }

        @Override
        public ComponentRegistry disableEnhancer(Class<? extends ConfigurationEnhancer> enhancerType) {
            return this;
        }

        @Override
        public ComponentRegistry disableEnhancer(String enhancerName) {
            return this;
        }

        @Override
        public void describeTo(@Nonnull org.axonframework.common.infra.ComponentDescriptor descriptor) {
            // no-op for tests
        }
    }

    private static final class StaticComponent<C> implements Component<C> {

        private final Identifier<C> identifier;
        private final C instance;

        private StaticComponent(Identifier<C> identifier, C instance) {
            this.identifier = identifier;
            this.instance = instance;
        }

        @Override
        public Identifier<C> identifier() {
            return identifier;
        }

        @Override
        public C resolve(Configuration configuration) {
            return instance;
        }

        @Override
        public boolean isInstantiated() {
            return true;
        }

        @Override
        public void initLifecycle(Configuration configuration, LifecycleRegistry lifecycleRegistry) {
            // no-op for tests
        }

        @Override
        public boolean isInitialized() {
            return true;
        }

        @Override
        public void describeTo(@Nonnull org.axonframework.common.infra.ComponentDescriptor descriptor) {
            // no-op for tests
        }
    }

    @SuppressWarnings("unchecked")
    private static List<TagResolver> extractDelegates(MultiTagResolver resolver) {
        try {
            var field = MultiTagResolver.class.getDeclaredField("delegates");
            field.setAccessible(true);
            return (List<TagResolver>) field.get(resolver);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to inspect delegates from MultiTagResolver", e);
        }
    }
}
