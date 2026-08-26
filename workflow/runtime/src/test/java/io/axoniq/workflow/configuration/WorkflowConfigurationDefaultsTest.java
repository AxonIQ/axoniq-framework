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
import org.axonframework.common.configuration.Module;
import org.axonframework.common.configuration.OverridePolicy;
import org.axonframework.common.configuration.SearchScope;
import org.axonframework.common.configuration.LifecycleRegistry;
import org.axonframework.eventsourcing.eventstore.MultiTagResolver;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;

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
    void defaultWorkflowTimerExecutorUsesBoundedTimerCapacity() {
        ScheduledThreadPoolExecutor executor = WorkflowConfigurationDefaults.defaultWorkflowTimerExecutor();
        try {
            assertThat(executor.getCorePoolSize())
                    .isEqualTo(WorkflowConfigurationDefaults.DEFAULT_WORKFLOW_TIMER_THREAD_COUNT);
        } finally {
            executor.shutdownNow();
        }
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

    private static final class CapturingComponentRegistry implements ComponentRegistry {

        private DecoratorDefinition.CompletedDecoratorDefinition<TagResolver, ? extends TagResolver> decoratorDefinition;

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
