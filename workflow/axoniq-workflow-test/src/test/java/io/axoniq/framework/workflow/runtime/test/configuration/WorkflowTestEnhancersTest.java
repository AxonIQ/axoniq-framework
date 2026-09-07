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
package io.axoniq.framework.workflow.runtime.test.configuration;

import io.axoniq.framework.workflow.configuration.WorkflowConfigurationDefaults;
import io.axoniq.framework.workflow.runtime.execution.ExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.execution.WorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.DelayedPublisher;
import io.axoniq.framework.workflow.runtime.test.utils.IdGenerator;
import io.axoniq.framework.workflow.runtime.test.utils.ManualExecuteStepActionResolver;
import io.axoniq.framework.workflow.runtime.test.utils.ManualWorkflowScheduler;
import io.axoniq.framework.workflow.runtime.test.utils.TestClock;
import io.axoniq.framework.workflow.runtime.test.utils.PrettyPrintingRecordingEventStore;
import io.axoniq.framework.workflow.runtime.test.utils.RecordingIdGenerator;
import io.axoniq.framework.workflow.runtime.test.utils.TestEventPublisher;
import org.axonframework.common.configuration.ComponentBuilder;
import org.axonframework.common.configuration.ComponentDecorator;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.InterceptingEventStore;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.util.concurrent.Executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for workflow test configuration enhancers.
 *
 * @author Simon Zambrovski
 */
class WorkflowTestEnhancersTest {

    @Test
    void steppingEnhancerRegistersManualTestingComponents() {
        ComponentRegistry registry = mock(ComponentRegistry.class, RETURNS_SELF);

        new WorkflowTestSteppingModeEnhancer().enhance(registry);

        verify(registry).registerComponent(eq(ManualExecuteStepActionResolver.class), any(ComponentBuilder.class));
        verify(registry).registerComponent(eq(ExecuteStepActionResolver.class), any(ComponentBuilder.class));
        verify(registry).registerComponent(eq(TestClock.class), any(ComponentBuilder.class));
        verify(registry).registerComponent(eq(Clock.class), any(ComponentBuilder.class));
        verify(registry).registerComponent(eq(ManualWorkflowScheduler.class), any(ComponentBuilder.class));
        verify(registry).registerComponent(eq(WorkflowScheduler.class), any(ComponentBuilder.class));
    }

    @Test
    void prettyRecordingEventStoreEnhancerRegistersDecorator() {
        ComponentRegistry registry = mock(ComponentRegistry.class, RETURNS_SELF);
        ArgumentCaptor<ComponentDecorator<EventStore, EventStore>> decoratorCaptor = componentDecoratorCaptor();
        EventStore delegate = mock(EventStore.class);

        new WorkflowTestPrettyRecordingEventStoreEnhancer().enhance(registry);

        verify(registry).registerDecorator(eq(EventStore.class),
                                           eq(InterceptingEventStore.DECORATION_ORDER - 1),
                                           decoratorCaptor.capture());
        EventStore decorated = decoratorCaptor.getValue().decorate(mock(Configuration.class), "eventStore", delegate);

        assertThat(decorated).isInstanceOf(PrettyPrintingRecordingEventStore.class);
        assertThat(decoratorCaptor.getValue().decorate(mock(Configuration.class), "eventStore", decorated))
                .isSameAs(decorated);
    }

    @Test
    void delayedPublisherEnhancerRegistersAndBuildsPublisherComponents() {
        ComponentRegistry registry = mock(ComponentRegistry.class, RETURNS_SELF);
        ArgumentCaptor<ComponentBuilder<IdGenerator>> idGeneratorBuilder = componentBuilderCaptor();
        ArgumentCaptor<ComponentBuilder<TestEventPublisher>> eventPublisherBuilder = componentBuilderCaptor();
        ArgumentCaptor<ComponentBuilder<DelayedPublisher>> delayedPublisherBuilder = componentBuilderCaptor();
        Configuration configuration = configurationForDelayedPublisher();

        new WorkflowTestEventPublicationEnhancer().enhance(registry);

        verify(registry).registerComponent(eq(IdGenerator.class), idGeneratorBuilder.capture());
        verify(registry).registerComponent(eq(TestEventPublisher.class), eventPublisherBuilder.capture());
        verify(registry).registerComponent(eq(DelayedPublisher.class), delayedPublisherBuilder.capture());
        assertThat(idGeneratorBuilder.getValue().build(configuration)).isInstanceOf(RecordingIdGenerator.class);
        assertThat(eventPublisherBuilder.getValue().build(configuration)).isInstanceOf(TestEventPublisher.class);
        assertThat(delayedPublisherBuilder.getValue().build(configuration)).isInstanceOf(DelayedPublisher.class);
    }

    private static Configuration configurationForDelayedPublisher() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getComponent(EventSink.class)).thenReturn(mock(EventSink.class));
        when(configuration.getComponent(MessageTypeResolver.class)).thenReturn(mock(MessageTypeResolver.class));
        when(configuration.getComponent(EventConverter.class)).thenReturn(mock(EventConverter.class));
        when(configuration.getComponent(Clock.class)).thenReturn(Clock.systemUTC());
        when(configuration.getComponent(IdGenerator.class)).thenReturn(mock(IdGenerator.class));
        when(configuration.getComponent(TestEventPublisher.class)).thenReturn(mock(TestEventPublisher.class));
        when(configuration.getComponent(Executor.class,
                                        WorkflowConfigurationDefaults.WORKFLOW_ENGINE_EXECUTOR)).thenReturn(
                Runnable::run);
        return configuration;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> ArgumentCaptor<ComponentBuilder<T>> componentBuilderCaptor() {
        return ArgumentCaptor.forClass((Class) ComponentBuilder.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <C, D extends C> ArgumentCaptor<ComponentDecorator<C, D>> componentDecoratorCaptor() {
        return ArgumentCaptor.forClass((Class) ComponentDecorator.class);
    }
}
